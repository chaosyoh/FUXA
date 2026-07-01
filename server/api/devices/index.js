/**
 * 'api/devices': Device import/export API for XLS and KepServer format
 */

var express = require("express");
const multer = require('multer');
const authJwt = require('../jwt-helper');
const xlsService = require('./devices-xls.service');
const kepConverter = require('./kepserver-converter');

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 100 * 1024 * 1024 } // 100MB
});

var runtime;
var secureFnc;
var checkGroupsFnc;

module.exports = {
    init: function (_runtime, _secureFnc, _checkGroupsFnc) {
        runtime = _runtime;
        secureFnc = _secureFnc;
        checkGroupsFnc = _checkGroupsFnc;
    },
    app: function () {
        var devicesApp = express();
        devicesApp.use(function (req, res, next) {
            if (!runtime.project) {
                res.status(404).end();
            } else {
                next();
            }
        });

        /**
         * GET /api/devices/export?type=xls
         * Export devices as xlsx file stream
         */
        devicesApp.get("/api/devices/export", secureFnc, async function (req, res) {
            const permission = checkGroupsFnc(req);
            if (res.statusCode === 403) {
                runtime.logger.error("api get devices/export: Token Expired");
                return;
            } else if (!authJwt.haveAdminPermission(permission)) {
                res.status(401).json({ error: "unauthorized_error", message: "Unauthorized!" });
                runtime.logger.error("api get devices/export: Unauthorized");
                return;
            }

            try {
                const devices = runtime.project.getDevices();
                const deviceFolders = runtime.project.getDeviceFolders() || {};
                let scripts = [];
                try {
                    scripts = await runtime.project.getScripts() || [];
                } catch (e) {
                    // scripts may not exist
                }

                const buffer = await xlsService.generateXls(devices, scripts, deviceFolders);

                const projectName = 'fuxa';
                const filename = `${projectName}-devices.xlsx`;

                res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
                res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
                res.setHeader('Content-Length', buffer.length);
                res.send(Buffer.from(buffer));
            } catch (err) {
                runtime.logger.error("api get devices/export: " + (err.message || err));
                res.status(500).json({ error: "export_failed", message: err.message || String(err) });
            }
        });

        /**
         * POST /api/devices/import?isTemplate=false
         * Import devices from uploaded xlsx file
         * Returns parsed devices array
         */
        devicesApp.post("/api/devices/import", secureFnc, upload.single('file'), async function (req, res) {
            const permission = checkGroupsFnc(req);
            if (res.statusCode === 403) {
                runtime.logger.error("api post devices/import: Token Expired");
                return;
            } else if (!authJwt.haveAdminPermission(permission)) {
                res.status(401).json({ error: "unauthorized_error", message: "Unauthorized!" });
                runtime.logger.error("api post devices/import: Unauthorized");
                return;
            }

            if (!req.file) {
                res.status(400).json({ error: "invalid_request", message: "No file uploaded" });
                return;
            }

            try {
                const isTemplate = req.query.isTemplate === 'true';
                let scripts = [];
                try {
                    scripts = await runtime.project.getScripts() || [];
                } catch (e) {
                    // scripts may not exist
                }

                const result = await xlsService.parseXls(req.file.buffer, isTemplate, scripts);

                res.json(result);
            } catch (err) {
                runtime.logger.error("api post devices/import: " + (err.message || err));
                res.status(400).json({ error: "import_failed", message: err.message || String(err) });
            }
        });

        /**
         * POST /api/devices/import-kepserver
         * Import devices from uploaded KepServer JSON file
         * Merge logic: same name + same type → update device property & merge tags by name
         */
        devicesApp.post("/api/devices/import-kepserver", secureFnc, upload.single('file'), async function (req, res) {
            const permission = checkGroupsFnc(req);
            if (res.statusCode === 403) {
                runtime.logger.error("api post devices/import-kepserver: Token Expired");
                return;
            } else if (!authJwt.haveAdminPermission(permission)) {
                res.status(401).json({ error: "unauthorized_error", message: "Unauthorized!" });
                runtime.logger.error("api post devices/import-kepserver: Unauthorized");
                return;
            }

            if (!req.file) {
                res.status(400).json({ error: "invalid_request", message: "No file uploaded" });
                return;
            }

            try {
                // Parse KepServer JSON (with BOM/comment stripping)
                const rawText = req.file.buffer.toString('utf8');
                const cleanText = kepConverter.stripJsonComments(rawText);
                const kepJson = JSON.parse(cleanText);

                // Convert to FUXA devices
                const convertedDevices = kepConverter.convertKepserverToFuxa(kepJson);

                // Get existing devices for merge
                const existingDevices = runtime.project.getDevices() || {};

                // Merge logic
                const mergedDevices = mergeKepDevices(convertedDevices, existingDevices);

                res.json({ devices: mergedDevices });
            } catch (err) {
                runtime.logger.error("api post devices/import-kepserver: " + (err.message || err));
                res.status(400).json({ error: "import_failed", message: err.message || String(err) });
            }
        });

        return devicesApp;
    }
};

/**
 * Merge converted KepServer devices with existing FUXA devices.
 * - If an existing device has the same name and same type, update its property and merge tags.
 * - If tag with same name exists in the device, update tag settings (keep existing id).
 * - Otherwise create new device/tag.
 */
function mergeKepDevices(convertedDevices, existingDevices) {
    // Build lookup: name+type → existing device
    const existingByNameType = {};
    for (const key of Object.keys(existingDevices)) {
        const dev = existingDevices[key];
        if (dev && dev.name && dev.type) {
            existingByNameType[dev.name + '|' + dev.type] = dev;
        }
    }

    const result = [];
    for (const newDev of convertedDevices) {
        const lookupKey = newDev.name + '|' + newDev.type;
        const existing = existingByNameType[lookupKey];

        if (existing) {
            // Merge: keep existing id, update property
            const merged = JSON.parse(JSON.stringify(existing));
            merged.property = newDev.property;
            merged.polling = newDev.polling;
            merged.enabled = newDev.enabled;

            // Merge tags by name
            const existingTagsByName = {};
            if (merged.tags) {
                for (const tagId of Object.keys(merged.tags)) {
                    const t = merged.tags[tagId];
                    if (t && t.name) {
                        existingTagsByName[t.name] = tagId;
                    }
                }
            } else {
                merged.tags = {};
            }

            for (const newTagId of Object.keys(newDev.tags)) {
                const newTag = newDev.tags[newTagId];
                const existingTagId = existingTagsByName[newTag.name];
                if (existingTagId) {
                    // Update existing tag: keep id, update other fields
                    const updatedTag = Object.assign({}, merged.tags[existingTagId], {
                        type: newTag.type,
                        address: newTag.address,
                        memaddress: newTag.memaddress,
                        description: newTag.description || merged.tags[existingTagId].description,
                        divisor: newTag.divisor,
                        scale: newTag.scale || merged.tags[existingTagId].scale,
                        groupId: newTag.groupId || merged.tags[existingTagId].groupId
                    });
                    merged.tags[existingTagId] = updatedTag;
                } else {
                    // New tag
                    merged.tags[newTag.id] = newTag;
                }
            }

            // Merge tagGroups
            if (newDev.tagGroups) {
                merged.tagGroups = Object.assign({}, merged.tagGroups || {}, newDev.tagGroups);
            }

            result.push(merged);
        } else {
            // New device
            result.push(newDev);
        }
    }
    return result;
}
