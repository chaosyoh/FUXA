/**
 * Device XLS (xlsx) import/export service
 * Uses ExcelJS to generate and parse xlsx files containing Devices and Tags sheets.
 */

const ExcelJS = require('exceljs');
const crypto = require('crypto');

const DEVICE_PREFIX = 'd_';
const TAG_PREFIX = 't_';

function getGUID(prefix) {
    return (prefix || '') + crypto.randomBytes(8).toString('hex');
}

function getShortGUID(prefix) {
    return (prefix || '') + crypto.randomBytes(3).toString('hex');
}

/**
 * Collect all unique device keys (excluding 'tags') across the device set
 */
function collectDeviceKeys(devices) {
    const baseKeys = ['id', 'name', 'enabled', 'type', 'polling'];
    const extraKeys = new Set();
    const propertyKeys = new Set();

    devices.forEach(device => {
        if (!device) return;
        Object.keys(device).forEach(key => {
            if (key === 'tags' || key === 'property') return;
            if (!baseKeys.includes(key)) {
                extraKeys.add(key);
            }
        });
        if (device.property && typeof device.property === 'object') {
            Object.keys(device.property).forEach(key => {
                propertyKeys.add(key);
            });
        }
    });

    return {
        deviceKeys: [...baseKeys, ...Array.from(extraKeys).sort()],
        propertyKeys: Array.from(propertyKeys).sort()
    };
}

/**
 * Collect all unique tag keys (excluding 'daq', 'value', 'timestamp') across all devices
 */
function collectTagKeys(devices) {
    const baseKeys = ['id', 'name', 'label', 'type', 'memaddress', 'address', 'divisor', 'init', 'format',
        'description', 'scaleReadFunction', 'scaleReadParams', 'scaleWriteFunction', 'scaleWriteParams',
        'scale', 'deadband', 'sysType', 'direction', 'edge'];
    const extraKeys = new Set();
    const daqKeys = ['enabled', 'changed', 'interval', 'restored'];
    const extraDaqKeys = new Set();

    devices.forEach(device => {
        if (!device || !device.tags) return;
        const tags = typeof device.tags === 'object' ? Object.values(device.tags) : [];
        tags.forEach(tag => {
            if (!tag) return;
            Object.keys(tag).forEach(key => {
                if (key === 'daq' || key === 'options' || key === 'value' || key === 'timestamp') return;
                if (!baseKeys.includes(key)) {
                    extraKeys.add(key);
                }
            });
            if (tag.daq && typeof tag.daq === 'object') {
                Object.keys(tag.daq).forEach(key => {
                    if (!daqKeys.includes(key)) {
                        extraDaqKeys.add(key);
                    }
                });
            }
        });
    });

    return {
        tagKeys: [...baseKeys, ...Array.from(extraKeys).sort()],
        daqKeys: [...daqKeys, ...Array.from(extraDaqKeys).sort()]
    };
}

/**
 * Build a script-name-by-id map
 */
function getScriptNameByIdMap(scripts) {
    const map = {};
    if (!scripts || !Array.isArray(scripts)) return map;
    scripts.forEach(s => {
        if (s && s.id && s.name) map[s.id] = s.name;
    });
    return map;
}

/**
 * Build a script-id-by-name map
 */
function getScriptIdByNameMap(scripts) {
    const map = {};
    if (!scripts || !Array.isArray(scripts)) return map;
    scripts.forEach(s => {
        if (s && s.id && s.name) map[s.name] = s.id;
    });
    return map;
}

/**
 * Format a cell value for export
 */
function formatCellValue(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') {
        try { return JSON.stringify(value); } catch (e) { return String(value); }
    }
    return value;
}

/**
 * Parse a cell value during import
 */
function parseCellValue(key, value) {
    if (value === null || value === undefined || value === '') return '';
    const str = String(value).trim();
    // Try JSON for object fields
    if ((str.startsWith('{') || str.startsWith('[')) && isJson(str)) {
        try { return JSON.parse(str); } catch (e) { }
    }
    // Boolean fields
    if (isBooleanKey(key)) {
        return str.toLowerCase() === 'true' || str === '1';
    }
    // Number fields
    if (isNumberKey(key)) {
        const num = parseFloat(str);
        return isNaN(num) ? str : num;
    }
    return str;
}

function isJson(str) {
    try { JSON.parse(str); return true; } catch (e) { return false; }
}

function isBooleanKey(key) {
    return key === 'enabled'
        || key.endsWith('.enabled')
        || key.endsWith('.changed')
        || key.endsWith('.restored')
        || key.endsWith('.forceFC16')
        || key.endsWith('.ascii')
        || key.endsWith('.octalIO');
}

function isNumberKey(key) {
    return key === 'polling'
        || key === 'divisor'
        || key === 'format'
        || key === 'sysType'
        || key.endsWith('.interval')
        || key.endsWith('.delay');
}

/**
 * Generate xlsx buffer from devices data
 * @param {Object} devices - devices dictionary {id: device}
 * @param {Array} scripts - scripts array
 * @param {Object} deviceFolders - device folders dictionary {id: {id, name, parentId}}
 * @returns {Promise<Buffer>}
 */
async function generateXls(devices, scripts, deviceFolders) {
    const deviceList = Array.isArray(devices) ? devices : Object.values(devices || {});
    const scriptNameById = getScriptNameByIdMap(scripts);
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'FUXA';
    workbook.created = new Date();

    // --- Devices Sheet ---
    const devSheet = workbook.addWorksheet('Devices');
    const { deviceKeys, propertyKeys } = collectDeviceKeys(deviceList);
    const devColumns = [
        ...deviceKeys.map(k => k),
        ...propertyKeys.map(k => `property.${k}`)
    ];
    devSheet.addRow(devColumns);
    // Style header row
    const devHeaderRow = devSheet.getRow(1);
    devHeaderRow.font = { bold: true };
    devHeaderRow.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } }; });

    deviceList.forEach(device => {
        if (!device) return;
        const row = [];
        deviceKeys.forEach(dk => {
            row.push(formatCellValue(device[dk]));
        });
        propertyKeys.forEach(pk => {
            row.push(device.property ? formatCellValue(device.property[pk]) : '');
        });
        devSheet.addRow(row);
    });

    // Auto-width columns
    devSheet.columns.forEach(col => {
        let maxLen = 10;
        col.eachCell(cell => {
            const len = cell.value ? String(cell.value).length : 0;
            if (len > maxLen) maxLen = Math.min(len, 50);
        });
        col.width = maxLen + 2;
    });

    // --- Tags Sheet ---
    const tagSheet = workbook.addWorksheet('Tags');
    const { tagKeys, daqKeys } = collectTagKeys(deviceList);
    const tagColumns = [
        'deviceId',
        ...tagKeys.map(k => k),
        'options',
        ...daqKeys.map(k => `daq.${k}`)
    ];
    tagSheet.addRow(tagColumns);
    const tagHeaderRow = tagSheet.getRow(1);
    tagHeaderRow.font = { bold: true };
    tagHeaderRow.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } }; });

    deviceList.forEach(device => {
        if (!device || !device.tags) return;
        const tags = typeof device.tags === 'object' ? Object.values(device.tags) : [];
        tags.forEach(tag => {
            if (!tag) return;
            const row = [device.id];
            tagKeys.forEach(tk => {
                let value = tag[tk];
                if (tk === 'scaleReadFunction' || tk === 'scaleWriteFunction') {
                    value = scriptNameById[value] || value;
                }
                row.push(formatCellValue(value));
            });
            // options
            row.push(formatCellValue(tag.options));
            // daq fields
            daqKeys.forEach(dk => {
                const value = tag.daq ? tag.daq[dk] : '';
                row.push(formatCellValue(value));
            });
            tagSheet.addRow(row);
        });
    });

    // Auto-width columns
    tagSheet.columns.forEach(col => {
        let maxLen = 10;
        col.eachCell(cell => {
            const len = cell.value ? String(cell.value).length : 0;
            if (len > maxLen) maxLen = Math.min(len, 50);
        });
        col.width = maxLen + 2;
    });

    // --- DeviceFolders Sheet ---
    const folderSheet = workbook.addWorksheet('DeviceFolders');
    const folderColumns = ['id', 'name', 'parentId'];
    folderSheet.addRow(folderColumns);
    const folderHeaderRow = folderSheet.getRow(1);
    folderHeaderRow.font = { bold: true };
    folderHeaderRow.eachCell(cell => { cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } }; });

    const folders = deviceFolders || {};
    for (const folder of Object.values(folders)) {
        if (!folder) continue;
        folderSheet.addRow([folder.id || '', folder.name || '', folder.parentId || '']);
    }

    // Auto-width columns
    folderSheet.columns.forEach(col => {
        let maxLen = 10;
        col.eachCell(cell => {
            const len = cell.value ? String(cell.value).length : 0;
            if (len > maxLen) maxLen = Math.min(len, 50);
        });
        col.width = maxLen + 2;
    });

    const buffer = await workbook.xlsx.writeBuffer();
    return buffer;
}

/**
 * Parse xlsx buffer to devices array
 * @param {Buffer} buffer - xlsx file buffer
 * @param {boolean} isTemplate - if true, generate new IDs
 * @param {Array} scripts - scripts array
 * @returns {Promise<Array>}
 */
async function parseXls(buffer, isTemplate, scripts) {
    const scriptIdByName = getScriptIdByNameMap(scripts);
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(buffer);

    // --- Parse Devices Sheet ---
    const devSheet = workbook.getWorksheet('Devices');
    if (!devSheet) {
        throw new Error('Missing "Devices" sheet in xlsx file');
    }

    const devHeaderRow = devSheet.getRow(1);
    const devHeaders = [];
    devHeaderRow.eachCell((cell, colNumber) => {
        devHeaders[colNumber] = cell.value ? String(cell.value).trim() : '';
    });

    const devices = {};
    devSheet.eachRow((row, rowNumber) => {
        if (rowNumber === 1) return; // skip header
        const device = { tags: {} };
        let hasProperty = false;

        row.eachCell((cell, colNumber) => {
            const key = devHeaders[colNumber];
            if (!key) return;
            const rawValue = cell.value;

            if (key.startsWith('property.')) {
                if (!device.property) device.property = {};
                hasProperty = true;
                const propKey = key.substring('property.'.length);
                device.property[propKey] = parseCellValue(key, rawValue);
            } else if (key === 'id') {
                device.id = rawValue ? String(rawValue) : '';
            } else if (key === 'enabled') {
                const v = rawValue;
                device.enabled = (v === true || String(v).toLowerCase() === 'true' || v === 1);
            } else if (key === 'polling') {
                device.polling = parseInt(rawValue) || 1000;
            } else {
                device[key] = parseCellValue(key, rawValue);
            }
        });

        if (!hasProperty && !device.property) {
            device.property = {};
        }
        if (!device.polling) device.polling = 1000;
        if (device.id) {
            devices[device.id] = device;
        }
    });

    // --- Parse Tags Sheet ---
    const tagSheet = workbook.getWorksheet('Tags');
    if (tagSheet) {
        const tagHeaderRow = tagSheet.getRow(1);
        const tagHeaders = [];
        tagHeaderRow.eachCell((cell, colNumber) => {
            tagHeaders[colNumber] = cell.value ? String(cell.value).trim() : '';
        });

        tagSheet.eachRow((row, rowNumber) => {
            if (rowNumber === 1) return;
            let deviceId = '';
            const tag = { daq: {} };

            row.eachCell((cell, colNumber) => {
                const key = tagHeaders[colNumber];
                if (!key) return;
                const rawValue = cell.value;

                if (key === 'deviceId') {
                    deviceId = rawValue ? String(rawValue) : '';
                } else if (key === 'id') {
                    tag.id = rawValue ? String(rawValue) : '';
                } else if (key === 'options') {
                    tag.options = parseCellValue(key, rawValue);
                } else if (key.startsWith('daq.')) {
                    const daqKey = key.substring('daq.'.length);
                    tag.daq[daqKey] = parseCellValue(key, rawValue);
                } else if (key === 'scaleReadFunction' || key === 'scaleWriteFunction') {
                    const val = rawValue ? String(rawValue) : '';
                    tag[key] = scriptIdByName[val] || val;
                } else if (key === 'divisor') {
                    tag.divisor = parseInt(rawValue) || 1;
                } else if (key === 'format') {
                    const f = parseInt(rawValue);
                    tag.format = isNaN(f) ? null : f;
                } else if (key === 'sysType') {
                    const s = parseInt(rawValue);
                    tag.sysType = isNaN(s) ? rawValue : s;
                } else {
                    tag[key] = parseCellValue(key, rawValue);
                }
            });

            if (!tag.id) {
                tag.id = getGUID(TAG_PREFIX);
            }

            if (deviceId && devices[deviceId]) {
                devices[deviceId].tags[tag.id] = tag;
            }
        });
    }

    let result = Object.values(devices);

    // --- Parse DeviceFolders Sheet ---
    let parsedDeviceFolders = {};
    const folderSheet = workbook.getWorksheet('DeviceFolders');
    if (folderSheet) {
        const folderHeaderRow = folderSheet.getRow(1);
        const folderHeaders = [];
        folderHeaderRow.eachCell((cell, colNumber) => {
            folderHeaders[colNumber] = cell.value ? String(cell.value).trim() : '';
        });

        folderSheet.eachRow((row, rowNumber) => {
            if (rowNumber === 1) return;
            const folder = {};
            row.eachCell((cell, colNumber) => {
                const key = folderHeaders[colNumber];
                if (!key) return;
                folder[key] = cell.value ? String(cell.value).trim() : '';
            });
            if (folder.id) {
                parsedDeviceFolders[folder.id] = {
                    id: folder.id,
                    name: folder.name || '',
                    parentId: folder.parentId || ''
                };
            }
        });
    }

    // Template mode: regenerate IDs, filter out FuxaServer
    if (isTemplate) {
        result = result.filter(d => d.type !== 'FuxaServer');
        result.forEach(device => {
            device.id = getGUID(DEVICE_PREFIX);
            device.name = getShortGUID(device.name + '_');
            if (device.tags) {
                const newTags = {};
                Object.values(device.tags).forEach(tag => {
                    const newId = getGUID(TAG_PREFIX);
                    tag.id = newId;
                    newTags[newId] = tag;
                });
                device.tags = newTags;
            }
        });
        // In template mode, regenerate folder IDs
        const oldToNewFolderId = {};
        const newFolders = {};
        for (const folder of Object.values(parsedDeviceFolders)) {
            const newId = 'df_' + crypto.randomBytes(8).toString('hex');
            oldToNewFolderId[folder.id] = newId;
            newFolders[newId] = { id: newId, name: folder.name, parentId: '' };
        }
        // Update parentId references
        for (const folder of Object.values(newFolders)) {
            const oldFolder = Object.values(parsedDeviceFolders).find(f => f.name === folder.name);
            if (oldFolder && oldFolder.parentId && oldToNewFolderId[oldFolder.parentId]) {
                folder.parentId = oldToNewFolderId[oldFolder.parentId];
            }
        }
        // Update device folderId references
        result.forEach(device => {
            if (device.folderId && oldToNewFolderId[device.folderId]) {
                device.folderId = oldToNewFolderId[device.folderId];
            } else {
                delete device.folderId;
            }
        });
        parsedDeviceFolders = newFolders;
    }

    return { devices: result, deviceFolders: parsedDeviceFolders };
}

module.exports = {
    generateXls,
    parseXls
};
