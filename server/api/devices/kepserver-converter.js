/**
 * KepServer JSON → FUXA Device Converter
 * Converts KepServer exported JSON configuration into FUXA device objects.
 *
 * Supported KepServer drivers:
 *   - "Siemens TCP/IP Ethernet" → FUXA "SiemensS7"
 *   - "Modbus TCP/IP Ethernet"  → FUXA "ModbusTCP"
 */

'use strict';

const crypto = require('crypto');

const DEVICE_PREFIX = 'd_';
const TAG_PREFIX = 't_';
const TAG_GROUP_PREFIX = 'tg_';

function getGUID(prefix) {
    return (prefix || '') + crypto.randomBytes(8).toString('hex');
}

// ─── Driver Type Mapping ─────────────────────────────────────────────────────

const KEPSERVER_DRIVER_MAP = {
    'Siemens TCP/IP Ethernet': 'SiemensS7',
    'Modbus TCP/IP Ethernet': 'ModbusTCP'
};

// ─── S7 Device Model → FUXA cpuType ─────────────────────────────────────────
// KepServer DEVICE_MODEL: 0=S7200, 1=S7300, 2=S7400, 3=S71200, 4=S71500, 5/6=NetLink(不支持)
const S7_MODEL_MAP = {
    0: 'S7200Smart',  // KepServer labels as S7200, but most modern use is S7-200 Smart
    1: 'S7300',
    2: 'S7400',
    3: 'S71200',
    4: 'S71500'
};

// ─── KepServer TAG_DATA_TYPE Mappings ────────────────────────────────────────
// 0=String, 1=Boolean, 2=Char, 3=Byte, 4=Int16, 5=Word, 6=Int32, 7=DWord, 8=Float, 9=Double

const S7_TAG_TYPE_MAP = {
    1: 'Boolean',
    2: 'Byte',
    3: 'Byte',
    4: 'Int',
    5: 'Word',
    6: 'DInt',
    7: 'DWord',
    8: 'Real',
    9: 'Real'   // S7 doesn't support Double natively, use Real
};

const MODBUS_TAG_TYPE_MAP = {
    1: 'Boolean',
    2: 'UInt16',
    3: 'UInt16',
    4: 'Int16',
    5: 'UInt16',
    6: 'Int32',
    7: 'UInt32',
    8: 'Float32',
    9: 'Float64'
};

// Modbus MLE (Mid-Little-Endian / word-swapped) type mapping
// Applied when KepServer device has DEVICE_FIRST_WORD_LOW=true (32-bit) or DEVICE_FIRST_DWORD_LOW=true (64-bit)
const MODBUS_MLE_WORD_MAP = {
    'Int32': 'Int32MLE',
    'UInt32': 'UInt32MLE',
    'Float32': 'Float32MLE'
};
const MODBUS_MLE_DWORD_MAP = {
    'Float64': 'Float64MLE'
};

// ─── Modbus Area Mapping ─────────────────────────────────────────────────────
// KepServer: first digit of address = area code
// FUXA memaddress: "0"=Coils, "100000"=DiscreteInputs, "300000"=InputRegisters, "400000"=HoldingRegisters
const MODBUS_AREA_MAP = {
    '0': '0',
    '1': '100000',
    '3': '300000',
    '4': '400000'
};

// ─── Main Converter ──────────────────────────────────────────────────────────

/**
 * Convert KepServer exported JSON to an array of FUXA device objects.
 * @param {object} kepJson - Parsed KepServer JSON (with comments stripped)
 * @returns {Array} Array of FUXA device objects
 */
function convertKepserverToFuxa(kepJson) {
    const devices = [];
    const channels = kepJson && kepJson.project && kepJson.project.channels;
    if (!channels || !Array.isArray(channels)) {
        return devices;
    }

    for (const channel of channels) {
        const driverName = channel['servermain.MULTIPLE_TYPES_DEVICE_DRIVER'];
        const fuxaType = KEPSERVER_DRIVER_MAP[driverName];
        if (!fuxaType) continue; // Unsupported driver, skip

        const channelDevices = channel.devices || [];
        for (const kepDevice of channelDevices) {
            const device = convertDevice(kepDevice, fuxaType);
            if (device) {
                devices.push(device);
            }
        }
    }

    return devices;
}

/**
 * Convert a single KepServer device to a FUXA device object.
 */
function convertDevice(kepDevice, fuxaType) {
    const deviceName = kepDevice['common.ALLTYPES_NAME'];
    const deviceModel = kepDevice['servermain.DEVICE_MODEL'];

    // Skip NetLink models (5, 6)
    if (deviceModel === 5 || deviceModel === 6) {
        return null;
    }

    const device = {
        id: getGUID(DEVICE_PREFIX),
        name: deviceName || 'Unnamed',
        enabled: true,
        type: fuxaType,
        polling: kepDevice['servermain.DEVICE_SCAN_MODE_RATE_MS'] || 1000,
        property: {},
        tags: {},
        tagGroups: {}
    };

    if (fuxaType === 'SiemensS7') {
        fillS7Property(device, kepDevice);
    } else if (fuxaType === 'ModbusTCP') {
        fillModbusProperty(device, kepDevice);
    }

    // Modbus byte order options (device-level)
    const firstWordLow = fuxaType === 'ModbusTCP' && kepDevice['modbus_ethernet.DEVICE_FIRST_WORD_LOW'] === true;
    const firstDWordLow = fuxaType === 'ModbusTCP' && kepDevice['modbus_ethernet.DEVICE_FIRST_DWORD_LOW'] === true;

    // Convert root-level tags
    const kepTags = kepDevice.tags || [];
    for (const kepTag of kepTags) {
        const tag = convertTag(kepTag, fuxaType, firstWordLow, firstDWordLow);
        if (tag) {
            device.tags[tag.id] = tag;
        }
    }

    // Convert tag_groups (recursive)
    const kepTagGroups = kepDevice.tag_groups || [];
    processTagGroups(kepTagGroups, device, fuxaType, '', firstWordLow, firstDWordLow);

    return device;
}

// ─── Tag Groups Processing ──────────────────────────────────────────────────

/**
 * Recursively process KepServer tag_groups and populate device.tagGroups and tags.
 * @param {Array} groups - tag_groups array from KepServer JSON
 * @param {Object} device - FUXA device being built
 * @param {string} fuxaType - Device type (SiemensS7/ModbusTCP)
 * @param {string} parentGroupId - Parent group ID (empty string for root)
 * @param {boolean} firstWordLow - Modbus DEVICE_FIRST_WORD_LOW flag
 * @param {boolean} firstDWordLow - Modbus DEVICE_FIRST_DWORD_LOW flag
 */
function processTagGroups(groups, device, fuxaType, parentGroupId, firstWordLow, firstDWordLow) {
    if (!groups || !Array.isArray(groups)) return;

    for (const group of groups) {
        const groupName = group['common.ALLTYPES_NAME'];
        if (!groupName) continue;

        const groupId = getGUID(TAG_GROUP_PREFIX);
        device.tagGroups[groupId] = {
            id: groupId,
            parentId: parentGroupId,
            deviceId: device.id,
            name: groupName
        };

        // Process tags within this group
        const groupTags = group.tags || [];
        for (const kepTag of groupTags) {
            const tag = convertTag(kepTag, fuxaType, firstWordLow, firstDWordLow);
            if (tag) {
                tag.groupId = groupId;
                device.tags[tag.id] = tag;
            }
        }

        // Recurse into nested tag_groups
        const nestedGroups = group.tag_groups || [];
        processTagGroups(nestedGroups, device, fuxaType, groupId, firstWordLow, firstDWordLow);
    }
}

// ─── S7 Property ─────────────────────────────────────────────────────────────

function fillS7Property(device, kepDevice) {
    const address = kepDevice['servermain.DEVICE_ID_STRING'] || '';
    const rack = kepDevice['siemens_tcpip_ethernet.DEVICE_S7_COMMUNICATIONS_CPU_RACK'];
    const slot = kepDevice['siemens_tcpip_ethernet.DEVICE_S7_COMMUNICATIONS_CPU_SLOT'];
    const model = kepDevice['servermain.DEVICE_MODEL'];
    const cpuType = S7_MODEL_MAP[model] || 'S71200';

    device.property = {
        address: address,
        port: '102',
        rack: rack != null ? String(rack) : '0',
        slot: slot != null ? String(slot) : '0',
        cpuType: cpuType
    };
}

// ─── Modbus Property ─────────────────────────────────────────────────────────

function fillModbusProperty(device, kepDevice) {
    // KepServer DEVICE_ID_STRING format: "<IP>.UNIT_ID"
    const idString = kepDevice['servermain.DEVICE_ID_STRING'] || '';
    let ip = '';
    let slaveId = '1';

    // Parse format like "<192.168.5.61>.0"
    const match = idString.match(/^<([^>]+)>\.(\d+)$/);
    if (match) {
        ip = match[1];
        slaveId = match[2];
    } else {
        // Try plain IP format
        ip = idString;
    }

    const port = kepDevice['modbus_ethernet.DEVICE_ETHERNET_PORT_NUMBER'] || 502;

    device.property = {
        address: ip,
        port: String(port),
        slaveid: slaveId,
        baudrate: '9600',
        databits: '8',
        stopbits: '1',
        parity: 'None',
        options: '',
        method: '',
        format: ''
    };
}

// ─── Tag Conversion ──────────────────────────────────────────────────────────

function convertTag(kepTag, fuxaType, firstWordLow, firstDWordLow) {
    const tagName = kepTag['common.ALLTYPES_NAME'];
    const tagDesc = kepTag['common.ALLTYPES_DESCRIPTION'] || '';
    const tagAddress = kepTag['servermain.TAG_ADDRESS'] || '';
    const tagDataType = kepTag['servermain.TAG_DATA_TYPE'];
    const tagAccess = kepTag['servermain.TAG_READ_WRITE_ACCESS']; // 0=RO, 1=RW
    const scalingType = kepTag['servermain.TAG_SCALING_TYPE'];

    if (!tagName || !tagAddress) return null;

    let type, address, memaddress;

    if (fuxaType === 'SiemensS7') {
        type = S7_TAG_TYPE_MAP[tagDataType] || 'Word';
        address = tagAddress; // S7 addresses like VW304, VD358, I0.5, M0.0 are used directly
        memaddress = '';
    } else if (fuxaType === 'ModbusTCP') {
        type = MODBUS_TAG_TYPE_MAP[tagDataType] || 'UInt16';
        const parsed = parseModbusAddress(tagAddress, tagDataType);
        if (!parsed) return null;
        address = parsed.address;
        memaddress = parsed.memaddress;
        // For coil/discrete areas, force Boolean type
        if (memaddress === '0' || memaddress === '100000') {
            type = 'Boolean';
        } else {
            // Apply MLE byte order based on device-level KepServer settings
            if (firstWordLow && MODBUS_MLE_WORD_MAP[type]) {
                type = MODBUS_MLE_WORD_MAP[type];
            }
            if (firstDWordLow && MODBUS_MLE_DWORD_MAP[type]) {
                type = MODBUS_MLE_DWORD_MAP[type];
            }
        }
    } else {
        return null;
    }

    const tag = {
        id: getGUID(TAG_PREFIX),
        name: tagName,
        label: '',
        type: type,
        address: address,
        memaddress: memaddress,
        description: tagDesc,
        divisor: 0,
        init: '',
        format: 0,
        daq: {
            enabled: false,
            changed: false,
            interval: 60,
            restored: false
        }
    };

    // Access: 0=RO (not stored in FUXA as separate field, skip)
    // Scaling
    if (scalingType === 1) {
        tag.scale = buildLinearScale(kepTag);
    }

    return tag;
}

// ─── Modbus Address Parser ───────────────────────────────────────────────────

/**
 * Parse KepServer Modbus address string.
 * Format: <area_digit><register_number> (e.g., "441155" → area=4, register=41155)
 * @returns {{ memaddress: string, address: string }} or null
 */
function parseModbusAddress(addrStr, dataType) {
    if (!addrStr || addrStr.length < 2) return null;

    const areaChar = addrStr.charAt(0);
    const fuxaMemaddress = MODBUS_AREA_MAP[areaChar];
    if (fuxaMemaddress === undefined) return null;

    const registerStr = addrStr.substring(1);
    const registerNum = parseInt(registerStr, 10);
    if (isNaN(registerNum) || registerNum < 1) return null;

    return {
        memaddress: fuxaMemaddress,
        address: String(registerNum)
    };
}

// ─── Scaling ─────────────────────────────────────────────────────────────────

function buildLinearScale(kepTag) {
    return {
        mode: 'linear',
        rawLow: kepTag['servermain.TAG_SCALING_RAW_LOW'] || 0,
        rawHigh: kepTag['servermain.TAG_SCALING_RAW_HIGH'] || 0,
        scaledLow: kepTag['servermain.TAG_SCALING_SCALED_LOW'] || 0,
        scaledHigh: kepTag['servermain.TAG_SCALING_SCALED_HIGH'] || 0,
        dateTimeFormat: null,
        readExpression: null,
        writeExpression: null
    };
}

// ─── JSON Comment Stripper ───────────────────────────────────────────────────

/**
 * Strip BOM and single-line comments (//...) from JSON text.
 * KepServer export files may contain BOM and comments that are invalid JSON.
 */
function stripJsonComments(text) {
    // Strip BOM
    if (text.charCodeAt(0) === 0xFEFF) {
        text = text.slice(1);
    }
    // Remove single-line comments (// ...) but not inside strings
    let result = '';
    let inString = false;
    let escaped = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (escaped) {
            result += ch;
            escaped = false;
            continue;
        }
        if (ch === '\\' && inString) {
            result += ch;
            escaped = true;
            continue;
        }
        if (ch === '"') {
            inString = !inString;
            result += ch;
            continue;
        }
        if (!inString && ch === '/' && i + 1 < text.length && text[i + 1] === '/') {
            // Skip until end of line
            while (i < text.length && text[i] !== '\n') {
                i++;
            }
            // Keep the newline
            if (i < text.length) {
                result += '\n';
            }
            continue;
        }
        result += ch;
    }
    return result;
}

// ─── Exports ─────────────────────────────────────────────────────────────────

module.exports = {
    convertKepserverToFuxa,
    stripJsonComments
};
