/**
 * 'FuxaServer': FUXA as device to use with the scripts
 */
 'use strict';

const utils = require('../../utils');
const deviceUtils = require('../device-utils');

function FuxaServer(_data, _logger, _events, _manager) {

    var data = JSON.parse(JSON.stringify(_data)); // Current Device data { id, name, tags, enabled, ... }
    var logger = _logger;
    var working = false;                // Working flag to manage overloading polling and connection
    var events = _events;               // Events to commit change to runtime
    var manager = _manager;             // Devices manager for cross-device tag access
    var varsValue = {};                 // Tags to send to frontend { id, type, value }
    var lastTimestampValue;             // Last Timestamp of values
    var tagsMap = {};                   // Map of tag id
    var overloading = 0;                // Overloading counter to mange the break connection
    var tocheck = false;                // Flag that define if there are tags to check by polling
    var connectionTags = [];            // Tags of connection status of devices
    var calculatedTags = {};            // Calculated tags with compiled expressions
    var type;

    /**
     * initialize the server device type
     */
    this.init = function (_type) {
        type = _type;
    }

    /**
     * Connected with itself
     */
    this.connect = function () {
        return new Promise(async function (resolve, reject) {
            resolve();
        });
    }


    /**
     * Disconnect with itself
     * Clear all Tags values
     */
    this.disconnect = function () {
        return new Promise(function (resolve, reject) {
            _clearVarsValue();
            resolve(true);
        });
    }

    /**
     * Read values in polling mode
     * Update the tags values list, save in DAQ if value changed or in interval and emit values to clients
     */
    this.polling = async function () {
        if (_checkWorking(true)) {
            try {
                if (tocheck) {
                    var varsValueChanged = await _checkVarsChanged();
                    lastTimestampValue = new Date().getTime();
                    _emitValues(varsValue);
                    if (this.addDaq && !utils.isEmptyObject(varsValueChanged)) {
                        this.addDaq(varsValueChanged, data.name, data.id);
                    }
                }
                _checkConnectionStatus();
            } catch (err) {
                logger.error(`'${data.name}' polling error: ${err}`);
            }
            _checkWorking(false);
        }
    }

    /**
     * Load Tags attribute to read with polling
     */
    this.load = function (_data) {
        varsValue = [];
        data = JSON.parse(JSON.stringify(_data));
        data.tags = data.tags || {};
        tagsMap = {};
        calculatedTags = {};
        var count = Object.keys(data.tags).length;
        connectionTags = [];
        for (var id in data.tags) {
            tagsMap[id] = data.tags[id];
            const dataTag = data.tags[id];

            if (dataTag.type === 'calculated') {
                // Calculated tag: compile expression, value starts as null
                data.tags[id].value = null;
                data.tags[id].access = 'ro';
                var compiled = _compileExpression(dataTag);
                if (compiled) {
                    calculatedTags[id] = compiled;
                }
            } else if (dataTag.sysType === TagSystemTypeEnum.deviceConnectionStatus) {
                data.tags[id].timestamp = Date.now();
                connectionTags.push(data.tags[id]);
                const shouldRestore = dataTag.daq && dataTag.daq.restored;
                if (shouldRestore) {
                    data.tags[id].value = null;
                } else if (dataTag.init !== undefined && dataTag.init !== null && dataTag.init !== '') {
                    data.tags[id].value = deviceUtils.parseValue(dataTag.init, dataTag.type);
                } else {
                    if (dataTag.type === 'boolean') {
                        data.tags[id].value = false;
                    } else if (dataTag.type === 'number') {
                        data.tags[id].value = 0;
                    } else if (dataTag.type === 'string') {
                        data.tags[id].value = '';
                    } else {
                        data.tags[id].value = null;
                    }
                }
            } else {
                const shouldRestore = dataTag.daq && dataTag.daq.restored;
                if (shouldRestore) {
                    data.tags[id].value = null;
                } else if (dataTag.init !== undefined && dataTag.init !== null && dataTag.init !== '') {
                    data.tags[id].value = deviceUtils.parseValue(dataTag.init, dataTag.type);
                } else {
                    if (dataTag.type === 'boolean') {
                        data.tags[id].value = false;
                    } else if (dataTag.type === 'number') {
                        data.tags[id].value = 0;
                    } else if (dataTag.type === 'string') {
                        data.tags[id].value = '';
                    } else {
                        data.tags[id].value = null;
                    }
                }
            }
            varsValue[id] = data.tags[id];
        }
        tocheck = !utils.isEmptyObject(data.tags);
        var calcCount = Object.keys(calculatedTags).length;
        logger.info(`'${data.name}' data loaded (${count}, calculated: ${calcCount})`, true);
    }

    /**
     * Return Tags values array { id: <tagId>, value: <value> }
     */
    this.getValues = function () {
        return data.tags;
    }

    /**
     * Return Tag value { id: <tagId>, value: <value>, ts: <lastTimestampValue> }
     */
    this.getValue = function (id) {
        if (varsValue[id]) {
            return { id: id, value: varsValue[id].value, ts: lastTimestampValue };
        }
        return null;
    }

    /**
     * Return connection status FUXA server is always connected, 'connect-ok'
     */
    this.getStatus = function () {
        return 'connect-ok';
    }

    /**
     * Return Tag property to show in frontend
     */
    this.getTagProperty = function (id) {
        if (data.tags[id]) {
            return { id: id, name: data.tags[id].name, type: data.tags[id].type, format: data.tags[id].format };
        } else {
            return null;
        }
    }

    /**
     * Set the Tag value to device
     */
    this.setValue = function (id, value) {
        if (varsValue[id]) {
            // Reject writes to calculated tags (read-only)
            if (varsValue[id].type === 'calculated') {
                logger.warn(`'${data.name}' setValue rejected: tag '${id}' is calculated (read-only)`);
                return false;
            }
            var val = _parseValue(value, varsValue[id].type);
            varsValue[id].value = val;
            varsValue[id].changed = true;
            logger.info(`'${data.name}' setValue(${id}, ${value})`, true, true);
            return true;
        }
        return false;
    }

    /**
     * Set the connection status to tag of device sttus
     * @param {*} deviceId
     * @param {*} status
     */
    this.setConnectionStatus = function(deviceId, status) {
        var tag = connectionTags.find(tag => tag.memaddress === deviceId);
        if (tag) {
            tag.value = status;
            tag.timestamp = Date.now();
        }
    }

    /**
     * Return connected with itself
     */
    this.isConnected = function () {
        return true;
    }

    /**
     * Bind the DAQ store function
     */
    this.bindAddDaq = function (fnc) {
        this.addDaq = fnc;                         // Add the DAQ value to db history
    }
    this.addDaq = null;

    /**
     * Return the timestamp of last read tag operation on polling
     * @returns
     */
     this.lastReadTimestamp = () => {
        return lastTimestampValue;
    }

    /**
     * Return the Daq settings of Tag
     * @returns
     */
    this.getTagDaqSettings = (tagId) => {
        return data.tags[tagId] ? data.tags[tagId].daq : null;
    }

    /**
     * Set Daq settings of Tag
     * @returns
     */
    this.setTagDaqSettings = (tagId, settings) => {
        if (data.tags[tagId]) {
            utils.mergeObjectsValues(data.tags[tagId].daq, settings);
        }
    }

    /**
     * Cheack and parse the value return converted value
     * @param {*} value as string
     */
    var _parseValue = function (value, type) {
        if (type === 'number') {
            return parseFloat(value);
        } else if (type === 'boolean') {
            // Handle null, undefined, empty string cases
            if (value === null || value === undefined || value === '') {
                return false; // Default to false for safety
            }
            if (typeof value === 'string') {
                // Properly handle boolean string values
                const lowerValue = value.toLowerCase().trim();
                return lowerValue === 'true' || lowerValue === '1';
            }
            return Boolean(value);
        } else if (type === 'string') {
            return value;
        } else {
            let val = parseFloat(value);
            if (Number.isNaN(val)) {
                // maybe boolean
                val = Number(value);
                // maybe string
                if (Number.isNaN(val)) {
                    val = value;
                }
            } else {
                val = parseFloat(val.toFixed(5));
            }
            return val;
        }
    }

    /**
     * Clear Tags value
     */
    var _clearVarsValue = function () {
        for (var id in varsValue) {
            varsValue[id].value = null;
        }
        _emitValues(varsValue);
    }

    /**
     * Return the Tags that have value changed and clear value changed flag of all Tags
     * Two-pass: first process normal tags, then evaluate calculated tags
     */
    var _checkVarsChanged = async () => {
        const timestamp = new Date().getTime();
        var result = {};

        // Pass 1: process non-calculated tags
        for (var id in data.tags) {
            if (data.tags[id].type === 'calculated') continue;
            if (!utils.isNullOrUndefined(data.tags[id].value)) {
                data.tags[id].value = await deviceUtils.tagValueCompose(data.tags[id].value, varsValue[id] ? varsValue[id].value : null, data.tags[id]);
                data.tags[id].timestamp = timestamp;
                if (this.addDaq && deviceUtils.tagDaqToSave(data.tags[id], timestamp)) {
                    result[id] = data.tags[id];
                }
            }
            data.tags[id].changed = false;
            varsValue[id] = data.tags[id];
        }

        // Pass 2: evaluate calculated tags
        for (var calcId in calculatedTags) {
            var calcInfo = calculatedTags[calcId];
            var tag = data.tags[calcId];
            if (!tag) continue;

            var badQualityMode = (tag.options && tag.options.badQualityMode) || 0;
            var values = [];
            var allGood = true;

            for (var i = 0; i < calcInfo.dependencies.length; i++) {
                var depId = calcInfo.dependencies[i];
                var depResult = manager ? manager.getTagValue(depId, true) : null;
                var depValue = depResult ? depResult.value : null;
                var isGood = !_isBadQuality(depValue);

                if (!isGood) {
                    allGood = false;
                    if (badQualityMode === 1) {
                        depValue = 0;
                    } else if (badQualityMode === 2) {
                        depValue = calcInfo.lastGoodValues[depId] !== undefined ? calcInfo.lastGoodValues[depId] : 0;
                    }
                    // mode 0: will mark result as null below
                } else {
                    // Update last good value
                    calcInfo.lastGoodValues[depId] = depValue;
                }
                values.push(depValue === null ? 0 : parseFloat(depValue));
            }

            if (!allGood && badQualityMode === 0) {
                // Mode 0: any bad dependency → result is null (bad quality)
                tag.value = null;
            } else {
                try {
                    tag.value = calcInfo.evalFn.apply(null, values);
                    if (typeof tag.value === 'number' && !isFinite(tag.value)) {
                        tag.value = null; // Infinity or NaN
                    }
                } catch (e) {
                    logger.error(`'${data.name}' calculated tag '${calcId}' eval error: ${e.message || e}`);
                    tag.value = null;
                }
            }
            tag.timestamp = timestamp;
            tag.changed = true;
            if (this.addDaq && deviceUtils.tagDaqToSave(tag, timestamp)) {
                result[calcId] = tag;
            }
            varsValue[calcId] = tag;
        }

        return result;
    }

    /**
     * Check if a value indicates bad quality
     */
    var _isBadQuality = function (value) {
        return value === null || value === undefined || (typeof value === 'number' && !isFinite(value));
    }

    /**
     * Compile a calculated tag's expression at load time.
     * Replaces {DeviceName.TagName} references with parameter placeholders,
     * then creates a Function for fast evaluation during polling.
     * @param {object} dataTag - the tag object with .expression
     * @returns {object|null} - { evalFn, dependencies, lastGoodValues } or null on failure
     */
    var _compileExpression = function (dataTag) {
        var expr = dataTag.expression;
        if (!expr || typeof expr !== 'string' || !expr.trim()) {
            logger.warn(`'${data.name}' calculated tag '${dataTag.name}' has no expression`);
            return null;
        }

        // Safety check: only allow safe characters
        var SAFE_EXPR = /^[\d\s+\-*/()._%a-zA-Z{}]+$/;
        if (!SAFE_EXPR.test(expr)) {
            logger.error(`'${data.name}' calculated tag '${dataTag.name}' expression contains unsafe characters`);
            return null;
        }

        var dependencies = [];
        var paramNames = [];
        var compiledExpr = expr;
        var paramIndex = 0;

        // Match {DeviceName.TagName} patterns
        var refRegex = /\{([^}]+)\.([^}]+)\}/g;
        var match;
        var seen = {};

        while ((match = refRegex.exec(expr)) !== null) {
            var deviceName = match[1];
            var tagName = match[2];
            var refKey = deviceName + '.' + tagName;

            if (!seen[refKey]) {
                var tagId = manager ? manager.getTagId(tagName, deviceName) : null;
                if (!tagId) {
                    logger.error(`'${data.name}' calculated tag '${dataTag.name}': cannot resolve '${refKey}'`);
                    return null;
                }
                var paramName = '_p' + paramIndex;
                seen[refKey] = paramName;
                dependencies.push(tagId);
                paramNames.push(paramName);
                paramIndex++;
            }
            // Replace all occurrences of this reference with the parameter name
            var escapedRef = match[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            compiledExpr = compiledExpr.replace(new RegExp(escapedRef, 'g'), seen[refKey]);
        }

        if (dependencies.length === 0) {
            logger.warn(`'${data.name}' calculated tag '${dataTag.name}' expression has no tag references`);
            return null;
        }

        try {
            var fnBody = 'return (' + compiledExpr + ');';
            var evalFn = new Function(paramNames.join(','), fnBody);
            return {
                evalFn: evalFn,
                dependencies: dependencies,
                lastGoodValues: {}
            };
        } catch (e) {
            logger.error(`'${data.name}' calculated tag '${dataTag.name}' expression compile error: ${e.message || e}`);
            return null;
        }
    }
    /**
     * Emit the Tags values array { id: <name>, value: <value>, type: <type> }
     * @param {*} values
     */
    var _emitValues = function (values) {
        events.emit('device-value:changed', { id: data.name, values: values });
    }

    /**
     * Used to manage the async connection and polling automation (that not overloading)
     * @param {*} check
     */
    var _checkWorking = function (check) {
        if (check && working) {
            overloading++;
            logger.warn(`'${data.name}' working (connection || polling) overload! ${overloading}`);
            // !The driver don't give the break connection
            if (overloading >= 3) {
                disconnect();
            } else {
                return false;
            }
        }
        working = check;
        overloading = 0;
        return true;
    }

    var _checkConnectionStatus = function () {
        var dt = Date.now() - 60000;
        connectionTags.forEach(tag => {
            if (tag.value && tag.timestamp < dt) {
                tag.value = 0;
            }
        });
    }

}

module.exports = {
    init: function (settings) {
    },
    create: function (data, logger, events, manager) {
        return new FuxaServer(data, logger, events, manager);
    }
}

var TagSystemTypeEnum  = {
    deviceConnectionStatus: 1,
}