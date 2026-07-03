/**
 * 's7': snap7 wrapper to communicate with Siemens PLC (S7)
 */

var snap7;
var datatypes;
const utils = require('../../utils');
const deviceUtils = require('../device-utils');

const MIX_GAP_THRESHOLD = 100; // Gap threshold in bytes for splitting ReadArea batches
const MAX_PDU_TOTAL = 230;  // Max total PDU bytes for ReadMultiVars (includes per-item overhead, conservative for S7-300 PDU=240)
const READ_MULTI_VARS_OVERHEAD = 12;  // Per-item protocol overhead in ReadMultiVars (address spec bytes)
const MAX_DB_READ = 200;  // Max bytes per single DBRead request
const MAX_MULTI_VARS_ITEMS = 20;  // snap7 hard limit: max 20 items per ReadMultiVars call

function S7client(_data, _logger, _events, _runtime) {

    var runtime = _runtime;
    var db = {};                        // Loaded Signal in DB format { DB index, start, size, ... }
    var data = JSON.parse(JSON.stringify(_data));                   // Current Device data { id, name, tags, enabled, ... }
    var logger = _logger;               // Logger
    var s7client = new snap7.S7Client();// Client node-S7
    var working = false;                // Working flag to manage overloading polling and connection
    var events = _events;               // Events to commit change to runtime
    var lastStatus = '';                // Last Device status
    var varsValue = [];                 // Signale to send to frontend { id, type, value }
    var dbItemsMap = {};                // DB Mapped Signale name with DbItem to find for set value
    var mixItemsMap = {};               // E/I/A/Q/M Mapped Signale name to find for read in polling and set value
    var overloading = 0;                // Overloading counter to mange the break connection
    var lastTimestampValue;             // Last Timestamp of asked values
    var useReadArea = false;            // For S7-200/Smart: use ReadArea instead of ReadMultiVars

    /**
     * Chunk mix items for ReadMultiVars, accounting for per-item protocol overhead
     * PDU consumption per item = READ_MULTI_VARS_OVERHEAD (12 bytes) + data_bytes
     * Total PDU must stay under MAX_PDU_TOTAL; item count must stay under MAX_MULTI_VARS_ITEMS
     * @param {array} items - mix items with .type property
     */
    var _chunkBySize = function (items) {
        var chunks = [];
        var current = [];
        var currentPdu = 0;
        for (var i = 0; i < items.length; i++) {
            var dataBytes = datatypes[items[i].type] ? datatypes[items[i].type].bytes : 1;
            var itemPdu = READ_MULTI_VARS_OVERHEAD + dataBytes;
            if (current.length > 0 && (currentPdu + itemPdu > MAX_PDU_TOTAL || current.length >= MAX_MULTI_VARS_ITEMS)) {
                chunks.push(current);
                current = [];
                currentPdu = 0;
            }
            current.push(items[i]);
            currentPdu += itemPdu;
        }
        if (current.length > 0) chunks.push(current);
        return chunks;
    }

    /**
     * Connect to PLC
     * Emit connection status to clients, clear all Tags values
     */
    this.connect = function () {
        return new Promise(function (resolve, reject) {
            var cpuType = data.property ? data.property.cpuType : '';
            useReadArea = (cpuType === 'S7200' || cpuType === 'S7200Smart');
            logger.info(`'${data.name}' connect: cpuType=${cpuType}, useReadArea=${useReadArea}`, true);
            var canConnect = false;
            if (cpuType === 'S7200' || cpuType === 'S7200Smart') {
                // S7-200/S7-200 Smart: no rack/slot needed
                canConnect = !!(data.property && data.property.address);
            } else {
                canConnect = !!(data.property && data.property.rack >= 0 && data.property.slot >= 0);
            }
            if (canConnect) {
                try {
                    if (!s7client.Connected() && _checkWorking(true)) {
                        logger.info(`'${data.name}' try to connect ${data.property.address} (cpuType: ${cpuType || 'default'})`, true);
                        _connectByType(cpuType, function (err) {
                            if (err) {
                                logger.error(`'${data.name}' connect failed! ${err}`);
                                _emitStatus('connect-error');
                                _clearVarsValue();
                                reject();
                            } else {
                                logger.info(`'${data.name}' connected!`, true);
                                _emitStatus('connect-ok');
                                resolve();
                            }
                            _checkWorking(false);
                        });
                    } else {
                        reject();
                    }
                } catch (err) {
                    logger.error(`'${data.name}' try to connect error! ${err}`);
                    _checkWorking(false);
                    _emitStatus('connect-error');
                    _clearVarsValue();
                    reject();
                }
            } else {
                logger.error(`'${data.name}' missing connection data!`);
                _emitStatus('connect-failed');
                _clearVarsValue();
                reject();
            }
        });
    }

    /**
     * Connect to PLC based on CPU type
     * S7-200: uses SetConnectionParams with TSAP
     * S7-200 Smart: uses SetConnectionParams with TSAP (local=0x0100, remote=0x0200)
     * Others: standard ConnectTo(ip, rack, slot)
     */
    var _connectByType = function (cpuType, callback) {
        var address = data.property.address;
        if (cpuType === 'S7200') {
            // S7-200: connect via TSAP (local=0x1000, remote=0x1000)
            s7client.SetConnectionParams(address, 0x1000, 0x1000);
            s7client.Connect(callback);
        } else if (cpuType === 'S7200Smart') {
            // S7-200 Smart: connect via TSAP (local=0x0100, remote=0x0200)
            s7client.SetConnectionParams(address, 0x0100, 0x0200);
            s7client.Connect(callback);
        } else {
            // S7-300/400/1200/1500: standard connection
            s7client.ConnectTo(address, parseInt(data.property.rack), parseInt(data.property.slot), callback);
        }
    }

    /**
     * Disconnect the PLC
     * Emit connection status to clients, clear all Tags values
     */
    this.disconnect = function () {
        return new Promise(function (resolve, reject) {
            _checkWorking(false);
            if (!s7client.Connected()) {
                _emitStatus('connect-off');
                _clearVarsValue();
                resolve(true);
            } else {
                var result = s7client.Disconnect();
                if (result) {
                    logger.info(`'${data.name}' disconnected!`, true);
                } else {
                    logger.error(`'${data.name}' try to disconnect failed!`);
                }
                _emitStatus('connect-off');
                _clearVarsValue();
                resolve(result);
            }
        });
    }

    /**
     * Read values in polling mode
     * Update the tags values list, save in DAQ if value changed or in interval and emit values to clients
     */
    this.polling = async function () {
        if (_checkWorking(true)) {
            // Pre-check: if snap7 already knows connection is broken, skip reads immediately
            if (!s7client.Connected()) {
                _checkWorking(false);
                logger.error(`'${data.name}' polling: connection already lost (Connected()=false)`);
                _emitStatus('connect-error');
                _clearVarsValue();
                return;
            }
            var readVarsfnc = [];
            logger.info(`'${data.name}' polling: db groups=${JSON.stringify(Object.keys(db))}, mixItems=${Object.keys(mixItemsMap).length}, useReadArea=${useReadArea}`, true, true);
            for (var dbnum in db) {
                readVarsfnc.push(_readDB(parseInt(dbnum), Object.values(db[dbnum].Items)).catch(err => {
                    logger.error(`'${data.name}' _readDB(${dbnum}) error: ${err.message || err}`);
                    return [];
                }));
            }
            if (Object.keys(mixItemsMap).length) {
                if (useReadArea) {
                    // S7-200/S7-200 Smart: use ReadArea per area group (more reliable)
                    readVarsfnc.push(_readMixByArea(Object.values(mixItemsMap)).catch(err => {
                        logger.error(`'${data.name}' _readMixByArea error: ${err.message || err}`);
                        return [];
                    }));
                } else {
                    // S7-300/400/1200/1500: use ReadMultiVars (efficient multi-read)
                    // Chunk by total data size to avoid exceeding PLC PDU limit
                    _chunkBySize(Object.values(mixItemsMap)).forEach((chunk) => {
                        readVarsfnc.push(_readVars(chunk).catch(err => {
                            logger.error(`'${data.name}' _readVars error: ${err.message || err}`);
                            return [];
                        }));
                    });
                }
            }
            try {
                const result = await Promise.all(readVarsfnc);
                _checkWorking(false);
                // Check if any read actually returned data
                var hasData = result.some(items => items.length > 0);
                if (hasData) {
                    let varsValueChanged = await _updateVarsValue(result);
                    lastTimestampValue = new Date().getTime();
                    _emitValues(varsValue);
                    if (this.addDaq && !utils.isEmptyObject(varsValueChanged)) {
                        this.addDaq(varsValueChanged, data.name, data.id);
                    }
                    if (lastStatus !== 'connect-ok') {
                        _emitStatus('connect-ok');
                    }
                } else if (readVarsfnc.length > 0) {
                    // All reads failed — connection is likely broken
                    logger.error(`'${data.name}' all read operations failed, connection lost`);
                    _emitStatus('connect-error');
                    _clearVarsValue();
                }
            } catch (reason) {
                if (reason && reason.stack) {
                    logger.error(`'${data.name}' _readVars error! ${reason.stack}`);
                } else {
                    logger.error(`'${data.name}' _readVars error! ${reason}`);
                }
                _checkWorking(false);
                _emitStatus('connect-error');
                _clearVarsValue();
            };
        } else {
            _emitStatus('connect-busy');
        }
    }

    /**
     * Load Tags attribute to read with polling
     */
    this.load = function (_data) {
        data = JSON.parse(JSON.stringify(_data));
        db = {};
        varsValue = [];
        dbItemsMap = {};
        mixItemsMap = {};
        var count = 0;
        var mixCount = 0;
        for (var id in data.tags) {
            try {
                var varDb = _getTagItem(data.tags[id]);
                if (varDb instanceof DbItem) {
                    if (!db[varDb.dbnum]) {
                        var grptag = new DbItems(varDb.dbnum);
                        db[varDb.dbnum] = grptag;
                    }
                    if (!db[varDb.dbnum].Items[varDb.Start]) {
                        db[varDb.dbnum].Items[varDb.Start] = varDb;
                    }
                    db[varDb.dbnum].Items[varDb.Start].Tags.push(data.tags[id]); // because you can have multiple tags at the same DB address
                    if (db[varDb.dbnum].MaxSize < varDb.Start + datatypes[varDb.type].S7WordLen) {
                        db[varDb.dbnum].MaxSize = varDb.Start + datatypes[varDb.type].S7WordLen;
                    }
                    // check Bit to Map
                    if (varDb.bit >= 0) {
                        varDb.BitMap[varDb.bit] = id;
                    }
                    count++;
                    dbItemsMap[id] = db[varDb.dbnum].Items[varDb.Start];
                    dbItemsMap[id].format = data.tags[id].format;
                } else if (varDb && !isNaN(varDb.Start)) {
                    varDb.id = id;
                    varDb.name = data.tags[id].name;
                    varDb.format = data.tags[id].format;
                    varDb.daq = data.tags[id].daq;
                    mixItemsMap[id] = varDb;
                    mixCount++;
                } else {
                    logger.warn(`'${data.name}' tag '${data.tags[id].name}' address '${data.tags[id].address}' type '${data.tags[id].type}' could not be parsed (returned: ${JSON.stringify(varDb)})`);
                }
            } catch (err) {
                logger.error(`'${data.name}' load error! ${err}`);
            }
        }
        logger.info(`'${data.name}' data loaded (DB tags: ${count}, Mix I/Q/M tags: ${mixCount}, DB groups: ${JSON.stringify(Object.keys(db))})`, true);
    }

    /**
     * Return Tags values array { id: <name>, value: <value>, type: <type> }
     */
    this.getValues = function () {
        return varsValue;
    }

    /**
     * Return Tag value { id: <name>, value: <value>, ts: <lastTimestampValue> }
     */
    this.getValue = function (id) {
        if (varsValue[id]) {
            return { id: id, value: varsValue[id].value, ts: lastTimestampValue };
        }
        return null;
    }

    /**
     * Return connection status 'connect-off', 'connect-ok', 'connect-error'
     */
    this.getStatus = function () {
        return lastStatus;
    }

    /**
     * Return Tag property
     */
    this.getTagProperty = function (id) {
        if (dbItemsMap[id]) {
            return { id: id, name: id, type: dbItemsMap[id].type, format: dbItemsMap[id].format };
        } else if (mixItemsMap[id]) {
            return { id: id, name: id, type: mixItemsMap[id].type, format: mixItemsMap[id].format };
        } else {
            return null;
        }
    }

    /**
     * Set the Tag value
     * Read the current Tag object, write the value in object and send to SPS
     */
    this.setValue = async function (sigid, value) {
        var item = _getTagItem(data.tags[sigid]);
        if (item) {
            value = await deviceUtils.tagRawCalculator(value, data.tags[sigid], runtime);
            item.value = value;
            var writePromise;
            if (item instanceof DbItem) {
                writePromise = _writeVars([item]);
            } else if (useReadArea) {
                // S7-200/Smart: use WriteArea for mix items (I/Q/M)
                writePromise = _writeVarByArea(item);
            } else {
                writePromise = _writeVars([item]);
            }
            writePromise.then(result => {
                logger.info(`'${data.name}' setValue(${sigid}, ${value})`, true, true);
            }, reason => {
                if (reason && reason.stack) {
                    logger.error(`'${data.name}' _writeVars error! ${reason.stack}`);
                } else {
                    logger.error(`'${data.name}' _writeVars error! ${reason}`);
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Return if PLC is connected
     * Don't work if PLC will disconnect
     */
    this.isConnected = function () {
        return s7client.Connected();
    }

    /**
     * Bind the DAQ store function
     */
    this.bindAddDaq = function (fnc) {
        this.addDaq = fnc;                         // Add the DAQ value to db history
    }

    this.addDaq = null;                             // Add the DAQ value to db history

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
     * Clear the Tags values by setting to null
     * Emit to clients
     */
    var _clearVarsValue = function () {
        for (var id in varsValue) {
            varsValue[id].value = null;
        }
        for (var dbid in db) {
            for (var itemid in db[dbid].Items) {
                db[dbid].Items[itemid].value = null;
            }
        }
        for (var mi in mixItemsMap) {
            mixItemsMap[mi].value = null;
        }
        _emitValues(varsValue);
    }

    /**
     * Update the Tags values read
     * @param {*} vars
     */
    var _updateVarsValue = async (vars) => {
        var someval = false;
        var tempTags = {};
        for (var vid in vars) {
            let items = vars[vid];
            for (var itemidx in items) {
                const changed = items[itemidx].changed;
                if (items[itemidx] instanceof DbItem) {
                    let type = items[itemidx].type;
                    let value = items[itemidx].value;
                    let tags = items[itemidx].Tags;
                    tags.forEach(tag => {
                        tempTags[tag.id] = {
                            id: tag.id,
                            rawValue: value,
                            type: type,
                            daq: tag.daq,
                            changed: changed,
                            tagref: tag
                        };
                        if (type === 'BOOL') {
                            try {
                                let pos = parseInt(tag.address.charAt(tag.address.length - 1));
                                tempTags[tag.id].rawValue = _getBit(value, pos) ? true : false;
                            } catch (err) { }
                        }
                        someval = true;
                    });
                } else {
                    if (items[itemidx].type === 'BOOL') {
                        try {
                            items[itemidx].value = (_getBit(items[itemidx].value, items[itemidx].bit)) ? true : false;
                        } catch (err) { }
                    }
                    tempTags[items[itemidx].id] = {
                        id: items[itemidx].id,
                        rawValue: items[itemidx].value,
                        type: items[itemidx].type,
                        daq: items[itemidx].daq,
                        changed: changed,
                        tagref: data.tags[items[itemidx].id] // <--- Passes full tag config including scaling
                    };
                    someval = true;
                }
            }
        }
        if (someval) {
            const timestamp = new Date().getTime();
            var result = {};
            for (var id in tempTags) {
                if (!utils.isNullOrUndefined(tempTags[id].rawValue)) {
                    tempTags[id].value = await deviceUtils.tagValueCompose(tempTags[id].rawValue, varsValue[id] ? varsValue[id].value : null, tempTags[id].tagref, runtime);
                    tempTags[id].timestamp = timestamp;
                    if (this.addDaq && deviceUtils.tagDaqToSave(tempTags[id], timestamp)) {
                        result[id] = tempTags[id];
                    }
                }
                varsValue[id] = tempTags[id];
                varsValue[id].changed = false;
            }
            return result;
        }
        return null;
    }

    //#region Bit Manipolation
    _getBit = function (number, bitPosition) {
        return ((number >> bitPosition) % 2 != 0)
    }

    _setBit = function (number, bitPosition) {
        return number | 1 << bitPosition;
    }

    _clearBit = function (number, bitPosition) {
        return number & ~(1 << bitPosition);
    }

    _toggleBit = function (number, bitPosition) {
        return _getBit(number, bitPosition) ? _clearBit(number, bitPosition) : _setBit(number, bitPosition);
    }
    //#endregion

    /**
     * Emit the PLC Tags values array { id: <name>, value: <value>, type: <type> }
     * @param {*} values
     */
    var _emitValues = function (values) {
        events.emit('device-value:changed', { id: data.id, values: values });
    }

    /**
     * Emit the PLC connection status
     * @param {*} status
     */
    var _emitStatus = function (status) {
        lastStatus = status;
        events.emit('device-status:changed', { id: data.id, status: status });
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
                s7client.Disconnect();
            } else {
                return false;
            }
        }
        working = check;
        overloading = 0;
        return true;
    }

    /**
     * Read a DB and parse the result
     * @param {int} DBNr - The DB Number to read
     * @param {array} vars - Array of Var objects
     * @param {int} vars[].start - Position of the first byte
     * @param {int} [vars[].bit] - Position of the bit in the byte
     * @param {Datatype} vars[].type - Data type (BYTE, WORD, INT, etc), see {@link /s7client/?api=Datatypes|Datatypes}
     * @returns {Promise} - Resolves to the vars array with populate *value* property
     */
    var _readDB = function (DBNr, vars) {
        return new Promise((resolve, reject) => {
            if (vars.length === 0) return resolve([]);

            let end = 0;
            let offset = Number.MAX_SAFE_INTEGER;
            vars.forEach(v => {
                if (v.Start < offset) offset = v.Start;
                if (end < v.Start + datatypes[v.type].bytes) {
                    end = v.Start + datatypes[v.type].bytes;
                }
            });
            var totalLen = end - offset;
            logger.info(`'${data.name}' _readDB: DBNr=${DBNr}, offset=${offset}, length=${totalLen}, vars=${vars.length}`, true, true);

            // Split into sub-ranges if total byte range exceeds DB read limit
            if (totalLen > MAX_DB_READ) {
                var sortedVars = vars.slice().sort((a, b) => a.Start - b.Start);
                var ranges = [];
                var rangeStart = sortedVars[0].Start;
                var rangeVars = [sortedVars[0]];
                for (var i = 1; i < sortedVars.length; i++) {
                    var v = sortedVars[i];
                    var vEnd = v.Start + datatypes[v.type].bytes;
                    if (vEnd - rangeStart > MAX_DB_READ) {
                        ranges.push({ start: rangeStart, vars: rangeVars });
                        rangeStart = v.Start;
                        rangeVars = [v];
                    } else {
                        rangeVars.push(v);
                    }
                }
                if (rangeVars.length > 0) ranges.push({ start: rangeStart, vars: rangeVars });

                logger.info(`'${data.name}' _readDB(${DBNr}): split into ${ranges.length} sub-ranges (total ${totalLen} bytes > DB read limit ${MAX_DB_READ})`, true, true);
                var rangePromises = ranges.map(r => {
                    var rEnd = 0;
                    r.vars.forEach(v => {
                        if (rEnd < v.Start + datatypes[v.type].bytes) rEnd = v.Start + datatypes[v.type].bytes;
                    });
                    return _readDBRange(DBNr, r.start, rEnd - r.start, r.vars);
                });
                return Promise.all(rangePromises).then(results => {
                    var allVars = [];
                    results.forEach(r => { allVars = allVars.concat(r); });
                    resolve(allVars);
                }).catch(err => reject(err));
            } else {
                _readDBRange(DBNr, offset, totalLen, vars).then(resolve).catch(reject);
            }
        });
    }

    /**
     * Read a single DB byte range and parse the result
     * If PDU size exceeded, automatically splits range in half and retries
     * @param {int} DBNr - DB number
     * @param {int} offset - Start byte offset
     * @param {int} length - Number of bytes to read
     * @param {array} vars - Vars within this range to parse
     * @param {number} _depth - internal recursion depth
     */
    var _readDBRange = function (DBNr, offset, length, vars, _depth) {
        _depth = _depth || 0;
        return new Promise((resolve, reject) => {
            s7client.DBRead(DBNr, offset, length, (err, res) => {
                if (err) {
                    var errText = s7client.ErrorText(err);
                    // PDU exceeded: split byte range in half and retry
                    if (errText.indexOf('PDU') >= 0 && length > 2 && _depth < 5) {
                        var halfLen = Math.ceil(length / 2);
                        var firstHalfVars = vars.filter(v => v.Start < offset + halfLen);
                        var secondHalfVars = vars.filter(v => v.Start >= offset + halfLen);
                        logger.warn(`'${data.name}' DBRead(${DBNr}) PDU exceeded (${length} bytes), splitting into ${halfLen}+${length - halfLen} (depth=${_depth})`);
                        var promises = [];
                        if (firstHalfVars.length > 0) {
                            var h1End = 0;
                            firstHalfVars.forEach(v => { if (v.Start + datatypes[v.type].bytes > h1End) h1End = v.Start + datatypes[v.type].bytes; });
                            promises.push(_readDBRange(DBNr, offset, h1End - offset, firstHalfVars, _depth + 1));
                        }
                        if (secondHalfVars.length > 0) {
                            var h2Start = secondHalfVars[0].Start;
                            var h2End = 0;
                            secondHalfVars.forEach(v => { if (v.Start + datatypes[v.type].bytes > h2End) h2End = v.Start + datatypes[v.type].bytes; });
                            promises.push(_readDBRange(DBNr, h2Start, h2End - h2Start, secondHalfVars, _depth + 1));
                        }
                        return Promise.all(promises).then(results => {
                            var merged = [];
                            results.forEach(r => { merged = merged.concat(r); });
                            resolve(merged);
                        }).catch(reject);
                    }
                    logger.error(`'${data.name}' DBRead(${DBNr}, ${offset}, ${length}) failed: errCode=${err}, errText=${errText}`);
                    return reject(new Error(`DBRead(${DBNr}) error: ${errText}`));
                }
                logger.info(`'${data.name}' DBRead(${DBNr}, ${offset}, ${length}) OK, buffer length=${res ? res.length : 'null'}`, true, true);
                vars.forEach(v => {
                    let value = null;
                    if (v.type === 'BOOL') {
                        value = datatypes['BYTE'].parser(res, v.Start - offset, -1);
                    } else {
                        value = datatypes[v.type].parser(res, v.Start - offset, v.bit);
                    }
                    v.changed = value !== v.value;
                    v.value = value;
                });
                resolve(vars);
            });
        });
    }

    /**
     * Read multiple Vars using ReadMultiVars (for S7-300/400/1200/1500)
     * If PDU size exceeded, automatically splits vars in half and retries recursively
     * @param {array} vars - items to read
     * @param {number} _depth - internal recursion depth (prevents infinite loop)
     */
    var _readVars = function (vars, _depth) {
        _depth = _depth || 0;
        return new Promise((resolve, reject) => {
            s7client.ReadMultiVars(vars, (err, res) => {
                if (err) {
                    var errText = s7client.ErrorText(err);
                    // PDU size exceeded: auto-split and retry with smaller chunks
                    if (errText.indexOf('PDU') >= 0 && vars.length > 1 && _depth < 5) {
                        var mid = Math.ceil(vars.length / 2);
                        logger.warn(`'${data.name}' ReadMultiVars PDU exceeded (${vars.length} items), splitting into ${mid}+${vars.length - mid} (depth=${_depth})`);
                        return Promise.all([
                            _readVars(vars.slice(0, mid), _depth + 1),
                            _readVars(vars.slice(mid), _depth + 1)
                        ]).then(results => {
                            var merged = [];
                            results.forEach(r => { merged = merged.concat(r); });
                            resolve(merged);
                        }).catch(reject);
                    }
                    return reject(new Error(`ReadMultiVars error: ${errText}`));
                }
                let errs = [];
                res = vars.map((v, i) => {
                    let value = null;
                    if (res[i].Result !== 0) {
                        errs.push(`${v.name} - ${s7client.ErrorText(res[i].Result)}`);
                    } else {
                        try {
                            if (v.type === 'BOOL') {
                                value = datatypes['BYTE'].parser(res[i].Data);
                            } else {
                                value = datatypes[v.type].parser(res[i].Data);
                            }
                            v.changed = value !== v.value;
                            v.value = value;
                            return v;
                        } catch { }
                    }
                });
                if (errs.length) return reject(_getErr(errs));
                resolve(vars);
            });
        });
    }

    /**
     * Read mix items by area using ReadArea (for S7-200/S7-200 Smart)
     * Groups items by area, reads each area range with ReadArea, then parses values
     * This is more reliable than ReadMultiVars for S7-200/Smart PLCs
     * @param {*} vars
     */
    var _readMixByArea = function (vars) {
        // Group items by Area
        var areaGroups = {};
        vars.forEach(v => {
            var areaKey = v.Area;
            if (!areaGroups[areaKey]) {
                areaGroups[areaKey] = [];
            }
            areaGroups[areaKey].push(v);
        });

        var readPromises = [];
        for (var areaKey in areaGroups) {
            readPromises.push(_readAreaGroup(parseInt(areaKey), areaGroups[areaKey]));
        }
        return Promise.all(readPromises).then(results => {
            // Flatten results
            var allVars = [];
            results.forEach(r => { allVars = allVars.concat(r); });
            return allVars;
        });
    }

    /**
     * Read a group of items from the same area using ReadArea
     * Splits items into sub-groups if gap between adjacent items exceeds threshold
     * @param {number} area - S7 area code (S7AreaPE, S7AreaPA, S7AreaMK)
     * @param {array} vars - items in this area
     */
    var _readAreaGroup = function (area, vars) {
        if (vars.length === 0) return Promise.resolve([]);

        // Sort by Start offset, then split by gap threshold
        var sorted = vars.slice().sort((a, b) => a.Start - b.Start);
        var subGroups = [];
        var currentGroup = [sorted[0]];

        for (var i = 1; i < sorted.length; i++) {
            var prev = sorted[i - 1];
            var curr = sorted[i];
            var prevEnd = prev.Start + (datatypes[prev.type] ? datatypes[prev.type].bytes : 1);
            var gap = curr.Start - prevEnd;

            if (gap > MIX_GAP_THRESHOLD) {
                subGroups.push(currentGroup);
                currentGroup = [curr];
            } else {
                currentGroup.push(curr);
            }
        }
        if (currentGroup.length > 0) subGroups.push(currentGroup);

        // Further split sub-groups if byte range exceeds PDU data limit
        var finalGroups = [];
        subGroups.forEach(group => {
            var gStart = group[0].Start;
            var gEnd = 0;
            group.forEach(v => {
                var byteLen = datatypes[v.type] ? datatypes[v.type].bytes : 1;
                if (v.Start + byteLen > gEnd) gEnd = v.Start + byteLen;
            });
            if (gEnd - gStart > MAX_DB_READ) {
                // Split this group further by PDU size
                var chunk = [];
                var chunkStart = group[0].Start;
                group.forEach(v => {
                    var byteLen = datatypes[v.type] ? datatypes[v.type].bytes : 1;
                    if (chunk.length > 0 && (v.Start + byteLen - chunkStart) > MAX_DB_READ) {
                        finalGroups.push(chunk);
                        chunk = [v];
                        chunkStart = v.Start;
                    } else {
                        chunk.push(v);
                    }
                });
                if (chunk.length > 0) finalGroups.push(chunk);
            } else {
                finalGroups.push(group);
            }
        });

        // Read each sub-group with a separate ReadArea call
        var promises = finalGroups.map(group => _readAreaSubGroup(area, group));
        return Promise.all(promises).then(results => {
            var allVars = [];
            results.forEach(r => { allVars = allVars.concat(r); });
            return allVars;
        });
    }

    /**
     * Read a contiguous sub-group of items from the same area using one ReadArea call
     * If PDU exceeded, auto-splits byte range in half and retries
     * @param {number} area - S7 area code
     * @param {array} vars - contiguous items (sorted by Start, gaps <= threshold)
     * @param {number} _depth - internal recursion depth
     */
    var _readAreaSubGroup = function (area, vars, _depth) {
        _depth = _depth || 0;
        return new Promise((resolve, reject) => {
            if (vars.length === 0) return resolve([]);

            // Calculate byte range to read
            let minStart = vars[0].Start;
            let maxEnd = 0;
            vars.forEach(v => {
                var byteLen = datatypes[v.type] ? datatypes[v.type].bytes : 1;
                if (v.Start + byteLen > maxEnd) maxEnd = v.Start + byteLen;
            });

            var readLen = maxEnd - minStart;
            if (readLen <= 0) return resolve([]);

            // ReadArea(area, dbNumber, start, amount, wordLen, callback)
            s7client.ReadArea(area, 0, minStart, readLen, s7client.S7WLByte, (err, buffer) => {
                if (err) {
                    var errText = s7client.ErrorText(err);
                    // PDU exceeded: split byte range in half and retry
                    if (errText.indexOf('PDU') >= 0 && readLen > 2 && _depth < 5) {
                        var halfLen = Math.ceil(readLen / 2);
                        var splitPoint = minStart + halfLen;
                        var firstHalf = vars.filter(v => v.Start < splitPoint);
                        var secondHalf = vars.filter(v => v.Start >= splitPoint);
                        logger.warn(`'${data.name}' ReadArea(${area}) PDU exceeded (${readLen} bytes), splitting (depth=${_depth})`);
                        var promises = [];
                        if (firstHalf.length > 0) promises.push(_readAreaSubGroup(area, firstHalf, _depth + 1));
                        if (secondHalf.length > 0) promises.push(_readAreaSubGroup(area, secondHalf, _depth + 1));
                        return Promise.all(promises).then(results => {
                            var merged = [];
                            results.forEach(r => { merged = merged.concat(r); });
                            resolve(merged);
                        }).catch(reject);
                    }
                    logger.error(`'${data.name}' ReadArea(${area}) error: ${errText}`, false);
                    return reject(new Error(`ReadArea error: ${errText}`));
                }
                vars.forEach(v => {
                    try {
                        let value = null;
                        var offset = v.Start - minStart;
                        if (v.type === 'BOOL') {
                            value = datatypes['BYTE'].parser(buffer, offset);
                        } else {
                            value = datatypes[v.type].parser(buffer, offset);
                        }
                        v.changed = value !== v.value;
                        v.value = value;
                    } catch (e) {
                        logger.error(`'${data.name}' ReadArea parse error for ${v.name}: ${e}`);
                    }
                });
                resolve(vars);
            });
        });
    }

    /**
     * Write a single mix item using WriteArea (for S7-200/S7-200 Smart)
     * @param {object} item - mix item with Area, Start, type, bit, value
     */
    var _writeVarByArea = function (item) {
        return new Promise((resolve, reject) => {
            var wordLen = datatypes[item.type].S7WordLen;
            var start = item.type === 'BOOL' ? item.Start * 8 + item.bit : item.Start;
            var buffer = datatypes[item.type].formatter(item.type === 'BOOL' ? Number(item.value) : parseFloat(item.value));
            // WriteArea(area, dbNumber, start, amount, wordLen, buffer, callback)
            s7client.WriteArea(item.Area, item.DBNumber || 0, start, 1, wordLen, buffer, (err) => {
                if (err) {
                    return reject(new Error(`WriteArea error: ${s7client.ErrorText(err)}`));
                }
                resolve(item);
            });
        });
    }

    /**
     * Write a DB and parse the result
     * @param {int} DBNr - The DB Number to read
     * @param {array} vars - Array of Var objects
     * @param {int} vars[].Start - Position of the first byte
     * @param {int} [vars[].bit] - Position of the bit in the byte
     * @param {Datatype} vars[].type - Data type (BYTE, WORD, INT, etc), see {@link /s7client/?api=Datatypes|Datatypes}
     * @returns {Promise} - Resolves to the vars array with populate *value* property
     */
    var _writeDB = function (DBNr, vars) {
        return new Promise((resolve, reject) => {
            if (vars.length === 0) return resolve([]);
            let end = 0;
            let offset = Number.MAX_SAFE_INTEGER;
            let v = vars[0];
            if (v.Start < offset) offset = v.Start;
            if (end < v.Start + datatypes[v.type].bytes) {
                end = v.Start + datatypes[v.type].bytes;
            }
            let buffer = datatypes[v.type].formatter(v.value)
            s7client.DBWrite(DBNr, offset, end - offset, buffer, (err, res) => {
                if (err) return reject(new Error(`DBWrite error: ${s7client.ErrorText(err)}`));
                resolve(vars);
            });
        });
    }

    /**
     * Write multiple Vars
     * @param {array} vars - Array of Var objects
     * @param {int} vars[].Start - Position of the first byte
     * @param {int} [vars[].bit] - Position of the bit in the byte
     * @param {Datatype} vars[].type - Data type (BYTE, WORD, INT, etc), see {@link /s7client/?api=Datatypes|Datatypes}
     * @param {string} vars[].area - Area (pe, pa, mk, db, ct, tm)
     * @param {string} [vars[].dbnr] - DB Nr if read from area=db
     * @param vars[].value - Value
     * @returns {Promise} - Resolves to the vars array with populate *value* property
     */
    var _writeVars = function (vars) {
        var toWrite = vars.map(v => ({
            Area: v.Area,
            WordLen: datatypes[v.type].S7WordLen,
            DBNumber: v.DBNumber || v.dbnum || 0,
            Start: v.type === 'BOOL' ? v.Start * 8 + v.bit : v.Start,
            Amount: 1,
            Data: datatypes[v.type].formatter(v.type === 'BOOL' ? Number(v.value) : parseFloat(v.value))
        }));
        return new Promise((resolve, reject) => {
            s7client.WriteMultiVars(toWrite, (err, res) => {
                if (err) return reject(new Error(`WriteMultiVars error: ${s7client.ErrorText(err)}`));
                let errs = [];

                res = vars.map((v, i) => {
                    if (res[i].Result !== 0) return errs.push(s7client.ErrorText(res[i].Result));
                    return v;
                });
                if (errs.length) return reject(_getErr(errs));
                resolve(res);
            });
        });
    }

    /**
     * Normalize tag type string to match datatypes dictionary keys
     * Frontend uses: Bool, Byte, Int, Word, DInt, DWord, Real, Char
     * KepServer import may use: Boolean, Integer, etc.
     * Datatypes keys: BOOL, BYTE, INT, WORD, DINT, DWORD, REAL, CHAR
     */
    var _normalizeType = function (typeStr) {
        if (!typeStr) return 'WORD'; // default
        var t = typeStr.toUpperCase();
        switch (t) {
            case 'BOOL':
            case 'BOOLEAN':
                return 'BOOL';
            case 'BYTE':
            case 'UINT8':
                return 'BYTE';
            case 'CHAR':
                return 'CHAR';
            case 'INT':
            case 'INT16':
            case 'INTEGER':
                return 'INT';
            case 'WORD':
            case 'UINT16':
                return 'WORD';
            case 'DINT':
            case 'INT32':
                return 'DINT';
            case 'DWORD':
            case 'UINT32':
                return 'DWORD';
            case 'REAL':
            case 'FLOAT':
            case 'FLOAT32':
                return 'REAL';
            default:
                return datatypes[t] ? t : 'WORD';
        }
    }

    /**
     * Return the Tag object (DbItem) with value
     * DB X DBX 10.3 = Bool, DB X DBB 10 = Byte/Char, DB X DBW 10 = Int/Word, DB X DBD 10 = DInt/DWord, DB X DBD 10 = Real
     */
    var _getTagItem = function (tag) {
        try {
            var variable = tag.address.toUpperCase().split(' ').join('');
            if (variable) {
                var prefix = variable.substring(0, 2);
                if (prefix === 'DB') {
                    // DB[n]"
                    var startpos = variable.indexOf('.');
                    var dbNum = parseInt(variable.substring(2, startpos));
                    if (dbNum >= 0) {
                        // DBX 0.0"
                        var dbType = variable.substring(startpos + 1, startpos + 4);
                        var dbStart = variable.substring(startpos + 4);
                        var result = new DbItem(dbNum);
                        result.type = _normalizeType(tag.type);
                        result.Area = s7client['S7AreaDB'];
                        if (dbType === 'DBB') {
                            result.Start = parseInt(dbStart);
                            if (result.Start >= 0) {
                                return result;
                            }
                        } else if (dbType === 'DBW') {
                            result.Start = parseInt(dbStart);
                            if (result.Start >= 0) {
                                return result;
                            }
                        } else if (dbType === 'DBD') {
                            result.Start = parseInt(dbStart);
                            if (result.Start >= 0) {
                                return result;
                            }
                        } else if (dbType === 'DBX') {
                            var dbBool = dbStart.split('.');
                            if (dbBool.length >= 2) {
                                result.Start = parseInt(dbBool[0]);
                                result.bit = parseInt(dbBool[1]);
                                if (result.Start >= 0 && result.bit >= 0) {
                                    return result;
                                }
                            }
                        }
                    }
                } else if (variable.charAt(0) === 'V') {
                    // S7-200/S7-200 Smart V area: VB, VW, VD, VX mapped to DB1
                    var vType = variable.substring(1, 2);
                    var vRest = variable.substring(2);
                    var vResult = new DbItem(1); // V area = DB1
                    vResult.type = _normalizeType(tag.type);
                    vResult.Area = s7client['S7AreaDB'];
                    if (vType === 'B') {
                        vResult.Start = parseInt(vRest);
                        if (vResult.Start >= 0) {
                            return vResult;
                        }
                    } else if (vType === 'W') {
                        vResult.Start = parseInt(vRest);
                        if (vResult.Start >= 0) {
                            return vResult;
                        }
                    } else if (vType === 'D') {
                        vResult.Start = parseInt(vRest);
                        if (vResult.Start >= 0) {
                            return vResult;
                        }
                    } else if (vType === 'X') {
                        // VX0.0 format
                        var vBool = vRest.split('.');
                        if (vBool.length >= 2) {
                            vResult.Start = parseInt(vBool[0]);
                            vResult.bit = parseInt(vBool[1]);
                            if (vResult.Start >= 0 && vResult.bit >= 0) {
                                return vResult;
                            }
                        }
                    } else {
                        // V0.0 format (bit address without X)
                        var vDot = variable.substring(1);
                        var vDotIdx = vDot.indexOf('.');
                        if (vDotIdx >= 0) {
                            vResult.Start = parseInt(vDot.substring(0, vDotIdx));
                            vResult.bit = parseInt(vDot.substring(vDotIdx + 1));
                            vResult.type = 'BOOL';
                            if (vResult.Start >= 0 && vResult.bit >= 0) {
                                return vResult;
                            }
                        } else {
                            // VD358 style without type prefix - infer from tag type
                            vResult.Start = parseInt(variable.substring(1));
                            if (vResult.Start >= 0) {
                                return vResult;
                            }
                        }
                    }
                } else {
                    var type = _normalizeType(tag.type);
                    var len = datatypes[type].S7WordLen;
                    switch (prefix) {
                        case 'EB':
                        case 'IB':
                        case 'EW':
                        case 'IW':
                        case 'ED':
                        case 'ID':
                            return { Area: s7client['S7AreaPE'], WordLen: len, DBNumber: 0, Start: parseInt(variable.substring(2)), Amount: 1, type: type };
                        case 'AB':
                        case 'QB':
                        case 'AW':
                        case 'QW':
                        case 'AD':
                        case 'QD':
                            return { Area: s7client['S7AreaPA'], WordLen: len, DBNumber: 0, Start: parseInt(variable.substring(2)), Amount: 1, type: type };
                        case 'MB':
                        case 'MW':
                        case 'MD':
                            return { Area: s7client['S7AreaMK'], WordLen: len, DBNumber: 0, Start: parseInt(variable.substring(2)), Amount: 1, type: type };
                        default:
                            len = datatypes['BYTE'].S7WordLen;
                            var start = parseInt(variable.substring(1, variable.indexOf('.')));
                            var bit = parseInt(variable.substring(variable.indexOf('.') + 1));
                            switch (prefix.substring(0, 1)) {
                                case 'E':
                                case 'I':
                                    return { Area: s7client['S7AreaPE'], WordLen: len, DBNumber: 0, Start: start, Amount: 1, type: type, bit: bit };
                                case 'A':
                                case 'Q':
                                    return { Area: s7client['S7AreaPA'], WordLen: len, DBNumber: 0, Start: start, Amount: 1, type: type, bit: bit };
                                case 'M':
                                    return { Area: s7client['S7AreaMK'], WordLen: len, DBNumber: 0, Start: start, Amount: 1, type: type, bit: bit };
                                case 'O':
                                case 'T':
                                case 'Z':
                                case 'C':
                                    return null;
                                default:
                                    return null;
                            }
                    }
                }
            }
        } catch (err) {

        }
        return null;
    }

    /**
     * Return error message, from error code
     * @param {*} s7err
     */
    var _getErr = function (s7err) {
        if (Array.isArray(s7err)) return new Error('Errors: ' + s7err.join('; '));
        return new Error(s7client.ErrorText(s7err));
    }
}

module.exports = {
    init: function (settings) {
        // deviceCloseTimeout = settings.deviceCloseTimeout || 15000;
    },
    create: function (data, logger, events, manager, runtime) {
        try { snap7 = require('node-snap7'); } catch { }
        if (!snap7 && manager) { try { snap7 = manager.require('node-snap7'); } catch { } }
        if (snap7) datatypes = require('./datatypes');
        else return null;
        return new S7client(data, logger, events, runtime);
    }
}

function DbItem(dbnum) {
    this.dbnum = dbnum;
    this.type = '';
    this.Area = -1;
    this.Start = -1;
    this.bit = -1;
    this.Tags = [];
    this.BitMap = {};
}

function DbItems(dbnum) {
    this.DBNumber = dbnum;
    this.MaxSize = 0;
    this.Items = {};
}
