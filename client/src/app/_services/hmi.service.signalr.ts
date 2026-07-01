import { Injectable, Output, EventEmitter } from '@angular/core';
import * as signalR from '@microsoft/signalr';

import { environment } from '../../environments/environment';
import { Tag, DeviceType } from '../_models/device';
import { Hmi, Variable, GaugeSettings, DaqQuery, DaqResult, GaugeEventSetValueType } from '../_models/hmi';
import { AlarmQuery, AlarmsFilter } from '../_models/alarm';
import { ProjectService } from '../_services/project.service';
import { EndPointApi } from '../_helpers/endpointapi';
import { Utils } from '../_helpers/utils';
import { ToastrService } from 'ngx-toastr';
import { TranslateService } from '@ngx-translate/core';
import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { AuthService, UserProfile } from './auth.service';
import { DeviceAdapterService } from '../device-adapter/device-adapter.service';
import { HttpClient } from '@angular/common/http';

@Injectable()
export class HmiService {

    @Output() onVariableChanged: EventEmitter<Variable> = new EventEmitter();
    @Output() onDeviceChanged: EventEmitter<boolean> = new EventEmitter();
    @Output() onDeviceBrowse: EventEmitter<any> = new EventEmitter();
    @Output() onDeviceNodeAttribute: EventEmitter<any> = new EventEmitter();
    @Output() onDaqResult: EventEmitter<DaqResult> = new EventEmitter();
    @Output() onDeviceProperty: EventEmitter<any> = new EventEmitter();
    @Output() onHostInterfaces: EventEmitter<any> = new EventEmitter();
    @Output() onAlarmsStatus: EventEmitter<any> = new EventEmitter();
    @Output() onDeviceWebApiRequest: EventEmitter<any> = new EventEmitter();
    @Output() onDeviceTagsRequest: EventEmitter<any> = new EventEmitter();
    @Output() onScriptConsole: EventEmitter<any> = new EventEmitter();
    @Output() onGoTo: EventEmitter<ScriptSetView> = new EventEmitter();
    @Output() onOpen: EventEmitter<ScriptOpenCard> = new EventEmitter();
    @Output() onSchedulerUpdated: EventEmitter<any> = new EventEmitter();
    @Output() onSchedulerEventActive: EventEmitter<any> = new EventEmitter();
    @Output() onSchedulerRemainingTime: EventEmitter<any> = new EventEmitter();
    @Output() onGaugeEvent: EventEmitter<any> = new EventEmitter();
    @Output() onProjectUpdated: EventEmitter<any> = new EventEmitter();

    onServerConnection$ = new BehaviorSubject<boolean>(false);

    public static separator = '^~^';
    public hmi: Hmi;
    viewSignalGaugeMap = new ViewSignalGaugeMap();
    variables = {};
    alarms = { highhigh: 0, high: 0, low: 0, info: 0 };
    // 替换 socket 为 signalR 连接
    private connection: signalR.HubConnection;
    private endPointConfig: string = EndPointApi.getURL();//"http://localhost:1881";
    private bridge: any = null;

    private addFunctionType = Utils.getEnumKey(GaugeEventSetValueType, GaugeEventSetValueType.add);
    private removeFunctionType = Utils.getEnumKey(GaugeEventSetValueType, GaugeEventSetValueType.remove);
    private homeTagsSubscription = [];
    private viewsTagsSubscription = [];

    getGaugeMapped: (gaugeName: string) => void; // function binded in GaugeManager

    constructor(public projectService: ProjectService,
            private translateService: TranslateService,
            private authService: AuthService,
            private deviceAdapaterService: DeviceAdapterService,
            private http: HttpClient,
            private toastr: ToastrService) {

        this.projectService.onLoadHmi.subscribe(() => {
            this.hmi = this.projectService.getHmi();
        });

        this.authService.currentUser$.subscribe((userProfile: UserProfile) => {
            this.initConnection(userProfile?.token);
        });
    }
    
    /**
     * Set signal value in current frontend signal array
     * Called from Test and value beckame from backend
     * @param sig
     */
    setSignalValue(sig: Variable) {
        // update the signals array value

        // notify the gui
        this.onVariableChanged.emit(sig);
    }

    /**
     * Set signal value to backend
     * Value input in frontend
     * @param sigId
     * @param value
     */
    putSignalValue(sigId: string, value: string, fnc: string = null) {
        sigId = this.deviceAdapaterService.resolveAdapterTagsId([sigId])[0];
        if (!this.variables[sigId]) {
            this.variables[sigId] = new Variable(sigId, null, null);
        }
        this.variables[sigId].value = this.getValueInFunction(this.variables[sigId].value, value, fnc);
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let device = this.projectService.getDeviceFromTagId(sigId);
            if (device) {
                this.variables[sigId]['source'] = device.id;
            }
            if (device?.type === DeviceType.internal) {
                this.variables[sigId].timestamp = new Date().getTime();
                this.setSignalValue(this.variables[sigId]);
                device.tags[sigId].value = value;
            } else {
                this.connection.invoke(IoEventTypes.DEVICE_VALUES, { cmd: 'set', var: this.variables[sigId], fnc: [fnc, value] }).catch(err => console.error('Error sending message: ', err));
            }
        } else if (this.bridge) {
            this.bridge.setDeviceValue(this.variables[sigId], { fnc: [fnc, value] });
        } else if (!environment.serverEnabled) {
            // for demo, only frontend
            this.setSignalValue(this.variables[sigId]);
        }
    }

    public getAllSignals() {
        return this.variables;
    }

    public initSignalValues(sigIds: Record<string, string>) {
        for (const [adapterId, deviceId] of Object.entries(sigIds)) {
            if (!Utils.isNullOrUndefined(this.variables[adapterId])) {
                this.variables[adapterId].value = this.variables[deviceId]?.value || null;
            }
        }
    }

    /**
     * return the value calculated with the function if defined
     * @param value
     * @param fnc
     */
    private getValueInFunction(current: any, value: string, fnc: string) {
        try {
            if (!fnc) { return value; }
            if (!current) {
                current = 0;
            }
            if (fnc === this.addFunctionType) {
                return parseFloat(current) + parseFloat(value);
            } else if (fnc === this.removeFunctionType) {
                return parseFloat(current) - parseFloat(value);
            }
        } catch (err) {
            console.error(err);
        }
        return value;
    }

    //#region Communication SignalR and Bridge
    /**
     * Init the bridge for client communication
     * @param bridge
     * @returns
     */
    initClient(bridge?: any) {
        if (!bridge) { return false; }
        this.bridge = bridge;
        if (this.bridge) {
            this.bridge.onDeviceValues = (tags: Variable[]) => this.onDeviceValues(tags);
            this.askDeviceValues();
            return true;
        }
        return false;
    }

    private onDeviceValues(tags: Variable[]) {
        for (let idx = 0; idx < tags.length; idx++) {
            let varid = tags[idx].id;
            if (!this.variables[varid]) {
                this.variables[varid] = new Variable(varid, null, null);
            }
            this.variables[varid].value = tags[idx].value;
            this.variables[varid].error = tags[idx].error;
            this.setSignalValue(this.variables[varid]);
        }
    }

        /**
         * Init the SignalR connection and subsribe to device status and signal value change
         */
        public initConnection(token: string = null) {
            // check to init SignalR connection
            if (!environment.serverEnabled) {
                return;
            }
            this.connection?.stop();
            const hubUrl = this.endPointConfig + '/DataHub';
            this.connection = new signalR.HubConnectionBuilder()
                .withUrl(hubUrl)
                .withAutomaticReconnect({
                    nextRetryDelayInMilliseconds: (retryContext) => {
                        // Retry every 5s for the first 2 minutes, then every 15s up to 5 minutes
                        if (retryContext.elapsedMilliseconds < 120000) {
                            return 5000;
                        } else if (retryContext.elapsedMilliseconds < 300000) {
                            return 15000;
                        } else {
                            return null; // Give up after 5 minutes
                        }
                    }
                })
                .withServerTimeout(60000) // Expect a message from server at least every 60s (heartbeat is 10s)
                .withKeepAliveInterval(15000) // Send ping every 15s
                .configureLogging(signalR.LogLevel.Information)
                .build();

            const startConnection = () => {
                        this.connection.start()
                            .then(() => {
                                this.onServerConnection$.next(true);
                                this.tagsSubscribe();
                                this.askDeviceValues();
                                this.askAlarmsStatus();
                                console.log('SignalR connection started');
                            })
                            .catch((err) => {
                                console.log('SignalR Connection Error', err.message);
                                // 第一次连接失败，10秒后重试
                                if (this.connection&&this.connection.state===signalR.HubConnectionState.Disconnected) {
                                    setTimeout(startConnection, 10000);
                                }
                            });
            };
            startConnection();

            // Reconnection in progress: notify UI
            this.connection.onreconnecting((error) => {
                console.warn('SignalR reconnecting...', error);
                this.onServerConnection$.next(false);
            });

            // Reconnection succeeded: restore state
            this.connection.onreconnected((connectionId) => {
                console.log('SignalR reconnected, connectionId:', connectionId);
                this.onServerConnection$.next(true);
                this.tagsSubscribe();
                this.askDeviceValues();
                this.askAlarmsStatus();
            });

            // Connection permanently closed (auto-reconnect gave up or manual stop)
            this.connection.onclose((error) => {
                this.onServerConnection$.next(false);
                console.warn('SignalR connection closed', error);
                // Try to reload if server is reachable (same as Socket.IO behavior)
                if (error) {
                    this.safeReloadIfServerAlive();
                }
            });
            // 设备状态
            this.connection.on(IoEventTypes.DEVICE_STATUS, (message) => {
                this.onDeviceChanged.emit(message);
                if (message.status === 'connect-error' && this.hmi?.layout?.show_connection_error) {
                    let name = message.id;
                    let device = this.projectService.getDeviceFromId(message.id);
                    if (device) { name = device.name; }
                    let msg = '';
                    this.translateService.get('msg.device-connection-error', { value: name }).subscribe((txt: string) => { msg = txt; });
                    this.toastr.error(msg, '', {
                        timeOut: 3000,
                        closeButton: true,
                        // disableTimeOut: true
                    });
                }
            });
            // 设备属性
            this.connection.on(IoEventTypes.DEVICE_PROPERTY, (message) => {
                this.onDeviceProperty.emit(message);
            });
            // 设备值
            this.connection.on(IoEventTypes.DEVICE_VALUES, (message) => {
                const updateVariable = (id: string, value: any, timestamp: any, quality: any) => {
                    if (Utils.isNullOrUndefined(this.variables[id])) {
                        this.variables[id] = new Variable(id, null, null);
                    }
                    this.variables[id].value = value;
                    this.variables[id].timestamp = timestamp;
                    this.variables[id].quality = quality;
                    this.setSignalValue(this.variables[id]);
                };

                for (let idx = 0; idx < message.values.length; idx++) {
                    const originalId = message.values[idx].id;
                    const value = message.values[idx].value;
                    const timestamp = message.values[idx].timestamp;
                    const quality = message.values[idx].quality;
                    updateVariable(originalId, value, timestamp, quality);
                    const adapterIds = this.deviceAdapaterService.resolveDeviceTagIdForAdapter(originalId);
                    if (adapterIds?.length) {
                        adapterIds.forEach(adapterId => {
                            updateVariable(adapterId, value, timestamp, quality);
                        });
                    }
                }
            });
            // 设备浏览
            this.connection.on(IoEventTypes.DEVICE_BROWSE, (message) => {
                this.onDeviceBrowse.emit(message);
            });
            // 设备节点属性
            this.connection.on(IoEventTypes.DEVICE_NODE_ATTRIBUTE, (message) => {
                this.onDeviceNodeAttribute.emit(message);
            });
            // scheduler updated (one-time events removed, etc.)
            this.connection.on(IoEventTypes.SCHEDULER_UPDATED, (message) => {
                this.onSchedulerUpdated.emit(message);
            });
            // scheduler event active state changed (START/STOP fired)
            this.connection.on(IoEventTypes.SCHEDULER_ACTIVE, (message) => {
                this.onSchedulerEventActive.emit(message);
            });
            // scheduler remaining time update
            this.connection.on(IoEventTypes.SCHEDULER_REMAINING, (message) => {
                this.onSchedulerRemainingTime.emit(message);
            });
            // DAQ 结果
            this.connection.on(IoEventTypes.DAQ_RESULT, (message) => {
                this.onDaqResult.emit(message);
            });
            // 警报状态
            this.connection.on(IoEventTypes.ALARMS_STATUS, (alarmsstatus) => {
                this.onAlarmsStatus.emit(alarmsstatus);
            });
            this.connection.on(IoEventTypes.HOST_INTERFACES, (message) => {
                this.onHostInterfaces.emit(message);
            });
            this.connection.on(IoEventTypes.DEVICE_WEBAPI_REQUEST, (message) => {
                this.onDeviceWebApiRequest.emit(message);
            });
            this.connection.on(IoEventTypes.DEVICE_TAGS_REQUEST, (message) => {
                this.onDeviceTagsRequest.emit(message);
            });
            // 脚本
            this.connection.on(IoEventTypes.SCRIPT_CONSOLE, (message) => {
                this.onScriptConsole.emit(message);
            });
            this.connection.on(IoEventTypes.SCRIPT_COMMAND, (message) => {
                this.onScriptCommand(message);
            });
            this.connection.on(IoEventTypes.ALIVE, (message) => {
                this.onServerConnection$.next(true);
            });
            // 项目更新通知
            this.connection.on(IoEventTypes.PROJECT_UPDATED, (message) => {
                console.log('[HmiService] Received PROJECT_UPDATED via SignalR:', message);
                this.onProjectUpdated.emit(message);
            });
        }

    /**
     * Ask device status to backend
     */
    public askDeviceStatus() {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            this.connection.invoke(IoEventTypes.DEVICE_STATUS, 'get').catch(err => console.error('Error asking device status: ', err));
        }
    }

    /**
     * Ask device status to backend
     */
    public askDeviceProperty(endpoint: EndPointSettings & any, type) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { endpoint: endpoint, type: type };
            this.connection.invoke(IoEventTypes.DEVICE_PROPERTY, msg).catch(err => console.error('Error asking device property: ', err));
        }
    }

    /**
     * Ask device webapi result to test
     */
    public askWebApiProperty(property) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { property: property };
            this.connection.invoke(IoEventTypes.DEVICE_WEBAPI_REQUEST, msg).catch(err => console.error('Error asking web api property: ', err));
        }
    }

    /**
     * Ask device tags settings
     */
    public askDeviceTags(deviceId: string) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { deviceId: deviceId };
            this.connection.invoke(IoEventTypes.DEVICE_TAGS_REQUEST, msg).catch(err => console.error('Error asking device tags: ', err));
        }
    }

    /**
     * Ask host interface available
     */
    public askHostInterface() {
        if (this.connection) {
            this.connection.invoke(IoEventTypes.HOST_INTERFACES, 'get').catch(err => console.error('Error asking host interface: ', err));
        }
    }

    /**
     * Ask device status to backend
     */
    public askDeviceValues() {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            this.connection.invoke(IoEventTypes.DEVICE_VALUES, { cmd: 'get' }).catch(err => console.error('Error asking device values: ', err));
        } else if (this.bridge) {
            this.bridge.getDeviceValues(null);
        }
    }

    /**
     * Ask alarms status to backend
     */
    public askAlarmsStatus() {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            this.connection.invoke(IoEventTypes.ALARMS_STATUS, 'get').catch(err => console.error('Error asking alarms status: ', err));
        }
    }

    public emitMappedSignalsGauge(domViewId: string) {
        let sigsToEmit = this.viewSignalGaugeMap.getSignalIds(domViewId);
        for (let idx = 0; idx < sigsToEmit.length; idx++) {
            if (this.variables[sigsToEmit[idx]]) {
                this.setSignalValue(this.variables[sigsToEmit[idx]]);
            }
        }
    }

    /**
     * Ask device browse to backend
     */
    public askDeviceBrowse(deviceId: string, node: any) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { device: deviceId, node: node };
            this.connection.invoke(IoEventTypes.DEVICE_BROWSE, msg).catch(err => console.error('Error asking device browse: ', err));
        }
    }

    /**
     * Ask device node attribute to backend
     */
    public askNodeAttributes(deviceId: string, node: any) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { device: deviceId, node: node };
            this.connection.invoke(IoEventTypes.DEVICE_NODE_ATTRIBUTE, msg).catch(err => console.error('Error asking node attributes: ', err));
        }
    }

    public queryDaqValues(msg: DaqQuery) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
             msg.sids = this.deviceAdapaterService.resolveAdapterTagsId(msg.sids);
            this.connection.invoke(IoEventTypes.DAQ_QUERY, msg).catch(err => console.error('Error querying daq values: ', err));
        }
    }

    private tagsSubscribe(sendLastValue: boolean = false) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
             const mergedArray = this.viewsTagsSubscription.concat(this.homeTagsSubscription);
            const mergedArrayResolvedAdapter = this.deviceAdapaterService.resolveAdapterTagsId(mergedArray);
            let msg = { tagsId: [...new Set(mergedArrayResolvedAdapter)], sendLastValue: sendLastValue };
            this.connection.invoke(IoEventTypes.DEVICE_TAGS_SUBSCRIBE, msg).catch(err => console.error('Error subscribing tags: ', err));
        }
    }

    /**
     * Subscribe views tags values
     */
    public viewsTagsSubscribe(tagsId: string[], sendLastValue: boolean = false) {
        this.viewsTagsSubscription = tagsId;
        this.tagsSubscribe(sendLastValue);
    }

    /**
     * Subscribe only home tags value
     */
    public homeTagsSubscribe(tagsId: string[]) {
        this.homeTagsSubscription = tagsId;
        this.tagsSubscribe();
    }

    /**
     * Unsubscribe to tags values
     */
    public tagsUnsubscribe(tagsId: string[]) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { tagsId: tagsId };
            this.connection.invoke(IoEventTypes.DEVICE_TAGS_UNSUBSCRIBE, msg).catch(err => console.error('Error unsubscribing tags: ', err));
        }
    }

    /**
     * Enable device
     * @param deviceName
     * @param enable
     */
    public deviceEnable(deviceName: string, enable: boolean) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = {
                deviceName: deviceName,
                enable: enable
            };
            this.connection.invoke(IoEventTypes.DEVICE_ENABLE, msg).catch(err => console.error('Error enabling device: ', err));
        }
    }

    /**
     * Restart device data collection
     */
    public restartDevice(deviceId: string) {
        if (this.connection&&this.connection.state===signalR.HubConnectionState.Connected) {
            let msg = { device: deviceId };
            this.connection.invoke(IoEventTypes.DEVICE_RESTART, msg).catch(err => console.error('Error restarting device: ', err));
        }
    }
    //#endregion

    //#region Signals Gauges Mapping
    addSignal(signalId: string) {
        // add to variable list
        if (!this.variables[signalId]) {
            this.variables[signalId] = new Variable(signalId, null, this.projectService.getDeviceFromTagId(signalId));
        }
    }

    /**
     * map the dom view with signal and gauge settings
     * @param domViewId
     * @param signalId
     * @param ga
     */
    addSignalGaugeToMap(domViewId: string, signalId: string, ga: GaugeSettings) {
        this.viewSignalGaugeMap.add(domViewId, signalId, ga);
        // add to variable list
        if (!this.variables[signalId]) {
            this.variables[signalId] = new Variable(signalId, null, this.projectService.getDeviceFromTagId(signalId));
        }
    }

    /**
     * remove mapped dom view Gauges
     * @param domViewId
     * return the removed gauge settings id list with signal id binded
     */
    removeSignalGaugeFromMap(domViewId: string) {
        let sigsIdremoved = this.viewSignalGaugeMap.getSignalIds(domViewId);
        let result = {};
        sigsIdremoved.forEach(sigid => {
            let gaugesSettings: GaugeSettings[] = this.viewSignalGaugeMap.signalsGauges(domViewId, sigid);
            if (gaugesSettings) {
                result[sigid] = gaugesSettings.map(gs => gs.id);
            }
        });
        this.viewSignalGaugeMap.remove(domViewId);
        return result;
    }

    /**
     * get the gauges settings list of mapped dom view with the signal
     * @param domViewId
     * @param sigid
     */
    getMappedSignalsGauges(domViewId: string, sigid: string): GaugeSettings[] {
        return Object.values(this.viewSignalGaugeMap.signalsGauges(domViewId, sigid));
    }

    /**
     * get all signals property mapped in all dom views
     * @param fulltext a copy with item name and source
     */
    getMappedVariables(fulltext: boolean): Variable[] {
        let result: Variable[] = [];
        this.viewSignalGaugeMap.getAllSignalIds().forEach(sigid => {
            if (this.variables[sigid]) {
                let toadd = this.variables[sigid];
                if (fulltext) {
                    toadd = Object.assign({}, this.variables[sigid]);
                    let device = this.projectService.getDeviceFromTagId(toadd.id);
                    if (device) {
                        toadd['source'] = device.name;
                        if (device.tags[toadd.id]) {
                            toadd['name'] = this.getTagLabel(device.tags[toadd.id]);
                        }
                    }
                }
                result.push(toadd);
            }
        });
        return result;
    }

    /**
     * get singal property, complate the signal property with device tag property
     * @param sigid
     * @param fulltext
     */
    getMappedVariable(sigid: string, fulltext: boolean): Variable {
        if (!this.variables[sigid]) { return null; }

        if (this.variables[sigid]) {
            let result = this.variables[sigid];
            if (fulltext) {
                result = Object.assign({}, this.variables[sigid]);
                let device = this.projectService.getDeviceFromTagId(result.id);
                if (device) {
                    result['source'] = device.name;
                    if (device.tags[result.id]) {
                        result['name'] = this.getTagLabel(device.tags[result.id]);
                    }
                }
            }
            return result;
        }
    }

    private getTagLabel(tag: Tag) {
        if (tag.label) {
            return tag.label;
        } else {
            return tag.name;
        }
    }

    //#endregion

    //#region Chart and Graph functions
    getChart(id: string) {
        return this.projectService.getChart(id);
    }

    getChartSignal(id: string) {
        let chart = this.projectService.getChart(id);
        if (chart) {
            let varsId = [];
            chart.lines.forEach(line => {
                varsId.push(line.id);
            });
            return varsId;
        }
    }

    getGraph(id: string) {
        return this.projectService.getGraph(id);
    }

    getGraphSignal(id: string) {
        let graph = this.projectService.getGraph(id);
        if (graph) {
            let varsId = [];
            graph.sources.forEach(source => {
                varsId.push(source.id);
            });
            return varsId;
        }
    }
    //#endregion

    //#region Current Alarms functions
    getAlarmsValues(alarmFilter?: AlarmsFilter) {
        return this.projectService.getAlarmsValues(alarmFilter);
    }

    getAlarmsHistory(query: AlarmQuery) {
        return this.projectService.getAlarmsHistory(query);
    }

    setAlarmAck(alarmName: string) {
        return this.projectService.setAlarmAck(alarmName);
    }
    //#endregion

    //#region DAQ functions served from project service
    getDaqValues(query: DaqQuery) {
        return this.projectService.getDaqValues(query);
    }
    //#endregion

    //#region Scheduler functions served from project service
    askSchedulerData(id: string) {
        return this.projectService.getSchedulerData(id);
    }

    setSchedulerData(id: string, data: any) {
        return this.projectService.setSchedulerData(id, data);
    }

    deleteSchedulerData(id: string) {
        return this.projectService.deleteSchedulerData(id);
    }
    //#endregion

    //#region My Static functions
    public static toVariableId(src: string, name: string) {
        return src + HmiService.separator + name;
    }

    //#endregion

    public onScriptCommand(message: ScriptCommandMessage) {
        if (message.params && message.params.length) {
            switch (message.command) {
                case ScriptCommandEnum.SETVIEW:
                    this.onGoTo.emit(<ScriptSetView>{ viewName: message.params[0], force: message.params[1] });
                    break;
                case ScriptCommandEnum.OPENCARD:
                    this.onOpen.emit(<ScriptOpenCard>{ viewName: message.params[0], options: message.params[1] });
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Ask backend to notify all clients about project update
     */
    public notifyProjectUpdate() {
        if (this.connection && this.connection.state === signalR.HubConnectionState.Connected) {
            console.log('[HmiService] Invoking PROJECT_NOTIFY_UPDATE via SignalR');
            this.connection.invoke(IoEventTypes.PROJECT_NOTIFY_UPDATE).catch(err => console.error('Error notifying project update: ', err));
        } else {
            console.warn('[HmiService] Cannot notify project update: SignalR is not connected', this.connection?.state);
        }
    }

    private async safeReloadIfServerAlive() {
            try {
                // Small test request to verify if server is reachable
                const res = await firstValueFrom(
                    this.http.get<string>(this.endPointConfig + '/api/version')
                );
                if (res) {
                    console.warn('Server reachable → forcing reload');
                    window.location.reload();
                } else {
                    console.warn('Server reachable but returned error, skipping reload');
                }
            } catch {
                console.warn('Server NOT reachable → do NOT reload');
            }
        }
}

class ViewSignalGaugeMap {
    views = {};

    public add(domViewId: string, signalId: string, ga: GaugeSettings) {
        if (!this.views[domViewId]) {
            this.views[domViewId] = {};
        }
        if (!this.views[domViewId][signalId]) {
            this.views[domViewId][signalId] = [];
        }
        this.views[domViewId][signalId].push(ga);
        return true;
    }

    public remove(domViewId: string) {
        delete this.views[domViewId];
        return;
    }

    public signalsGauges(domViewId: string, sigid: string) {
        return this.views[domViewId][sigid];
    }

    public getSignalIds(domViewId: string) {
        let result: string[] = [];
        if (this.views[domViewId]) {
            result = Object.keys(this.views[domViewId]);
        }
        return result;
    }

    public getAllSignalIds() {
        let result: string[] = [];
        Object.values(this.views).forEach(evi => {
            Object.keys(evi).forEach(key => {
                if (result.indexOf(key) === -1) {
                    result.push(key);
                }
            });
        });
        return result;
    }
}

export enum IoEventTypes {
    DEVICE_STATUS = 'device-status',
    DEVICE_PROPERTY = 'device-property',
    DEVICE_VALUES = 'device-values',
    DEVICE_BROWSE = 'device-browse',
    DEVICE_NODE_ATTRIBUTE = 'device-node-attribute',
    DEVICE_WEBAPI_REQUEST = 'device-webapi-request',
    DEVICE_TAGS_REQUEST = 'device-tags-request',
    DEVICE_TAGS_SUBSCRIBE = 'device-tags-subscribe',
    DEVICE_TAGS_UNSUBSCRIBE = 'device-tags-unsubscribe',
    DEVICE_ENABLE = 'device-enable',
    DEVICE_RESTART = 'device-restart',
    DAQ_QUERY = 'daq-query',
    DAQ_RESULT = 'daq-result',
    DAQ_ERROR = 'daq-error',
    ALARMS_STATUS = 'alarms-status',
    HOST_INTERFACES = 'host-interfaces',
    SCRIPT_CONSOLE = 'script-console',
    SCRIPT_COMMAND = 'script-command',
    ALIVE = 'heartbeat',
    SCHEDULER_UPDATED = 'scheduler:updated',
    SCHEDULER_ACTIVE = 'scheduler:event-active',
    SCHEDULER_REMAINING = 'scheduler:remaining-time',
    PROJECT_UPDATED = 'project-updated',
    PROJECT_NOTIFY_UPDATE = 'project-notify-update'
}

export const ScriptCommandEnum = {
    SETVIEW: 'SETVIEW',
    OPENCARD: 'OPENCARD',
};

export interface ScriptCommandMessage {
    command: string;
    params: any[];
}

export interface ScriptSetView {
    viewName: string;
    force: boolean;
}

export interface ScriptOpenCard {
    viewName: string;
    options: {};
}

export interface EndPointSettings {
    address: string;
    uid: string;
    pwd: string;
    id?: string;
}
