/* eslint-disable @angular-eslint/component-class-suffix */
import {Component, OnDestroy, OnInit, ViewChild} from '@angular/core';
import {Subscription} from 'rxjs';
import {Router} from '@angular/router';

import {DeviceTreeComponent} from './device-tree/device-tree.component';
import {Device, DEVICE_PREFIX, DevicesUtils, DeviceType, TAG_PREFIX} from './../_models/device';
import {ProjectService} from '../_services/project.service';
import {HmiService} from '../_services/hmi.service';
import {DEVICE_READONLY} from '../_models/hmi';
import {Utils} from '../_helpers/utils';

@Component({
    selector: 'app-device',
    templateUrl: './device.component.html',
    styleUrls: ['./device.component.scss']
})
export class DeviceComponent implements OnInit, OnDestroy {

    @ViewChild('devicetree', {static: false}) deviceTree: DeviceTreeComponent;

    private subscriptionLoad: Subscription;
    private askStatusTimer;

    readonly = false;
    reloadActive = false;

    constructor(private router: Router,
        private projectService: ProjectService,
        private hmiService: HmiService) {
        if (this.router.url.indexOf(DEVICE_READONLY) >= 0) {
            this.readonly = true;
        }
    }

    ngOnInit() {
        this.subscriptionLoad = this.projectService.onLoadHmi.subscribe(res => {
            if (this.deviceTree) {
                this.deviceTree.buildTree();
            }
        });
        this.askStatusTimer = setInterval(() => {
            this.hmiService.askDeviceStatus();
        }, 10000);
        this.hmiService.askDeviceStatus();
    }

    ngOnDestroy() {
        try {
            if (this.subscriptionLoad) {
                this.subscriptionLoad.unsubscribe();
            }
        } catch (e) {
        }
        try {
            clearInterval(this.askStatusTimer);
            this.askStatusTimer = null;
        } catch { }
    }

    onReload() {
        this.projectService.onRefreshProject();
        this.reloadActive = true;
        setTimeout(() => {
            this.reloadActive = false;
        }, 1000);
    }

    onOpenColumnSettings() {
        if (this.deviceTree) {
            this.deviceTree.openColumnSettings();
        }
    }

    onExport(type: string) {
        try {
            if (type === 'xls') {
                this.projectService.exportDevicesXls();
            } else {
                this.projectService.exportDevices(type);
            }
        } catch (err) {
            console.error(err);
        }
    }

    onImport() {
        let ele = document.getElementById('devicesConfigFileUpload') as HTMLElement;
        ele.click();
    }

    onImportTpl() {
        let ele = document.getElementById('devicesConfigTplUpload') as HTMLElement;
        ele.click();
    }

    onImportKepserver() {
        let ele = document.getElementById('kepserverFileUpload') as HTMLElement;
        ele.click();
    }

    onKepserverFileChange(event) {
        let input = event.target as HTMLInputElement;
        const file = input.files[0];
        if (file) {
            this.projectService.importKepserver(file);
        }
        if (input) {
            input.value = '';
        }
    }

    /**
     * open Project event file loaded
     * @param event file resource
     * @param isTemplate use template for import, if true, generate new device id and tag id
     */
    onDevTplChangeListener(event, isTemplate: boolean){
        let input = event.target as HTMLInputElement;
        const file = input.files[0];

        // Check if file is xlsx/xls - use backend API
        const fileName = file.name.toLowerCase();
        if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
            this.projectService.importDevicesXls(file, isTemplate);
            if (input) {
                input.value = '';
            }
            return;
        }

        const processText = (text: string) => {
            let devices: Device[];
            let deviceFolders: any = null;
            if (Utils.isJson(text)) {
                // JSON - could be old format (array) or new format (object with devices+deviceFolders)
                const parsed = JSON.parse(text);
                if (Array.isArray(parsed)) {
                    devices = parsed;
                } else if (parsed && parsed.devices) {
                    devices = parsed.devices;
                    deviceFolders = parsed.deviceFolders || null;
                } else {
                    devices = [];
                }
            } else {
                // CSV
                devices = DevicesUtils.csvToDevices(text, this.projectService.getScripts());
            }
            //generate new id and filte fuxa
            let importDev = [];
            if(isTemplate) {
                devices.forEach((device: Device) => {
                    if (device.type != DeviceType.FuxaServer) {
                        device.id = Utils.getGUID(DEVICE_PREFIX);
                        device.name = Utils.getShortGUID(device.name + '_', '');
                        if (device.tags) {
                            let newTags = {};
                            Object.keys(device.tags).forEach((key) => {
                                const id = Utils.getGUID(TAG_PREFIX);
                                //change tags key to new id
                                newTags[id] = device.tags[key];
                                newTags[id].id = id;
                            });
                            device.tags = newTags;
                        }
                        importDev.push(device);
                    }
                });
            }
            this.projectService.importDevices(isTemplate ? importDev : devices);
            if (deviceFolders && !isTemplate) {
                this.projectService.importDeviceFolders(deviceFolders);
            }
            setTimeout(() => { this.projectService.onRefreshProject(); }, 2000);
        };

        const onError = () => {
            let msg = 'Unable to read ' + file;
            // this.translateService.get('msg.project-load-error', {value: file}).subscribe((txt: string) => { msg = txt });
            alert(msg);
        };

        // First try UTF-8; if replacement characters are detected, retry with GBK
        let reader = new FileReader();
        reader.onload = () => {
            const text = reader.result as string;
            if (text.indexOf('\uFFFD') !== -1) {
                // Detected garbled characters, retry with GBK encoding
                let gbkReader = new FileReader();
                gbkReader.onload = () => { processText(gbkReader.result as string); };
                gbkReader.onerror = onError;
                gbkReader.readAsText(file, 'GBK');
            } else {
                processText(text);
            }
        };
        reader.onerror = onError;
        reader.readAsText(file, 'UTF-8');

        if (input) {
            input.value = '';
        }
    }
}
