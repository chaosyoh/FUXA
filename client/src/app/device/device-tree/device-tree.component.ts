import { Component, OnInit, OnDestroy, Input, Output, EventEmitter, ChangeDetectorRef, ViewChild, AfterViewInit, ElementRef, HostListener } from '@angular/core';
import { FlatTreeControl } from '@angular/cdk/tree';
import { MatTreeFlatDataSource, MatTreeFlattener } from '@angular/material/tree';
import { MatDialog } from '@angular/material/dialog';
import { MatTableDataSource } from '@angular/material/table';
import { MatSort } from '@angular/material/sort';
import { MatPaginator } from '@angular/material/paginator';
import { MatSnackBar } from '@angular/material/snack-bar';
import { TranslateService } from '@ngx-translate/core';
import { Subscription } from 'rxjs';

import { Device, Tag, TagGroup, DeviceFolder, TAG_GROUP_PREFIX, TAG_PREFIX, DEVICE_PREFIX, DEVICE_FOLDER_PREFIX, DeviceType, DeviceNetProperty } from '../../_models/device';
import { ProjectService } from '../../_services/project.service';
import { HmiService } from '../../_services/hmi.service';
import { AppService } from '../../_services/app.service';
import { PluginService } from '../../_services/plugin.service';
import { Utils } from '../../_helpers/utils';
import { EditNameComponent } from '../../gui-helpers/edit-name/edit-name.component';
import { ConfirmDialogComponent } from '../../gui-helpers/confirm-dialog/confirm-dialog.component';
import { TagPropertyService } from '../tag-property/tag-property.service';
import { TagOptionType, TagOptionsComponent } from '../tag-options/tag-options.component';
import { DevicePropertyComponent } from '../device-property/device-property.component';
import { TagWriteValueComponent, TagWriteValueData } from '../tag-write-value/tag-write-value.component';
import { DeviceTreeColumnSettingsComponent, ColumnSetting } from './device-tree-column-settings/device-tree-column-settings.component';

// Tree node structure
interface TreeNode {
    id: string;
    name: string;
    type: 'device' | 'group' | 'folder';
    deviceId: string;
    children?: TreeNode[];
}

// Flat node with level info
interface FlatNode {
    id: string;
    name: string;
    type: 'device' | 'group' | 'folder';
    deviceId: string;
    level: number;
    expandable: boolean;
}

@Component({
    selector: 'app-device-tree',
    templateUrl: './device-tree.component.html',
    styleUrls: ['./device-tree.component.scss']
})
export class DeviceTreeComponent implements OnInit, AfterViewInit, OnDestroy {
    @Input() readonly = false;
    @Output() goto = new EventEmitter();

    // System columns are always visible and frozen (not shown in settings dialog)
    private static readonly LEFT_FROZEN_COLUMNS = ['index', 'select'];
    private static readonly RIGHT_FROZEN_COLUMNS = ['writeValue', 'remove'];

    readonly defAllColumns = ['index', 'select', 'name', 'address', 'device', 'type', 'access', 'value', 'format', 'daq', 'timestamp', 'description', 'writeValue', 'remove'];
    readonly defInternalColumns = ['index', 'select', 'name', 'device', 'type', 'access', 'value', 'format', 'timestamp', 'description', 'writeValue', 'remove'];
    readonly defGpioColumns = ['index', 'select', 'name', 'device', 'address', 'direction', 'access', 'value', 'format', 'timestamp', 'description', 'writeValue', 'remove'];
    readonly defWebcamColumns = ['index', 'select', 'name', 'device', 'address', 'access', 'value', 'format', 'timestamp', 'description', 'writeValue', 'remove'];
    readonly defAllExtColumns = ['index', 'select', 'name', 'address', 'device', 'type', 'access', 'value', 'format', 'daq', 'timestamp', 'quality', 'description', 'writeValue', 'remove'];

    displayedColumns = this.defAllColumns;
    dataSource = new MatTableDataSource([]);
    tagsMap = {};
    isDeviceToEdit = true;
    isWithOptions = true;

    @ViewChild(MatSort, {static: false}) sort: MatSort;
    @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;
    pageSizeOptions = [10, 25, 100];
    currentPageSize = 25;
    pageJumpInput: number = null;

    // Device status tracking
    devicesStatus: { [id: string]: { status: string; last: number } } = {};
    private subscriptionDeviceChange: Subscription;
    private subscriptionVariableChange: Subscription;

    // Throttled value update
    private _updatePending = false;
    private _updateTimer: any = null;
    private readonly _updateThrottleMs = 1000;

    // Tree search
    treeFilterText = '';

    // Type column filter
    selectedTypes = new Set<string>();
    typeFilterOptions: string[] = [];
    private textFilterValue = '';

    // Column resize
    private resizeColumn: string | null = null;
    private resizeStartX = 0;
    private resizeStartWidth = 0;
    private resizeWidths: { [key: string]: number } = {};
    private onMouseMoveHandler: ((e: MouseEvent) => void) | null = null;
    private onMouseUpHandler: (() => void) | null = null;

    // Device plugins (available device types)
    plugins = [];

    // Column configuration persistence
    private static readonly STORAGE_KEY = 'fuxa-device-tree-column-config';
    private columnSettings: ColumnSetting[] = [];
    private columnStyleElement: HTMLStyleElement | null = null;

    selectedNode: FlatNode = null;
    selectedDevice: Device = null;

    // Tree control
    private _transformer = (node: TreeNode, level: number): FlatNode => ({
        id: node.id,
        name: node.name,
        type: node.type,
        deviceId: node.deviceId,
        level: level,
        expandable: !!node.children && node.children.length > 0
    });

    treeControl = new FlatTreeControl<FlatNode>(
        node => node.level,
        node => node.expandable
    );

    treeFlattener = new MatTreeFlattener(
        this._transformer,
        node => node.level,
        node => node.expandable,
        node => node.children
    );

    treeDataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener);

    constructor(
        private projectService: ProjectService,
        private hmiService: HmiService,
        private appService: AppService,
        private pluginService: PluginService,
        private translateService: TranslateService,
        private changeDetector: ChangeDetectorRef,
        private dialog: MatDialog,
        private tagPropertyService: TagPropertyService,
        private snackBar: MatSnackBar,
        private elementRef: ElementRef
    ) {}

    ngOnInit() {
        this.loadAvailableType();
        this.buildTree();
        // Subscribe to device status changes
        this.subscriptionDeviceChange = this.hmiService.onDeviceChanged.subscribe(event => {
            this.setDeviceStatus(event);
        });
        // Subscribe to variable value changes
        this.subscriptionVariableChange = this.hmiService.onVariableChanged.subscribe(event => {
            this.updateTagValues();
        });
    }

    ngAfterViewInit() {
        this.dataSource.sort = this.sort;
        this.reassignPaginator();
    }

    ngOnDestroy() {
        if (this.subscriptionDeviceChange) {
            this.subscriptionDeviceChange.unsubscribe();
        }
        if (this.subscriptionVariableChange) {
            this.subscriptionVariableChange.unsubscribe();
        }
        if (this._updateTimer) {
            clearTimeout(this._updateTimer);
            this._updateTimer = null;
        }
        this.removeResizeListeners();
    }

    // ═══════════════════════════════════════════════
    // Column Resize
    // ═══════════════════════════════════════════════

    /** Detect mousedown near header cell right edge to start column resize */
    @HostListener('mousedown', ['$event'])
    onHostMouseDown(event: MouseEvent) {
        const target = event.target as HTMLElement;
        const headerCell = target.closest('mat-header-cell, .mat-mdc-header-cell, .mat-header-cell') as HTMLElement;
        if (!headerCell) return;

        // Skip system/frozen columns (index, select, writeValue, remove)
        const systemCols = [...DeviceTreeComponent.LEFT_FROZEN_COLUMNS, ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS];
        const classList = Array.from(headerCell.classList);
        const columnKey = classList.find(c => c.startsWith('mat-column-'))?.replace('mat-column-', '');
        if (!columnKey || systemCols.includes(columnKey)) return;

        const rect = headerCell.getBoundingClientRect();
        if (rect.right - event.clientX > 6) return;

        event.preventDefault();
        this.resizeColumn = columnKey;
        this.resizeStartX = event.clientX;
        this.resizeStartWidth = rect.width;

        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'col-resize';

        this.onMouseMoveHandler = (e: MouseEvent) => this.onResizeMove(e);
        this.onMouseUpHandler = () => this.onResizeEnd();
        document.addEventListener('mousemove', this.onMouseMoveHandler);
        document.addEventListener('mouseup', this.onMouseUpHandler);
    }

    private onResizeMove(event: MouseEvent) {
        if (!this.resizeColumn) return;
        const diff = event.clientX - this.resizeStartX;
        const newWidth = Math.max(50, Math.round(this.resizeStartWidth + diff));
        this.resizeWidths[this.resizeColumn] = newWidth;
        this.applyColumnStyles();
        this.changeDetector.detectChanges();
    }

    private onResizeEnd() {
        this.resizeColumn = null;
        document.body.style.userSelect = '';
        document.body.style.cursor = '';
        this.removeResizeListeners();
    }

    private removeResizeListeners() {
        if (this.onMouseMoveHandler) {
            document.removeEventListener('mousemove', this.onMouseMoveHandler);
            this.onMouseMoveHandler = null;
        }
        if (this.onMouseUpHandler) {
            document.removeEventListener('mouseup', this.onMouseUpHandler);
            this.onMouseUpHandler = null;
        }
    }

    /** Throttled update of tag values from HMI signals */
    private updateTagValues() {
        if (this._updatePending) { return; }
        this._updatePending = true;
        if (this._updateTimer) { clearTimeout(this._updateTimer); }
        this._updateTimer = setTimeout(() => {
            this._updatePending = false;
            const sigs = this.hmiService.getAllSignals();
            for (const id in sigs) {
                if (this.tagsMap[id]) {
                    const signal = sigs[id];
                    this.tagsMap[id].value = signal.value;
                    this.tagsMap[id].error = signal.error;
                    this.tagsMap[id].timestamp = signal.timestamp;
                    this.tagsMap[id].quality = signal.quality;
                }
            }
            this.changeDetector.detectChanges();
        }, this._updateThrottleMs);
    }

    /** Reassign paginator after view changes */
    private reassignPaginator() {
        setTimeout(() => {
            if (this.paginator) {
                this.dataSource.paginator = this.paginator;
            }
        });
    }

    hasChild = (_: number, node: FlatNode) => node.expandable;

    /** Refresh tree data from project */
    buildTree() {
        const devices = this.projectService.getDevices();
        if (!devices) {
            this.treeDataSource.data = [];
            return;
        }

        const folders = this.projectService.getDeviceFolders() as { [id: string]: DeviceFolder };
        const allDevices = Object.values(devices) as Device[];
        const treeNodes: TreeNode[] = [];

        // Build top-level device folders (parentId === '')
        for (const folder of Object.values(folders)) {
            if ((folder.parentId || '') === '') {
                const folderNode = this.buildDeviceFolderNode(folder, folders, allDevices);
                if (folderNode) {
                    treeNodes.push(folderNode);
                }
            }
        }

        // Build top-level devices (no folderId)
        for (const device of allDevices) {
            if (!device || device.folderId) continue;

            let children: TreeNode[];
            if (this.treeFilterText) {
                children = this.buildFilteredGroupChildren(device, '');
            } else {
                children = this.buildGroupChildren(device, '');
            }

            const nameMatch = !this.treeFilterText || device.name.toLowerCase().includes(this.treeFilterText);
            if (nameMatch || children.length > 0) {
                treeNodes.push({
                    id: device.id,
                    name: device.name,
                    type: 'device',
                    deviceId: device.id,
                    children: children
                });
            }
        }
        this.treeDataSource.data = treeNodes;
        // Do NOT expand by default
    }

    /** Build a device folder tree node recursively */
    private buildDeviceFolderNode(
        folder: DeviceFolder,
        allFolders: { [id: string]: DeviceFolder },
        allDevices: Device[]
    ): TreeNode | null {
        const children: TreeNode[] = [];

        // Add sub-folders
        for (const subFolder of Object.values(allFolders)) {
            if (subFolder.parentId === folder.id) {
                const childNode = this.buildDeviceFolderNode(subFolder, allFolders, allDevices);
                if (childNode) {
                    children.push(childNode);
                }
            }
        }

        // Add devices in this folder
        for (const device of allDevices) {
            if (!device || device.folderId !== folder.id) continue;

            let tagChildren: TreeNode[];
            if (this.treeFilterText) {
                tagChildren = this.buildFilteredGroupChildren(device, '');
            } else {
                tagChildren = this.buildGroupChildren(device, '');
            }

            const nameMatch = !this.treeFilterText || device.name.toLowerCase().includes(this.treeFilterText);
            if (nameMatch || tagChildren.length > 0) {
                children.push({
                    id: device.id,
                    name: device.name,
                    type: 'device',
                    deviceId: device.id,
                    children: tagChildren
                });
            }
        }

        const nameMatch = !this.treeFilterText || folder.name.toLowerCase().includes(this.treeFilterText);
        if (nameMatch || children.length > 0) {
            return {
                id: folder.id,
                name: folder.name,
                type: 'folder',
                deviceId: '',
                children: children
            };
        }
        return null;
    }

    /** Build child group nodes recursively */
    private buildGroupChildren(device: Device, parentId: string): TreeNode[] {
        const children: TreeNode[] = [];
        if (!device.tagGroups) return children;

        for (const group of Object.values(device.tagGroups) as TagGroup[]) {
            if ((group.parentId || '') === parentId) {
                const groupNode: TreeNode = {
                    id: group.id,
                    name: group.name,
                    type: 'group',
                    deviceId: device.id,
                    children: this.buildGroupChildren(device, group.id)
                };
                children.push(groupNode);
            }
        }
        return children;
    }

    /** Handle tree node selection */
    onNodeSelect(node: FlatNode) {
        this.selectedNode = node;

        // Folder nodes: clear table, no device selected
        if (node.type === 'folder') {
            this.selectedDevice = null;
            this.dataSource.data = [];
            this.tagsMap = {};
            return;
        }

        const devices = this.projectService.getDevices();
        if (!devices) return;

        this.selectedDevice = (Object.values(devices) as Device[]).find(d => d.id === node.deviceId) || null;
        if (!this.selectedDevice) return;

        // Update columns based on device type
        this.updateColumns();

        // Build tags map for this device
        this.tagsMap = {};
        if (this.selectedDevice.tags) {
            Object.values(this.selectedDevice.tags).forEach((t: Tag) => {
                this.tagsMap[t.id] = t;
            });
        }

        // Initialize type filter options and selection (reset on node change)
        const allTags = Object.values(this.selectedDevice.tags || {}) as Tag[];
        this.typeFilterOptions = [...new Set(allTags.map(t => t.type).filter(Boolean))].sort();
        this.selectedTypes = new Set(this.typeFilterOptions);
        this.textFilterValue = '';

        this.bindToTable();
    }

    /** Update displayed columns based on selected device type */
    private updateColumns() {
        if (!this.selectedDevice) return;
        this.isDeviceToEdit = !Device.isWebApiProperty(this.selectedDevice);
        let baseColumns: string[];
        if (this.selectedDevice.type === DeviceType.internal) {
            baseColumns = this.defInternalColumns;
        } else if (this.selectedDevice.type === DeviceType.GPIO) {
            baseColumns = this.defGpioColumns;
        } else if (this.selectedDevice.type === DeviceType.WebCam) {
            baseColumns = this.defWebcamColumns;
        } else if (this.selectedDevice.type === DeviceType.REDIS) {
            baseColumns = this.defAllExtColumns;
        } else {
            baseColumns = this.defAllColumns;
        }
        this.isWithOptions = (this.selectedDevice.type === DeviceType.internal || this.selectedDevice.type === DeviceType.WebCam) ? false : true;

        // Filter out system columns (frozen left/right) from user-configurable columns
        const systemCols = new Set([...DeviceTreeComponent.LEFT_FROZEN_COLUMNS, ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS]);
        const userColumns = baseColumns.filter(c => !systemCols.has(c));

        // Apply saved column settings for user-configurable columns only
        this.columnSettings = this.loadColumnConfig(userColumns);
        const visibleUserCols = this.columnSettings.filter(s => s.visible).map(s => s.key);
        this.displayedColumns = [
            ...DeviceTreeComponent.LEFT_FROZEN_COLUMNS,
            ...visibleUserCols,
            ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS
        ];
        this.applyColumnStyles();
    }

    /** Bind filtered tags to the table (preserves type filter state) */
    private bindToTable() {
        if (!this.selectedDevice || !this.selectedNode) {
            this.dataSource.data = [];
            return;
        }
        let tags: Tag[];
        if (this.selectedNode.type === 'device') {
            tags = Object.values(this.selectedDevice.tags || {}).filter(
                (t: Tag) => !t.groupId
            );
        } else {
            tags = Object.values(this.selectedDevice.tags || {}).filter(
                (t: Tag) => t.groupId === this.selectedNode.id
            );
        }
        this.dataSource.data = tags;

        // Update type options to include any new types added during editing
        const currentTypes = new Set(tags.map(t => t.type).filter(Boolean));
        for (const t of currentTypes) {
            if (!this.typeFilterOptions.includes(t)) {
                this.typeFilterOptions.push(t);
                this.selectedTypes.add(t);
            }
        }
        this.typeFilterOptions.sort();

        // Set up combined filter predicate (text + type)
        this.dataSource.filterPredicate = (data: any, filter: string) => {
            const f = JSON.parse(filter);
            const textMatch = !f.text || JSON.stringify(data).toLowerCase().includes(f.text);
            const typeMatch = !f.types || f.types.length === 0 || f.types.includes(data.type);
            return textMatch && typeMatch;
        };
        this.applyCombinedFilter();

        this.reassignPaginator();
        this.hmiService.viewsTagsSubscribe(tags.map(t => t.id));
    }

    /** Get tag address display */
    getAddress(tag: Tag): string {
        if (!tag.address) return '';
        if (this.selectedDevice?.type === DeviceType.ModbusRTU || this.selectedDevice?.type === DeviceType.ModbusTCP) {
            return String(parseInt(tag.address) + parseInt(tag.memaddress));
        } else if (this.selectedDevice?.type === DeviceType.WebAPI) {
            if (tag.options) {
                return tag.address + ' / ' + tag.options.selval;
            }
            return tag.address;
        } else if (this.selectedDevice?.type === DeviceType.MQTTclient) {
            if (tag.options && tag.options.subs && tag.type === 'json') {
                return this.tagPropertyService.formatAddress(tag.address, tag.memaddress);
            }
            return tag.address;
        }
        return tag.address;
    }

    /** Get tag label */
    getTagLabel(tag: Tag): string {
        if (this.selectedDevice?.type === DeviceType.BACnet || this.selectedDevice?.type === DeviceType.WebAPI) {
            return tag.label || tag.name;
        } else if (this.selectedDevice?.type === DeviceType.OPCUA) {
            return tag.label;
        }
        return tag.name;
    }

    /** Check if tag is editable */
    isToEdit(tag: Tag): boolean {
        if (!this.selectedDevice) return false;
        const type = this.selectedDevice.type;
        if (type === DeviceType.SiemensS7 || type === DeviceType.ModbusTCP || type === DeviceType.ModbusRTU ||
            type === DeviceType.internal || type === DeviceType.EthernetIP || type === DeviceType.FuxaServer ||
            type === DeviceType.OPCUA || type === DeviceType.GPIO || type === DeviceType.ADSclient ||
            type === DeviceType.WebCam || type === DeviceType.MELSEC || type === DeviceType.REDIS) {
            return true;
        } else if (type === DeviceType.MQTTclient) {
            if (tag && tag.options && (tag.options.pubs || tag.options.subs)) {
                return true;
            }
        }
        return false;
    }

    /** Edit tag row */
    onEditRow(row: Tag) {
        if (!this.selectedDevice) return;
        if (this.selectedDevice.type === DeviceType.MQTTclient) {
            this.editTopics(row);
        } else {
            this.editTag(row, false);
        }
    }

    /** Add new tag */
    onAddTag() {
        if (!this.selectedDevice) return;
        if (this.selectedDevice.type === DeviceType.OPCUA || this.selectedDevice.type === DeviceType.BACnet || this.selectedDevice.type === DeviceType.WebAPI) {
            this.addOpcTags();
        } else if (this.selectedDevice.type === DeviceType.MQTTclient) {
            this.editTopics();
        } else {
            let tag = new Tag(Utils.getGUID(TAG_PREFIX));
            if (this.selectedNode && this.selectedNode.type === 'group') {
                tag.groupId = this.selectedNode.id;
            }
            this.editTag(tag, true);
        }
    }

    /** Add OPC/BACnet/WebAPI tags */
    private addOpcTags() {
        if (this.selectedDevice.type === DeviceType.OPCUA) {
            this.tagPropertyService.addTagsOpcUa(this.selectedDevice, this.tagsMap).subscribe(() => {
                this.bindToTable();
            });
        } else if (this.selectedDevice.type === DeviceType.BACnet) {
            this.tagPropertyService.editTagPropertyBacnet(this.selectedDevice, this.tagsMap).subscribe(() => {
                this.bindToTable();
            });
        } else if (this.selectedDevice.type === DeviceType.WebAPI) {
            this.tagPropertyService.editTagPropertyWebapi(this.selectedDevice, this.tagsMap).subscribe(() => {
                this.bindToTable();
            });
        }
    }

    /** Edit tag using tag property service */
    private editTag(tag: Tag, checkToAdd: boolean) {
        const type = this.selectedDevice.type;
        const afterEdit = () => {
            this.tagsMap[tag.id] = tag;
            this.bindToTable();
        };

        if (type === DeviceType.SiemensS7) {
            this.tagPropertyService.editTagPropertyS7(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.FuxaServer) {
            this.tagPropertyService.editTagPropertyServer(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.ModbusRTU || type === DeviceType.ModbusTCP) {
            this.tagPropertyService.editTagPropertyModbus(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.internal) {
            this.tagPropertyService.editTagPropertyInternal(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.EthernetIP) {
            this.tagPropertyService.editTagPropertyEthernetIp(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.OPCUA) {
            this.tagPropertyService.editTagPropertyOpcUa(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.ADSclient) {
            this.tagPropertyService.editTagPropertyADSclient(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.GPIO) {
            this.tagPropertyService.editTagPropertyGpio(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.WebCam) {
            this.tagPropertyService.editTagPropertyWebcam(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.MELSEC) {
            this.tagPropertyService.editTagPropertyMelsec(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        } else if (type === DeviceType.REDIS) {
            this.tagPropertyService.editTagPropertyRedis(this.selectedDevice, tag, checkToAdd).subscribe(afterEdit);
        }
    }

    /** Edit MQTT topic */
    private editTopics(topic: Tag = null) {
        this.tagPropertyService.editTagPropertyMqtt(this.selectedDevice, topic, this.tagsMap, () => {
            this.bindToTable();
        });
    }

    /** Edit tag options (DAQ, format, etc.) */
    onEditOptions(row: Tag) {
        let dialogRef = this.dialog.open(TagOptionsComponent, {
            disableClose: true,
            data: { device: this.selectedDevice, tags: [row] },
            position: { top: '60px' }
        });
        dialogRef.afterClosed().subscribe((tagOption: TagOptionType) => {
            if (tagOption) {
                row.daq = tagOption.daq;
                row.format = tagOption.format;
                row.deadband = tagOption.deadband;
                row.scale = tagOption.scale;
                row.scaleReadFunction = tagOption.scaleReadFunction;
                row.scaleReadParams = tagOption.scaleReadParams;
                row.scaleWriteFunction = tagOption.scaleWriteFunction;
                row.scaleWriteParams = tagOption.scaleWriteParams;
                row.unsPath = tagOption.unsPath;
                this.projectService.setDeviceTags(this.selectedDevice);
            }
        });
    }

    /** Remove a tag */
    onRemoveRow(row: Tag) {
        if (!this.selectedDevice) return;
        delete this.selectedDevice.tags[row.id];
        this.bindToTable();
        this.projectService.setDeviceTags(this.selectedDevice);
    }

    /** Copy tag to clipboard */
    onCopyTagToClipboard(tag: Tag) {
        Utils.copyToClipboard(JSON.stringify(tag));
    }

    /** Copy tag ID to clipboard on double-click */
    onCopyId(element: any) {
        if (element?.id) {
            Utils.copyToClipboard(element.id);
            this.snackBar.open(`变量ID已复制: ${element.id}`, undefined, { duration: 2000 });
        }
    }

    /** Open write value dialog */
    onWriteValue(tag: Tag) {
        let dialogRef = this.dialog.open(TagWriteValueComponent, {
            data: <TagWriteValueData>{
                tagName: tag.name || tag.label,
                tagType: tag.type,
                access: tag.access || 'rw'
            },
            position: { top: '60px' }
        });
        dialogRef.afterClosed().subscribe(result => {
            if (result !== undefined && result !== null) {
                this.hmiService.putSignalValue(tag.id, result);
            }
        });
    }

    /** Get access display text */
    getAccessLabel(tag: Tag): string {
        return tag.access === 'ro' ? 'RO' : 'RW';
    }

    /** Filter tags by text */
    applyFilter(filterValue: string) {
        this.textFilterValue = filterValue.trim().toLowerCase();
        this.applyCombinedFilter();
    }

    /** Apply combined text + type filter to dataSource */
    private applyCombinedFilter() {
        this.dataSource.filter = JSON.stringify({
            text: this.textFilterValue,
            types: this.selectedTypes.size === this.typeFilterOptions.length ? [] : Array.from(this.selectedTypes)
        });
    }

    /** Check if a type is in the filter */
    isTypeFilterActive(): boolean {
        return this.selectedTypes.size < this.typeFilterOptions.length;
    }

    /** Check if all types are selected */
    isAllTypesSelected(): boolean {
        return this.selectedTypes.size === this.typeFilterOptions.length && this.typeFilterOptions.length > 0;
    }

    /** Check if some but not all types are selected */
    isTypeIndeterminate(): boolean {
        return this.selectedTypes.size > 0 && this.selectedTypes.size < this.typeFilterOptions.length;
    }

    /** Toggle a single type in the filter */
    toggleTypeFilter(type: string) {
        if (this.selectedTypes.has(type)) {
            this.selectedTypes.delete(type);
        } else {
            this.selectedTypes.add(type);
        }
        this.applyCombinedFilter();
    }

    /** Toggle select all types */
    toggleAllTypes() {
        if (this.isAllTypesSelected()) {
            this.selectedTypes.clear();
        } else {
            this.selectedTypes = new Set(this.typeFilterOptions);
        }
        this.applyCombinedFilter();
    }

    /** Total filtered rows count */
    get totalRows(): number {
        return this.dataSource.filteredData?.length ?? 0;
    }

    get currentPage(): number {
        return (this.paginator?.pageIndex ?? 0) + 1;
    }

    get totalPages(): number {
        if (!this.paginator || !this.paginator.pageSize) return 1;
        return Math.max(1, Math.ceil(this.totalRows / this.paginator.pageSize));
    }

    onPageSizeChange(size: number) {
        if (!this.paginator) return;
        this.currentPageSize = size;
        this.paginator.pageSize = size;
        this.paginator.pageIndex = 0;
        this.paginator.page.emit({ pageIndex: 0, pageSize: size, length: this.paginator.length });
        this.changeDetector.detectChanges();
    }

    prevPage() {
        if (!this.paginator || this.paginator.pageIndex === 0) return;
        this.paginator.pageIndex--;
        this.paginator.page.emit({ pageIndex: this.paginator.pageIndex, pageSize: this.paginator.pageSize, length: this.paginator.length });
        this.changeDetector.detectChanges();
    }

    nextPage() {
        if (!this.paginator || this.currentPage >= this.totalPages) return;
        this.paginator.pageIndex++;
        this.paginator.page.emit({ pageIndex: this.paginator.pageIndex, pageSize: this.paginator.pageSize, length: this.paginator.length });
        this.changeDetector.detectChanges();
    }

    goToPage() {
        if (!this.paginator || this.pageJumpInput == null) return;
        const page = Math.max(1, Math.min(Number(this.pageJumpInput), this.totalPages));
        this.paginator.pageIndex = page - 1;
        this.paginator.page.emit({ pageIndex: this.paginator.pageIndex, pageSize: this.paginator.pageSize, length: this.paginator.length });
        this.pageJumpInput = null;
        this.changeDetector.detectChanges();
    }

    /** Add a new folder under the selected node */
    addGroup() {
        if (!this.selectedNode || !this.selectedDevice) return;

        const parentId = this.selectedNode.type === 'group' ? this.selectedNode.id : '';
        const deviceId = this.selectedNode.deviceId;

        let dialogRef = this.dialog.open(EditNameComponent, {
            data: { name: '' },
            position: { top: '60px' }
        });

        dialogRef.afterClosed().subscribe((result: any) => {
            if (result && result.name) {
                const device = this.getDeviceById(deviceId);
                if (!device) return;
                if (!device.tagGroups) device.tagGroups = {};

                const groupId = Utils.getGUID(TAG_GROUP_PREFIX);
                const newGroup: TagGroup = new TagGroup(groupId, deviceId, result.name, parentId);
                device.tagGroups[groupId] = newGroup;
                this.projectService.setDeviceTags(device);
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Rename the selected folder */
    renameGroup() {
        if (!this.selectedNode || this.selectedNode.type !== 'group') return;

        const device = this.getDeviceById(this.selectedNode.deviceId);
        if (!device || !device.tagGroups) return;
        const group = device.tagGroups[this.selectedNode.id];
        if (!group) return;

        let dialogRef = this.dialog.open(EditNameComponent, {
            data: { name: group.name },
            position: { top: '60px' }
        });

        dialogRef.afterClosed().subscribe((result: any) => {
            if (result && result.name) {
                group.name = result.name;
                this.projectService.setDeviceTags(device);
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Delete the selected folder and move tags to root */
    deleteGroup() {
        if (!this.selectedNode || this.selectedNode.type !== 'group') return;

        let msg = '';
        this.translateService.get('msg.confirm-delete-folder').subscribe((txt: string) => { msg = txt; });

        let dialogRef = this.dialog.open(ConfirmDialogComponent, {
            disableClose: true,
            data: { msg: msg || 'Delete this folder? Tags will be moved to root.' },
            position: { top: '60px' }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result) {
                const device = this.getDeviceById(this.selectedNode.deviceId);
                if (!device || !device.tagGroups) return;

                const groupId = this.selectedNode.id;
                // Recursively collect all child group ids
                const allGroupIds = this.collectChildGroupIds(device, groupId);
                allGroupIds.push(groupId);

                // Move all tags from these groups to root
                if (device.tags) {
                    for (const tag of Object.values(device.tags) as Tag[]) {
                        if (tag.groupId && allGroupIds.includes(tag.groupId)) {
                            delete tag.groupId;
                        }
                    }
                }

                // Delete all groups
                for (const gid of allGroupIds) {
                    delete device.tagGroups[gid];
                }

                this.projectService.setDeviceTags(device);
                this.selectedNode = null;
                this.dataSource.data = [];
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Collect all descendant group IDs */
    private collectChildGroupIds(device: Device, parentId: string): string[] {
        const ids: string[] = [];
        if (!device.tagGroups) return ids;
        for (const group of Object.values(device.tagGroups) as TagGroup[]) {
            if (group.parentId === parentId) {
                ids.push(group.id);
                ids.push(...this.collectChildGroupIds(device, group.id));
            }
        }
        return ids;
    }

    /** Get device by ID */
    private getDeviceById(deviceId: string): Device | null {
        const devices = this.projectService.getDevices();
        if (!devices) return null;
        return (Object.values(devices) as Device[]).find(d => d.id === deviceId) || null;
    }

    /** Add a new device folder */
    addDeviceFolder() {
        const parentId = (this.selectedNode && this.selectedNode.type === 'folder') ? this.selectedNode.id : '';
        let dialogRef = this.dialog.open(EditNameComponent, {
            data: { name: '' },
            position: { top: '60px' }
        });
        dialogRef.afterClosed().subscribe((result: any) => {
            if (result && result.name) {
                const folder = new DeviceFolder(Utils.getGUID(DEVICE_FOLDER_PREFIX), result.name, parentId);
                this.projectService.setDeviceFolder(folder);
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Rename the selected device folder */
    renameDeviceFolder() {
        if (!this.selectedNode || this.selectedNode.type !== 'folder') return;
        const folders = this.projectService.getDeviceFolders() as { [id: string]: DeviceFolder };
        const folder = folders[this.selectedNode.id];
        if (!folder) return;

        let dialogRef = this.dialog.open(EditNameComponent, {
            data: { name: folder.name },
            position: { top: '60px' }
        });
        dialogRef.afterClosed().subscribe((result: any) => {
            if (result && result.name) {
                folder.name = result.name;
                this.projectService.setDeviceFolder(folder);
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Delete the selected device folder, move children to parent */
    deleteDeviceFolder() {
        if (!this.selectedNode || this.selectedNode.type !== 'folder') return;

        let msg = '';
        this.translateService.get('msg.confirm-delete-folder').subscribe((txt: string) => { msg = txt; });

        let dialogRef = this.dialog.open(ConfirmDialogComponent, {
            disableClose: true,
            data: { msg: msg || 'Delete this folder? Contents will be moved to parent.' },
            position: { top: '60px' }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result) {
                const folders = this.projectService.getDeviceFolders() as { [id: string]: DeviceFolder };
                const folderId = this.selectedNode.id;
                const folder = folders[folderId];
                if (!folder) return;
                const parentId = folder.parentId || '';

                // Collect all descendant folder ids
                const toDelete = this.collectDeviceFolderIds(folders, folderId);
                toDelete.push(folderId);

                // Move devices from deleted folders to parent
                const allDevices = Object.values(this.projectService.getDevices() || {}) as Device[];
                for (const device of allDevices) {
                    if (device.folderId && toDelete.includes(device.folderId)) {
                        device.folderId = parentId || undefined;
                        this.projectService.setDevice(device, null);
                    }
                }

                // Move sub-folders that are NOT in toDelete to parent
                for (const f of Object.values(folders)) {
                    if (f.parentId === folderId && !toDelete.includes(f.id)) {
                        f.parentId = parentId;
                        this.projectService.setDeviceFolder(f);
                    }
                }

                // Delete all collected folders
                for (const id of toDelete) {
                    if (folders[id]) {
                        this.projectService.removeDeviceFolder(folders[id]);
                    }
                }

                this.selectedNode = null;
                this.selectedDevice = null;
                this.dataSource.data = [];
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Collect all descendant device folder IDs */
    private collectDeviceFolderIds(folders: { [id: string]: DeviceFolder }, parentId: string): string[] {
        const ids: string[] = [];
        for (const f of Object.values(folders)) {
            if (f.parentId === parentId) {
                ids.push(f.id);
                ids.push(...this.collectDeviceFolderIds(folders, f.id));
            }
        }
        return ids;
    }

    /** Check if node is selected */
    isSelected(node: FlatNode): boolean {
        return this.selectedNode?.id === node.id;
    }

    /** Get icon for node */
    getNodeIcon(node: FlatNode): string {
        if (node.type === 'device') return 'dns';
        if (node.type === 'folder') return this.treeControl.isExpanded(node) ? 'folder_open' : 'folder';
        return 'folder';
    }

    // ═══════════════════════════════════════════════
    // Device Status
    // ═══════════════════════════════════════════════

    /** Set device status from event */
    setDeviceStatus(event: any) {
        this.devicesStatus[event.id] = { status: event.status, last: new Date().getTime() };
        this.changeDetector.detectChanges();
    }

    /** Get device status color for tree node dot */
    getDeviceStatusColor(node: FlatNode): string {
        if (node.type !== 'device') return '';
        const device = this.getDeviceById(node.deviceId);
        if (!device) return '#999';
        if (!device.enabled) return '#999'; // gray - disabled
        if (this.devicesStatus[node.deviceId]) {
            const st = this.devicesStatus[node.deviceId].status;
            if (st === 'connect-ok') return '#00b050'; // green
            if (st === 'connect-error' || st === 'connect-failed') return '#ff2d2d'; // red
            if (st === 'connect-off' || st === 'connect-busy') return '#ffc000'; // yellow
        }
        return '#999'; // gray - unknown
    }

    // ═══════════════════════════════════════════════
    // Tree Search / Filter
    // ═══════════════════════════════════════════════

    /** Filter tree nodes by search text */
    applyTreeFilter(filterValue: string) {
        this.treeFilterText = filterValue.trim().toLowerCase();
        this.buildTree();
        if (this.treeFilterText) {
            this.treeControl.expandAll();
        }
    }

    /** Check if a tree node should be visible based on filter */
    isTreeNodeVisible(node: FlatNode): boolean {
        if (!this.treeFilterText) return true;
        return node.name.toLowerCase().includes(this.treeFilterText);
    }

    /** Override buildTree to support filtering */
    private buildFilteredGroupChildren(device: Device, parentId: string): TreeNode[] {
        const children: TreeNode[] = [];
        if (!device.tagGroups) return children;

        for (const group of Object.values(device.tagGroups) as TagGroup[]) {
            if ((group.parentId || '') === parentId) {
                const subChildren = this.buildFilteredGroupChildren(device, group.id);
                const nameMatch = group.name.toLowerCase().includes(this.treeFilterText);
                if (nameMatch || subChildren.length > 0) {
                    const groupNode: TreeNode = {
                        id: group.id,
                        name: group.name,
                        type: 'group',
                        deviceId: device.id,
                        children: subChildren
                    };
                    children.push(groupNode);
                }
            }
        }
        return children;
    }

    // ═══════════════════════════════════════════════
    // Device CRUD
    // ═══════════════════════════════════════════════

    /** Load available device types (plugins) */
    private loadAvailableType() {
        this.plugins = [];
        if (!this.appService.isClientApp && !this.appService.isDemoApp) {
            this.pluginService.getPlugins().subscribe(plugins => {
                Object.values(plugins).forEach((pg: any) => {
                    if (pg.current.length) {
                        this.plugins.push(pg.type);
                    }
                });
            }, error => {});
            this.plugins.push(DeviceType.WebAPI);
            this.plugins.push(DeviceType.MQTTclient);
            this.plugins.push(DeviceType.internal);
        } else {
            this.plugins.push(DeviceType.internal);
        }
    }

    /** Add a new device */
    addDevice() {
        let device = new Device(Utils.getGUID(DEVICE_PREFIX));
        device.property = new DeviceNetProperty();
        device.enabled = false;
        device.tags = {};
        // If a folder node is selected, place new device inside it
        if (this.selectedNode && this.selectedNode.type === 'folder') {
            device.folderId = this.selectedNode.id;
        }
        this.editDevice(device, false);
    }

    /** Edit an existing device */
    editSelectedDevice() {
        if (!this.selectedNode || this.selectedNode.type !== 'device') return;
        const device = this.getDeviceById(this.selectedNode.deviceId);
        if (!device) return;
        this.editDevice(device, false);
    }

    /** Restart device data collection */
    restartDevice() {
        if (!this.selectedNode || this.selectedNode.type !== 'device') return;
        const device = this.getDeviceById(this.selectedNode.deviceId);
        if (!device) return;
        this.hmiService.restartDevice(device.id);
    }

    /** Delete the selected device */
    deleteSelectedDevice() {
        if (!this.selectedNode || this.selectedNode.type !== 'device') return;
        if (this.selectedNode.deviceId === '0') return; // Internal device cannot be deleted
        const device = this.getDeviceById(this.selectedNode.deviceId);
        if (!device) return;
        this.editDevice(device, true);
    }

    /** Open device property dialog */
    private editDevice(device: Device, toRemove: boolean) {
        const devices = this.projectService.getDevices();
        let exist = Object.values(devices).filter((d: Device) => d.id !== device.id).map((d: Device) => d.name);
        exist.push('server');
        let tempdevice = JSON.parse(JSON.stringify(device));
        let dialogRef = this.dialog.open(DevicePropertyComponent, {
            disableClose: true,
            panelClass: 'dialog-property',
            data: {
                device: tempdevice, remove: toRemove, exist: exist, availableType: this.plugins,
                projectService: this.projectService
            },
            position: { top: '60px' }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result) {
                if (toRemove) {
                    this.projectService.removeDevice(device);
                } else {
                    let olddevice = JSON.parse(JSON.stringify(device));
                    device.name = tempdevice.name;
                    device.type = tempdevice.type;
                    device.enabled = tempdevice.enabled;
                    device.polling = tempdevice.polling;
                    device.folderId = tempdevice.folderId || '';
                    if (device.property && tempdevice.property) {
                        device.property.address = tempdevice.property.address;
                        device.property.port = parseInt(tempdevice.property.port);
                        device.property.slot = parseInt(tempdevice.property.slot);
                        device.property.rack = parseInt(tempdevice.property.rack);
                        device.property.cpuType = tempdevice.property.cpuType;
                        device.property.slaveid = tempdevice.property.slaveid;
                        device.property.baudrate = tempdevice.property.baudrate;
                        device.property.databits = tempdevice.property.databits;
                        device.property.stopbits = tempdevice.property.stopbits;
                        device.property.parity = tempdevice.property.parity;
                        device.property.options = tempdevice.property.options;
                        device.property.delay = tempdevice.property.delay;
                        device.property.method = tempdevice.property.method;
                        device.property.format = tempdevice.property.format;
                        device.property.broadcastAddress = tempdevice.property.broadcastAddress;
                        device.property.adpuTimeout = tempdevice.property.adpuTimeout;
                        device.property.local = tempdevice.property.local;
                        device.property.router = tempdevice.property.router;
                        if (tempdevice.property.connectionOption) {
                            device.property.connectionOption = tempdevice.property.connectionOption;
                        }
                        device.property.socketReuse = tempdevice.property.socketReuse;
                        device.property.forceFC16 = tempdevice.property.forceFC16;
                    }
                    this.projectService.setDevice(device, olddevice, result.security);
                }
                this.selectedNode = null;
                this.dataSource.data = [];
                this.buildTree();
                this.changeDetector.detectChanges();
            }
        });
    }

    /**
     * Format tag value for display only.
     * Bool type: 0 -> 'false', 1 -> 'true'
     */
    formatTagValue(element: any): string {
        if (element.value == null || element.value === '') return '';
        if (element.type === 'Bool' || element.type === 'Boolean') {
            if (element.value === true || element.value === 1 || element.value === '1') return 'true';
            if (element.value === false || element.value === 0 || element.value === '0') return 'false';
        }
        return element.value;
    }

    // ═══════════════════════════════════════════════
    // Column Settings & Persistence
    // ═══════════════════════════════════════════════

    /** Open column settings dialog */
    openColumnSettings() {
        if (!this.selectedDevice) return;
        const baseColumns = this.getBaseColumnsForType(this.selectedDevice.type);
        // Exclude system (frozen) columns from dialog
        const systemCols = new Set([...DeviceTreeComponent.LEFT_FROZEN_COLUMNS, ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS]);
        const userColumns = baseColumns.filter(c => !systemCols.has(c));
        const dialogRef = this.dialog.open(DeviceTreeColumnSettingsComponent, {
            data: {
                allColumns: userColumns,
                currentSettings: this.columnSettings
            },
            position: { top: '60px' }
        });
        dialogRef.afterClosed().subscribe((result: ColumnSetting[] | null) => {
            if (result && this.selectedDevice) {
                // Merge: keep settings for columns not in current device's list
                const resultKeys = new Set(result.map(s => s.key));
                const otherSettings = this.columnSettings.filter(s => !resultKeys.has(s.key));
                this.columnSettings = [...otherSettings, ...result];
                this.saveColumnConfig(this.columnSettings);
                const visibleUserCols = this.columnSettings
                    .filter(s => s.visible && userColumns.includes(s.key))
                    .map(s => s.key);
                this.displayedColumns = [
                    ...DeviceTreeComponent.LEFT_FROZEN_COLUMNS,
                    ...visibleUserCols,
                    ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS
                ];
                this.applyColumnStyles();
                this.changeDetector.detectChanges();
            }
        });
    }

    /** Get base (un-customized) columns for a device type */
    private getBaseColumnsForType(deviceType: string): string[] {
        if (deviceType === DeviceType.internal) return [...this.defInternalColumns];
        if (deviceType === DeviceType.GPIO) return [...this.defGpioColumns];
        if (deviceType === DeviceType.WebCam) return [...this.defWebcamColumns];
        if (deviceType === DeviceType.REDIS) return [...this.defAllExtColumns];
        return [...this.defAllColumns];
    }

    /** Apply custom column widths, borders, and resize handles via dynamic <style> element */
    private applyColumnStyles() {
        if (!this.columnStyleElement) {
            this.columnStyleElement = document.createElement('style');
            this.columnStyleElement.id = 'device-tree-column-styles';
            document.head.appendChild(this.columnStyleElement);
        }
        const rules: string[] = [];

        // Column width rules
        for (const s of this.columnSettings) {
            const w = this.resizeWidths[s.key] || s.width;
            if (w) {
                rules.push(`.tags-panel .mat-column-${s.key} { flex: 0 0 ${w}px !important; }`);
            }
        }

        // Header cell borders and resize handle (exclude system/frozen columns)
        const systemCols = [...DeviceTreeComponent.LEFT_FROZEN_COLUMNS, ...DeviceTreeComponent.RIGHT_FROZEN_COLUMNS];
        const notSelector = systemCols.map(c => `:not(.mat-column-${c})`).join('');
        
        // All header cells get bottom border
        rules.push(`
.tags-panel mat-header-cell,
.tags-panel .mat-mdc-header-cell {
    border-bottom: 2px solid rgba(128, 128, 128, 0.4) !important;
}`);
        
        // Only data columns get right border, position relative, and resize handle
        rules.push(`
.tags-panel mat-header-cell${notSelector},
.tags-panel .mat-mdc-header-cell${notSelector} {
    position: relative !important;
    border-right: 1px solid rgba(128, 128, 128, 0.2) !important;
}
.tags-panel mat-header-cell${notSelector}::after,
.tags-panel .mat-mdc-header-cell${notSelector}::after {
    content: '';
    position: absolute;
    right: 0;
    top: 0;
    width: 6px;
    height: 100%;
    cursor: col-resize;
    z-index: 100;
    background-color: transparent;
    transition: background-color 0.2s;
}
.tags-panel mat-header-cell${notSelector}::after:hover,
.tags-panel .mat-mdc-header-cell${notSelector}::after:hover {
    background-color: var(--primaryColor, #1976d2);
    opacity: 0.6;
}`);

        // Data cell styles (text overflow, borders)
        rules.push(`
.tags-panel mat-cell,
.tags-panel .mat-mdc-cell,
.tags-panel .mdc-data-table__cell {
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    border-bottom: 1px solid rgba(128, 128, 128, 0.25) !important;
}
.tags-panel mat-row,
.tags-panel .mat-mdc-row,
.tags-panel .my-mat-row {
    border-bottom: 1px solid rgba(128, 128, 128, 0.12) !important;
}`);

        this.columnStyleElement.textContent = rules.join('\n');
    }

    /** Load global column config from localStorage */
    private loadColumnConfig(baseColumns: string[]): ColumnSetting[] {
        try {
            const stored = localStorage.getItem(DeviceTreeComponent.STORAGE_KEY);
            if (stored) {
                const saved: ColumnSetting[] = JSON.parse(stored);
                if (saved && saved.length > 0) {
                    // Merge saved settings with current device's base columns
                    const savedMap = new Map(saved.map(s => [s.key, s]));
                    return baseColumns.map(key => {
                        const s = savedMap.get(key);
                        return s ? { key, visible: s.visible, width: s.width } : { key, visible: true, width: null };
                    });
                }
            }
        } catch (e) { /* ignore */ }
        return baseColumns.map(key => ({ key, visible: true, width: null }));
    }

    /** Save global column config to localStorage */
    private saveColumnConfig(settings: ColumnSetting[]) {
        try {
            localStorage.setItem(DeviceTreeComponent.STORAGE_KEY, JSON.stringify(settings));
        } catch (e) { /* ignore */ }
    }
}
