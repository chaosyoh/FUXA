import { Component, Inject, ViewChild } from '@angular/core';
import { TableCell, TableCellType, TableType, GaugeActionsType, GaugeProperty,
         GaugeEvent, GaugeEventType, GaugeEventActionType, InputOptionType,
         TableCellInputOptions, TableCellButtonOptions, TableCellSwitchOptions, TableCellSelectOptions, TableCellSelectItem, View } from '../../../../../_models/hmi';
import { ProjectService } from '../../../../../_services/project.service';
import { MatDialogRef as MatDialogRef, MAT_DIALOG_DATA as MAT_DIALOG_DATA } from '@angular/material/dialog';
import { TranslateService } from '@ngx-translate/core';
import { FlexActionComponent } from '../../../../gauge-property/flex-action/flex-action.component';
import { FlexEventComponent } from '../../../../gauge-property/flex-event/flex-event.component';
import { HtmlInputComponent } from '../../../../controls/html-input/html-input.component';
import { HtmlSwitchComponent } from '../../../../controls/html-switch/html-switch.component';
import { HtmlSelectComponent } from '../../../../controls/html-select/html-select.component';
import { Script } from '../../../../../_models/script';

@Component({
    selector: 'app-table-customizer-cell-edit',
    templateUrl: './table-customizer-cell-edit.component.html',
    styleUrls: ['./table-customizer-cell-edit.component.css']
})
export class TableCustomizerCellEditComponent {

    @ViewChild('flexaction', { static: false }) flexAction: FlexActionComponent;
    @ViewChild('inputFlexEvent', { static: false }) inputFlexEvent: FlexEventComponent;
    @ViewChild('buttonFlexEvent', { static: false }) buttonFlexEvent: FlexEventComponent;
    @ViewChild('switchFlexEvent', { static: false }) switchFlexEvent: FlexEventComponent;
    @ViewChild('selectFlexEvent', { static: false }) selectFlexEvent: FlexEventComponent;

    tableType = TableType;
    cellType = TableCustomizerCellRowType;
    columnType = TableCellType;
    inputOptionType = InputOptionType;
    devicesValues = { devices: null };
    dateTimeFormatsLabel = '';
    actionData: any;
    actionProperty: GaugeProperty;
    scripts: Script[];
    inputEventProperty: GaugeProperty;
    buttonEventProperty: GaugeProperty;
    inputEventData: any;
    buttonEventData: any;
    switchEventProperty: GaugeProperty;
    switchEventData: any;
    selectEventProperty: GaugeProperty;
    selectEventData: any;

    constructor(
        private projectService: ProjectService,
        private translateService: TranslateService,
        public dialogRef: MatDialogRef<TableCustomizerCellEditComponent>,
        @Inject(MAT_DIALOG_DATA) public data: TableCustomizerCellType) {

        this.devicesValues.devices = Object.values(this.projectService.getDevices());
        this.dateTimeFormatsLabel = this.translateService.instant('table.cell-ts-format');
        if (this.isHistory()) {
            this.dateTimeFormatsLabel += this.translateService.instant('table.cell-ts-interval');
        }
        this.scripts = this.projectService.getScripts();
        const views = this.projectService.getViews();
        // Build event data objects for flex-event component
        this.inputEventData = {
            devices: Object.values(this.projectService.getDevices()),
            views: views,
            settings: { type: HtmlInputComponent.TypeTag },
            inputs: []
        };
        this.buttonEventData = {
            devices: Object.values(this.projectService.getDevices()),
            views: views,
            inputs: []
        };
        this.switchEventData = {
            devices: Object.values(this.projectService.getDevices()),
            views: views,
            settings: { type: HtmlSwitchComponent.TypeTag },
            inputs: []
        };
        this.selectEventData = {
            devices: Object.values(this.projectService.getDevices()),
            views: views,
            settings: { type: HtmlSelectComponent.TypeTag },
            inputs: []
        };
        // Initialize action data for data-table row cells
        if (this.data.table === TableType.data) {
            this.actionData = {
                devices: Object.values(this.projectService.getDevices()),
                withActions: { color: GaugeActionsType.color }
            };
            this.actionProperty = <GaugeProperty>{ actions: this.data.cell.actions || [] };
        }
        // Initialize input/button options and events
        this.initCellOptions();
    }

    onNoClick(): void {
        this.dialogRef.close();
    }

    onOkClick(): void {
        if (this.flexAction) {
            this.data.cell.actions = this.flexAction.getActions() || [];
        }
        if (this.inputFlexEvent) {
            this.data.cell.inputEvents = this.inputFlexEvent.getEvents() || [];
        }
        if (this.buttonFlexEvent) {
            this.data.cell.buttonEvents = this.buttonFlexEvent.getEvents() || [];
        }
        if (this.switchFlexEvent) {
            this.data.cell.switchEvents = this.switchFlexEvent.getEvents() || [];
        }
        if (this.selectFlexEvent) {
            this.data.cell.selectEvents = this.selectFlexEvent.getEvents() || [];
        }
        this.dialogRef.close(this.data);
    }

    onAddAction(): void {
        this.flexAction.onAddAction();
    }

    onSetVariable(event) {
        this.data.cell.variableId = event.variableId;
        this.data.cell.bitmask = event.bitmask;
        if (this.data.table === TableType.data) {
            if (event.variableRaw) {
                this.data.cell.label = event.variableRaw.name;
                if (this.data.cell.type === TableCellType.device) {
                    let device = this.projectService.getDeviceFromTagId(event.variableId);
                    this.data.cell.label = device ? device.name : '';
                }
            } else {
                this.data.cell.label = null;
            }
        } else if (this.data.table === TableType.history) {
            if (event.variableRaw) {
                if (this.data.cell.type === TableCellType.device) {
                    let device = this.projectService.getDeviceFromTagId(event.variableId);
                    this.data.cell['exname'] = device ? device.name : '';
                }
            }
        }
    }

    isHistory(): boolean {
        return this.data.table === TableType.history;
    }

    onCellTypeChange(): void {
        this.initCellOptions();
    }

    isInputNumber(): boolean {
        return this.data.cell.inputOptions?.type === InputOptionType.number;
    }

    isInputTimeOrDate(): boolean {
        const t = this.data.cell.inputOptions?.type;
        return t === InputOptionType.time || t === InputOptionType.datetime || t === InputOptionType.date;
    }

    private initCellOptions(): void {
        if (this.data.cell.type === TableCellType.input) {
            if (!this.data.cell.inputOptions) {
                this.data.cell.inputOptions = <TableCellInputOptions>{
                    type: InputOptionType.text,
                    updated: true
                };
            }
            if (!this.data.cell.inputEvents) {
                this.data.cell.inputEvents = [];
            }
            this.inputEventProperty = <GaugeProperty>{ events: this.data.cell.inputEvents };
        }
        if (this.data.cell.type === TableCellType.button) {
            if (!this.data.cell.buttonOptions) {
                this.data.cell.buttonOptions = <TableCellButtonOptions>{
                    text: 'Button',
                    style: {}
                };
            }
            if (!this.data.cell.buttonEvents) {
                this.data.cell.buttonEvents = [];
            }
            this.buttonEventProperty = <GaugeProperty>{ events: this.data.cell.buttonEvents };
        }
        if (this.data.cell.type === TableCellType.switch) {
            if (!this.data.cell.switchOptions) {
                this.data.cell.switchOptions = <TableCellSwitchOptions>{
                    offValue: 0,
                    onValue: 1,
                    offBackground: '#ccc',
                    onBackground: '#ccc',
                    offSliderColor: '#fff',
                    onSliderColor: '#0CC868',
                    offTextColor: '#000',
                    onTextColor: '#fff',
                    offText: '',
                    onText: '',
                    fontSize: 12,
                    radius: 0
                };
            }
            if (!this.data.cell.switchEvents) {
                this.data.cell.switchEvents = [];
            }
            this.switchEventProperty = <GaugeProperty>{ events: this.data.cell.switchEvents };
        }
        if (this.data.cell.type === TableCellType.select) {
            if (!this.data.cell.selectOptions) {
                this.data.cell.selectOptions = <TableCellSelectOptions>{
                    items: [{ value: 0, text: 'Item 1' }, { value: 1, text: 'Item 2' }],
                    readonly: false
                };
            }
            if (!this.data.cell.selectEvents) {
                this.data.cell.selectEvents = [];
            }
            this.selectEventProperty = <GaugeProperty>{ events: this.data.cell.selectEvents };
        }
    }

    onAddSelectItem(): void {
        if (!this.data.cell.selectOptions?.items) {
            this.data.cell.selectOptions.items = [];
        }
        const nextValue = this.data.cell.selectOptions.items.length;
        this.data.cell.selectOptions.items.push(<TableCellSelectItem>{ value: nextValue, text: 'Item ' + (nextValue + 1) });
    }

    onRemoveSelectItem(index: number): void {
        this.data.cell.selectOptions?.items?.splice(index, 1);
    }
}

export interface TableCustomizerCellType {
    type: TableCustomizerCellRowType;
    cell: TableCell;
    table: TableType;
}

export enum TableCustomizerCellRowType {
    column,
    row,
}
