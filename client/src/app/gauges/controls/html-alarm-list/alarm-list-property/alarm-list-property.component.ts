import { Component, EventEmitter, OnInit, Input, Output, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { TranslateService } from '@ngx-translate/core';

import { GaugeAlarmListProperty, AlarmListOptions } from '../../../../_models/hmi';
import { AlarmColumns, AlarmColumnsType, AlarmHistoryColumns } from '../../../../_models/alarm';
import { Utils } from '../../../../_helpers/utils';
import { AlarmListGaugeComponent } from '../alarm-list/alarm-list.component';
import { AlarmListGroupsComponent } from '../alarm-list-groups/alarm-list-groups.component';

@Component({
    selector: 'app-alarm-list-property',
    templateUrl: './alarm-list-property.component.html',
    styleUrls: ['./alarm-list-property.component.scss']
})
export class AlarmListPropertyComponent implements OnInit, OnDestroy {

    @Input() data: any;
    @Output() onPropChanged: EventEmitter<any> = new EventEmitter();
    @Input('reload') set reload(b: any) {
        this._reload();
    }

    defaultColor = Utils.defaultColor;
    property: GaugeAlarmListProperty;
    options: AlarmListOptions;
    allColumns: string[] = AlarmColumns;
    historyAllColumns: string[] = AlarmHistoryColumns;

    private destroy$ = new Subject<void>();

    constructor(private dialog: MatDialog,
                private translateService: TranslateService) { }

    ngOnInit() {
        this._reload();
    }

    ngOnDestroy() {
        this.destroy$.next(null);
        this.destroy$.complete();
    }

    private _reload() {
        this.property = this.data?.settings?.property;
        if (this.property) {
            this.options = { ...AlarmListGaugeComponent.DefaultOptions(), ...this.property.options };
            if (!this.property.historyColumns) {
                this.property.historyColumns = [...AlarmHistoryColumns];
            }
        } else {
            this.property = <GaugeAlarmListProperty>{
                groups: [],
                columns: [...AlarmColumns],
                historyColumns: [...AlarmHistoryColumns],
                options: AlarmListGaugeComponent.DefaultOptions()
            };
            this.options = { ...AlarmListGaugeComponent.DefaultOptions() };
        }
    }

    onChanged() {
        this.property.options = JSON.parse(JSON.stringify(this.options));
        this.onPropChanged.emit(this.data.settings);
    }

    isColumnSelected(colId: string): boolean {
        return this.property.columns?.includes(colId) || false;
    }

    onColumnToggle(colId: string, selected: boolean) {
        if (!this.property.columns) {
            this.property.columns = [];
        }
        const index = this.property.columns.indexOf(colId);
        if (selected && index === -1) {
            this.property.columns.push(colId);
        } else if (!selected && index !== -1) {
            this.property.columns.splice(index, 1);
        }
        this.onChanged();
    }

    isHistoryColumnSelected(colId: string): boolean {
        return this.property.historyColumns?.includes(colId) || false;
    }

    onHistoryColumnToggle(colId: string, selected: boolean) {
        if (!this.property.historyColumns) {
            this.property.historyColumns = [];
        }
        const index = this.property.historyColumns.indexOf(colId);
        if (selected && index === -1) {
            this.property.historyColumns.push(colId);
        } else if (!selected && index !== -1) {
            this.property.historyColumns.splice(index, 1);
        }
        this.onChanged();
    }

    getColumnName(colId: string): string {
        return this.translateService.instant('alarms.view-' + colId);
    }

    getColumnWidth(colId: string): number {
        return this.options?.columnWidths?.[colId] || 0;
    }

    getAllColumnsUnion(): string[] {
        const seen = new Set<string>();
        for (const col of AlarmColumns) { seen.add(col); }
        for (const col of AlarmHistoryColumns) { seen.add(col); }
        return Array.from(seen);
    }

    setColumnWidth(colId: string, value: number) {
        if (!this.options.columnWidths) {
            this.options.columnWidths = {};
        }
        this.options.columnWidths[colId] = value || 0;
        this.onChanged();
    }

    onSelectGroups() {
        this.dialog.open(AlarmListGroupsComponent, {
            data: { groups: this.property.groups || [] },
            width: '450px',
            position: { top: '60px' }
        }).afterClosed().subscribe((result: string[]) => {
            if (result) {
                this.property.groups = result;
                this.onChanged();
            }
        });
    }

    getGroupsDisplayText(): string {
        if (!this.property.groups || this.property.groups.length === 0) {
            return this.translateService.instant('alarmlist.property-groups-all');
        }
        return this.property.groups.join(', ');
    }
}
