import { Component, OnInit, AfterViewInit, ViewChild, OnDestroy, HostBinding } from '@angular/core';
import { MatTable, MatTableDataSource } from '@angular/material/table';
import { MatPaginator } from '@angular/material/paginator';
import { MatSort } from '@angular/material/sort';
import { MatDialog } from '@angular/material/dialog';
import { Subject, Subscription } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { TranslateService } from '@ngx-translate/core';
import { format } from 'fecha';

import { HmiService } from '../../../../_services/hmi.service';
import { LanguageService } from '../../../../_services/language.service';
import { AlarmBaseType, AlarmColumnsType, AlarmHistoryColumns, AlarmPriorityType, AlarmQuery, AlarmStatusType } from '../../../../_models/alarm';
import { GaugeAlarmListProperty, AlarmListOptions, IDateRange } from '../../../../_models/hmi';
import { DaterangeDialogComponent } from '../../../../gui-helpers/daterange-dialog/daterange-dialog.component';

@Component({
    selector: 'app-alarm-list-gauge',
    templateUrl: './alarm-list.component.html',
    styleUrls: ['./alarm-list.component.scss'],
})
export class AlarmListGaugeComponent implements OnInit, AfterViewInit, OnDestroy {

    @ViewChild(MatTable, { static: false }) table: MatTable<any>;
    @ViewChild(MatSort, { static: false }) sort: MatSort;
    @ViewChild(MatPaginator, { static: false }) paginator: MatPaginator;

    @HostBinding('class.has-border') get hasBorder(): boolean {
        return !!this.options?.showBorder;
    }

    id: string;
    isEditor: boolean;
    property: GaugeAlarmListProperty;

    statusText = AlarmStatusType;
    priorityText = AlarmPriorityType;
    alarmColumnType = AlarmColumnsType;

    displayedColumns: string[] = [];
    dataSource = new MatTableDataSource<AlarmRow>([]);
    options: AlarmListOptions;

    isHistoryMode = false;
    realtimeColumns: string[] = [];
    dateRangeLabel = '';

    private destroy$ = new Subject<void>();
    private realtimeSub: Subscription;
    private lastRenderedSignature = '';

    constructor(
        private hmiService: HmiService,
        private languageService: LanguageService,
        private translateService: TranslateService,
        private dialog: MatDialog) { }

    ngOnInit() {
        if (!this.property) {
            this.property = <GaugeAlarmListProperty>{
                groups: [],
                columns: ['ontime', 'text', 'type', 'group', 'status', 'ack'],
                historyColumns: ['ontime', 'text', 'type', 'group', 'status', 'offtime', 'acktime', 'userack'],
                options: AlarmListGaugeComponent.DefaultOptions()
            };
        }
        this.options = { ...AlarmListGaugeComponent.DefaultOptions(), ...this.property.options };
        this.realtimeColumns = this.property.columns || ['ontime', 'text', 'type', 'group', 'status', 'ack'];
        this.displayedColumns = [...this.realtimeColumns];

        Object.keys(this.statusText).forEach(key => {
            this.statusText[key] = this.translateService.instant(this.statusText[key]);
        });
        Object.keys(this.priorityText).forEach(key => {
            this.priorityText[key] = this.translateService.instant(this.priorityText[key]);
        });

        if (!this.isEditor) {
            this.startRealtimeSubscription();
        }
    }

    ngAfterViewInit() {
        if (this.sort) {
            this.sort.disabled = false;
        }
        this.bindTableControls();
    }

    ngOnDestroy() {
        this.stopRealtimeSubscription();
        this.destroy$.next(null);
        this.destroy$.complete();
    }

    // #region Column width

    getColumnFlex(colId: string): string {
        const w = this.options?.columnWidths?.[colId];
        if (w && w > 0) {
            return `0 0 ${w}px`;
        }
        return '1 1 0px';
    }

    // #endregion

    // #region Realtime subscription

    private startRealtimeSubscription() {
        this.hmiService.getAlarmsValues().subscribe(result => {
            this.updateAlarmsTable(result);
        });
        this.realtimeSub = this.hmiService.onAlarmsStatus.pipe(
            takeUntil(this.destroy$)
        ).subscribe(() => {
            this.hmiService.getAlarmsValues().subscribe(result => {
                this.updateAlarmsTable(result);
            });
        });
    }

    private stopRealtimeSubscription() {
        if (this.realtimeSub) {
            this.realtimeSub.unsubscribe();
            this.realtimeSub = null;
        }
    }

    // #endregion

    // #region Toolbar: Realtime / History toggle

    onToggleMode() {
        if (this.isHistoryMode) {
            // Switch back to realtime
            this.isHistoryMode = false;
            this.dateRangeLabel = '';
            this.displayedColumns = [...this.realtimeColumns];
            this.dataSource.data = [];
            this.lastRenderedSignature = '';
            this.startRealtimeSubscription();
            setTimeout(() => this.bindTableControls(), 100);
        }
    }

    onDateRange() {
        if (this.isEditor) { return; }
        const dialogRef = this.dialog.open(DaterangeDialogComponent, {
            panelClass: 'light-dialog-container'
        });
        dialogRef.afterClosed().subscribe((dateRange: IDateRange) => {
            if (dateRange) {
                // Switch to history mode
                this.isHistoryMode = true;
                this.stopRealtimeSubscription();

                // Use configured history columns
                this.displayedColumns = this.property.historyColumns?.length
                    ? [...this.property.historyColumns]
                    : ['ontime', 'text', 'type', 'group', 'status', 'offtime', 'acktime', 'userack'];

                // Format range label
                this.dateRangeLabel = `${format(new Date(dateRange.start), 'MM/DD HH:mm')} - ${format(new Date(dateRange.end), 'MM/DD HH:mm')}`;

                // Query history
                const query: AlarmQuery = {
                    start: new Date(dateRange.start),
                    end: new Date(dateRange.end)
                };
                this.hmiService.getAlarmsHistory(query).subscribe(result => {
                    this.updateHistoryTable(result);
                });
                setTimeout(() => this.bindTableControls(), 100);
            }
        });
    }

    // #endregion

    // #region Data updates

    private updateAlarmsTable(alrs: AlarmBaseType[]) {
        let filtered = alrs;
        if (this.property.groups && this.property.groups.length > 0) {
            filtered = alrs.filter(alr => this.property.groups.includes(alr.group));
        }

        let rows: AlarmRow[] = [];
        filtered.forEach(alr => {
            let row: AlarmRow = {
                type: { stringValue: this.priorityText[alr.type] },
                name: { stringValue: alr.name },
                status: { stringValue: this.statusText[alr.status] },
                text: { stringValue: this.languageService.getTranslation(alr.text) ?? alr.text },
                group: { stringValue: this.languageService.getTranslation(alr.group) ?? alr.group },
                ontime: { stringValue: format(new Date(alr.ontime), 'YYYY.MM.DD HH:mm:ss') },
                color: alr.color,
                bkcolor: alr.bkcolor,
                toack: alr.toack
            };
            rows.push(row);
        });

        const nextSignature = rows.map(r => JSON.stringify(r)).join('|');
        if (nextSignature === this.lastRenderedSignature) {
            return;
        }
        this.lastRenderedSignature = nextSignature;
        this.dataSource.data = rows;
    }

    private updateHistoryTable(alrs: AlarmBaseType[]) {
        let filtered = alrs;
        if (this.property.groups && this.property.groups.length > 0) {
            filtered = alrs.filter(alr => this.property.groups.includes(alr.group));
        }

        const rows: AlarmRow[] = filtered.map(alr => ({
            type: { stringValue: this.priorityText[alr.type] ?? alr.type },
            name: { stringValue: alr.name },
            status: { stringValue: this.statusText[alr.status] ?? alr.status },
            text: { stringValue: this.languageService.getTranslation(alr.text) ?? alr.text },
            group: { stringValue: this.languageService.getTranslation(alr.group) ?? alr.group },
            ontime: { stringValue: alr.ontime ? format(new Date(alr.ontime), 'YYYY.MM.DD HH:mm:ss') : '' },
            offtime: { stringValue: alr.offtime ? format(new Date(alr.offtime), 'YYYY.MM.DD HH:mm:ss') : '' },
            acktime: { stringValue: alr.acktime ? format(new Date(alr.acktime), 'YYYY.MM.DD HH:mm:ss') : '' },
            userack: { stringValue: alr.userack ? String(alr.userack) : '' },
            color: alr.color,
            bkcolor: alr.bkcolor,
            toack: false
        }));

        this.lastRenderedSignature = '';
        this.dataSource.data = rows;
    }

    // #endregion

    onAckAlarm(alarm: AlarmRow) {
        if (!this.isEditor) {
            this.hmiService.setAlarmAck(alarm.name?.stringValue).subscribe(() => {
            }, error => {
                console.error('Error setAlarmAck', error);
            });
        }
    }

    getScrollHeight(): string {
        let offset = 28; // toolbar always visible
        if (this.options?.paginator?.show) { offset += 36; }
        return `calc(100% - ${offset}px)`;
    }

    private bindTableControls() {
        if (this.options.paginator.show && this.paginator) {
            this.dataSource.paginator = this.paginator;
        }
        if (this.sort) {
            this.dataSource.sort = this.sort;
            this.dataSource.sortingDataAccessor = (data, sortHeaderId) => data[sortHeaderId]?.stringValue || '';
        }
    }

    trackByRow(index: number, row: AlarmRow): string {
        return `${row?.name?.stringValue || ''}|${row?.ontime?.stringValue || ''}`;
    }

    public static DefaultOptions(): AlarmListOptions {
        return <AlarmListOptions>{
            header: {
                show: true,
                height: 30,
                background: '#F0F0F0',
                color: '#757575',
                fontSize: 12
            },
            row: {
                height: 28,
                fontSize: 10,
                background: '#F9F9F9',
                color: '#000000'
            },
            paginator: {
                show: false,
                pageSize: 25,
                fontSize: 12
            },
            columnWidths: {},
            showBorder: false
        };
    }
}

interface AlarmRow {
    type: { stringValue: string };
    name: { stringValue: string };
    status: { stringValue: string };
    text: { stringValue: string };
    group: { stringValue: string };
    ontime: { stringValue: string };
    offtime?: { stringValue: string };
    acktime?: { stringValue: string };
    userack?: { stringValue: string };
    color: string;
    bkcolor: string;
    toack: boolean;
}
