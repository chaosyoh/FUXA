import { Component, ViewChild } from '@angular/core';
import { MatDialogRef as MatDialogRef } from '@angular/material/dialog';

import * as moment from 'moment';
import { DaterangepickerComponent } from '../daterangepicker';
import { IDateRange } from '../../_models/hmi';

@Component({
    selector: 'app-daterange-dialog',
    templateUrl: './daterange-dialog.component.html',
    styleUrls: ['./daterange-dialog.component.css']
})
export class DaterangeDialogComponent {

    @ViewChild('dtrange', {static: false}) public dtrange: DaterangepickerComponent;

    options = { };

    quickRanges = [
        { label: '近8小时', getRange: () => [moment().subtract(8, 'hours'), moment()] },
        { label: '近1天', getRange: () => [moment().subtract(1, 'days'), moment()] },
        { label: '近7天', getRange: () => [moment().subtract(7, 'days'), moment()] },
        { label: '今日', getRange: () => [moment().startOf('day'), moment().endOf('day')] },
        { label: '本周', getRange: () => [moment().startOf('week'), moment().endOf('week')] },
    ];

    activeQuickRange: string = null;
    private savedStartDate: any = null;
    private savedEndDate: any = null;

    constructor(public dialogRef: MatDialogRef<DaterangeDialogComponent>) { }

    onQuickRangeHover(range: any) {
        if (!this.savedStartDate) {
            this.savedStartDate = this.dtrange.startDate.clone();
            this.savedEndDate = this.dtrange.endDate.clone();
        }
        const [start, end] = range.getRange();
        this.dtrange.startDate = start;
        this.dtrange.endDate = end;
        this.dtrange.updateView();
        this.activeQuickRange = range.label;
    }

    onQuickRangeLeave() {
        if (this.savedStartDate) {
            this.dtrange.startDate = this.savedStartDate;
            this.dtrange.endDate = this.savedEndDate;
            this.dtrange.updateView();
            this.savedStartDate = null;
            this.savedEndDate = null;
        }
        this.activeQuickRange = null;
    }

    onQuickRangeClick(range: any) {
        const [start, end] = range.getRange();
        this.dtrange.startDate = start;
        this.dtrange.endDate = end;
        this.dtrange.updateView();
        this.savedStartDate = null;
        this.savedEndDate = null;
        this.activeQuickRange = range.label;
    }

    getStartDateStr(): string {
        return this.dtrange?.startDate ? this.dtrange.startDate.format('YYYY-MM-DD') : '';
    }

    getStartTimeStr(): string {
        return this.dtrange?.startDate ? this.dtrange.startDate.format('HH:mm:ss') : '00:00:00';
    }

    getEndDateStr(): string {
        return this.dtrange?.endDate ? this.dtrange.endDate.format('YYYY-MM-DD') : '';
    }

    getEndTimeStr(): string {
        return this.dtrange?.endDate ? this.dtrange.endDate.format('HH:mm:ss') : '00:00:00';
    }

    onStartTimeChange(event: any) {
        const val = event.target.value;
        if (val && this.dtrange?.startDate) {
            const parts = val.split(':');
            this.dtrange.startDate.hour(+parts[0]);
            this.dtrange.startDate.minute(+parts[1]);
            this.dtrange.startDate.second(parts[2] ? +parts[2] : 0);
            this.dtrange.updateView();
        }
    }

    onEndTimeChange(event: any) {
        const val = event.target.value;
        if (val && this.dtrange?.endDate) {
            const parts = val.split(':');
            this.dtrange.endDate.hour(+parts[0]);
            this.dtrange.endDate.minute(+parts[1]);
            this.dtrange.endDate.second(parts[2] ? +parts[2] : 0);
            this.dtrange.updateView();
        }
    }

    onOkClick() {
        let dateRange = <IDateRange> { start: this.dtrange.startDate.toDate().getTime(),
            end: this.dtrange.endDate.toDate().getTime() };
        this.dialogRef.close(dateRange);
    }

    onNoClick() {
        this.dialogRef.close();
    }
}
