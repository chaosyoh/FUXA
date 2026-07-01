import { Component, Inject, OnInit } from '@angular/core';
import { Utils } from '../../../_helpers/utils';
import { MatDialog as MatDialog, MatDialogRef as MatDialogRef, MAT_DIALOG_DATA as MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ChartLine, ChartLineZone } from '../../../_models/chart';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { DeviceTagSelectionComponent, DeviceTagSelectionData } from '../../../device/device-tag-selection/device-tag-selection.component';
import { DevicesUtils, Device } from '../../../_models/device';
import { ProjectService } from '../../../_services/project.service';

@Component({
    selector: 'app-chart-line-property',
    templateUrl: './chart-line-property.component.html',
    styleUrls: ['./chart-line-property.component.scss']
})
export class ChartLinePropertyComponent implements OnInit {
    defaultColor = Utils.defaultColor;
    chartAxesType = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    formGroup: UntypedFormGroup;

    constructor(
        private fb: UntypedFormBuilder,
        private dialog: MatDialog,
        private projectService: ProjectService,
        public dialogRef: MatDialogRef<ChartLinePropertyComponent>,
        @Inject(MAT_DIALOG_DATA) public data: ChartLineAndInterpolationsType) {
    }

    ngOnInit() {
        this.formGroup = this.fb.group({
            name: [{value: this.data.name, disabled: true }, Validators.required],
            label: [this.data.label, Validators.required],
            yaxis: [this.data.yaxis],
            yaxisLabel: [this.data.yaxisLabel],
            yaxisShow: [this.data.yaxisShow !== false],
            lineInterpolation: [this.data.lineInterpolation],
            lineWidth: [this.data.lineWidth],
            spanGaps: [this.data.spanGaps]
        });
    }

    onNoClick(): void {
        this.dialogRef.close();
    }

    onOkClick(): void {
        this.dialogRef.close({...this.data, ...this.formGroup.getRawValue()});
    }

    onAddZone() {
        if (!this.data.zones) {
            this.data.zones = [];
        }
        this.data.zones.push(<ChartLineZone> {
            min: 0,
            max: 0,
            stroke: '#FF2525',
            fill: '#BFCDDB'
        });
    }

    onRemoveZone(index: number) {
        this.data.zones.splice(index, 1);
    }

    onChangeVariable() {
        let dialogRef = this.dialog.open(DeviceTagSelectionComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <DeviceTagSelectionData>{
                variableId: this.data.id,
                multiSelection: false
            }
        });
        dialogRef.afterClosed().subscribe((result: DeviceTagSelectionData) => {
            if (result && result.variableId) {
                let devices = Object.values(this.projectService.getDevices()) as Device[];
                let device = DevicesUtils.getDeviceFromTagId(devices, result.variableId);
                let tag = DevicesUtils.getTagFromTagId(devices, result.variableId);
                if (tag && device) {
                    this.data.id = tag.id;
                    this.data.name = tag.label || tag.name;
                    this.data.device = device.name;
                    this.formGroup.get('name').setValue(this.data.name);
                }
            }
        });
    }
}

export type ChartLineAndInterpolationsType = ChartLine & {
    lineInterpolationType: { text: string; value: any }[];
};
