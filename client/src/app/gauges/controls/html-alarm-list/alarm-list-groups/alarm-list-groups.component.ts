import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

import { ProjectService } from '../../../../_services/project.service';
import { Alarm } from '../../../../_models/alarm';

@Component({
    selector: 'app-alarm-list-groups',
    templateUrl: './alarm-list-groups.component.html',
    styleUrls: ['./alarm-list-groups.component.scss']
})
export class AlarmListGroupsComponent {

    allGroups: string[] = [];
    selectedGroups: string[] = [];

    constructor(
        private projectService: ProjectService,
        public dialogRef: MatDialogRef<AlarmListGroupsComponent>,
        @Inject(MAT_DIALOG_DATA) public data: { groups: string[] }) {

        this.selectedGroups = [...(data.groups || [])];
        this.allGroups = this.extractAllGroups();
    }

    private extractAllGroups(): string[] {
        const groups = new Set<string>();
        const alarms = this.projectService.getAlarms();
        if (alarms) {
            alarms.forEach((alarm: Alarm) => {
                if (alarm.highhigh?.group) { groups.add(alarm.highhigh.group); }
                if (alarm.high?.group) { groups.add(alarm.high.group); }
                if (alarm.low?.group) { groups.add(alarm.low.group); }
                if (alarm.info?.group) { groups.add(alarm.info.group); }
            });
        }
        return Array.from(groups).sort();
    }

    isGroupSelected(group: string): boolean {
        return this.selectedGroups.includes(group);
    }

    onGroupToggle(group: string, selected: boolean) {
        const index = this.selectedGroups.indexOf(group);
        if (selected && index === -1) {
            this.selectedGroups.push(group);
        } else if (!selected && index !== -1) {
            this.selectedGroups.splice(index, 1);
        }
    }

    onSelectAll() {
        this.selectedGroups = [...this.allGroups];
    }

    onClearAll() {
        this.selectedGroups = [];
    }

    onNoClick(): void {
        this.dialogRef.close();
    }

    onOkClick(): void {
        this.dialogRef.close(this.selectedGroups);
    }
}
