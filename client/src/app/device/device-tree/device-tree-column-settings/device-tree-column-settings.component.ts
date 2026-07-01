import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { TranslateService } from '@ngx-translate/core';

export interface ColumnSetting {
    key: string;
    visible: boolean;
    width: number | null;
}

export interface ColumnSettingsDialogData {
    allColumns: string[];
    currentSettings: ColumnSetting[];
}

const DEFAULT_WIDTHS: Record<string, number> = {
    name: 200, address: 200, device: 140,
    type: 120, access: 140, min: 100, max: 100, value: 180,
    format: 120, daq: 120, timestamp: 160, quality: 100,
    description: 140, direction: 120
};

const COLUMN_I18N: Record<string, string> = {
    name: 'device.list-name',
    address: 'device.list-address',
    device: 'device.list-id',
    type: 'device.list-type',
    access: 'device.list-access',
    min: 'device.list-min',
    max: 'device.list-max',
    value: 'device.list-value',
    format: 'device.list-format',
    daq: 'device.list-daq-enable',
    timestamp: 'device.list-timestamp',
    quality: 'device.list-quality',
    description: 'device.list-description',
    direction: 'device.list-direction'
};

@Component({
    selector: 'app-device-tree-column-settings',
    templateUrl: './device-tree-column-settings.component.html',
    styleUrls: ['./device-tree-column-settings.component.scss']
})
export class DeviceTreeColumnSettingsComponent implements OnInit {

    settings: { key: string; label: string; visible: boolean; width: number | null }[] = [];

    constructor(
        private dialogRef: MatDialogRef<DeviceTreeColumnSettingsComponent>,
        @Inject(MAT_DIALOG_DATA) private data: ColumnSettingsDialogData,
        private translateService: TranslateService
    ) {}

    ngOnInit() {
        const settingsMap = new Map<string, ColumnSetting>();
        if (this.data.currentSettings) {
            this.data.currentSettings.forEach(s => settingsMap.set(s.key, s));
        }
        this.settings = this.data.allColumns.map(key => {
            const saved = settingsMap.get(key);
            return {
                key,
                label: this.getColumnLabel(key),
                visible: saved ? saved.visible : true,
                width: saved?.width || null
            };
        });
    }

    /** Get display label for a column key using i18n */
    private getColumnLabel(key: string): string {
        const i18nKey = COLUMN_I18N[key];
        if (!i18nKey) return key;
        if (i18nKey === '#') return '#';
        try {
            const translated = this.translateService.instant(i18nKey);
            return translated !== i18nKey ? translated : key;
        } catch {
            return key;
        }
    }

    /** Get effective display width (user-set or default) */
    getDisplayWidth(s: { key: string; width: number | null }): number {
        return s.width || DEFAULT_WIDTHS[s.key] || 100;
    }

    /** Move a column up in the list */
    moveUp(index: number) {
        if (index <= 0) return;
        const temp = this.settings[index];
        this.settings[index] = this.settings[index - 1];
        this.settings[index - 1] = temp;
    }

    /** Move a column down in the list */
    moveDown(index: number) {
        if (index >= this.settings.length - 1) return;
        const temp = this.settings[index];
        this.settings[index] = this.settings[index + 1];
        this.settings[index + 1] = temp;
    }

    /** Handle width input change */
    onWidthChange(s: { width: number | null }, value: string) {
        const num = parseInt(value);
        s.width = (!isNaN(num) && num >= 30) ? num : null;
    }

    /** Reset all settings to defaults */
    resetToDefaults() {
        this.settings.forEach(s => {
            s.visible = true;
            s.width = null;
        });
    }

    /** Close dialog without saving */
    onCancel() {
        this.dialogRef.close(null);
    }

    /** Close dialog and return current settings */
    onApply() {
        const result: ColumnSetting[] = this.settings.map(s => ({
            key: s.key,
            visible: s.visible,
            width: s.width
        }));
        this.dialogRef.close(result);
    }
}
