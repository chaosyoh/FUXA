import { Component, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
    selector: 'app-tag-write-value',
    templateUrl: './tag-write-value.component.html',
    styleUrls: ['./tag-write-value.component.scss']
})
export class TagWriteValueComponent {
    tagName: string;
    tagType: string;
    writeValue: any = null;
    isReadOnly = false;

    /** Determine input mode based on tag type */
    get inputMode(): 'bool' | 'int' | 'float' | 'string' {
        if (!this.tagType) return 'string';
        const t = this.tagType.toLowerCase();
        if (t === 'bool' || t === 'boolean') return 'bool';
        if (t === 'byte' || t === 'word' || t === 'dword' || t === 'int16' || t === 'uint16' ||
            t === 'int32' || t === 'uint32' || t === 'int64' || t === 'uint64' ||
            t === 'short' || t === 'integer') return 'int';
        if (t === 'float' || t === 'double' || t === 'real' || t === 'lreal') return 'float';
        return 'string';
    }

    constructor(
        public dialogRef: MatDialogRef<TagWriteValueComponent>,
        @Inject(MAT_DIALOG_DATA) public data: TagWriteValueData
    ) {
        this.tagName = data.tagName || '';
        this.tagType = data.tagType || '';
        this.isReadOnly = data.access === 'ro';
        // Set default value
        if (this.inputMode === 'bool') {
            this.writeValue = 'false';
        }
    }

    onCancel(): void {
        this.dialogRef.close();
    }

    onConfirm(): void {
        if (this.isReadOnly) return;
        let value: any = this.writeValue;
        if (this.inputMode === 'bool') {
            value = this.writeValue === 'true' ? 1 : 0;
        } else if (this.inputMode === 'int') {
            value = parseInt(value, 10);
            if (isNaN(value)) value = 0;
        } else if (this.inputMode === 'float') {
            value = parseFloat(value);
            if (isNaN(value)) value = 0;
        }
        this.dialogRef.close(value);
    }
}

export interface TagWriteValueData {
    tagName: string;
    tagType: string;
    access: string;
}
