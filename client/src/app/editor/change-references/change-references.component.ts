import { Component, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Device, Tag } from '../../_models/device';

export interface ChangeRefsData {
    devices: Device[];
    references: ReferenceItem[];
}

export interface ReferenceItem {
    gaugeId: string;
    gaugeName: string;
    variableId: string;
    tagName: string;
    deviceName: string;
    path: string;
    pathDisplay: string;
}

interface DisplayRow {
    isGroup: boolean;
    gaugeId?: string;
    gaugeName?: string;
    ref?: ReferenceItem;
    matched?: boolean;
    replaced?: boolean;
    newTagName?: string;
    error?: string;
}

@Component({
    selector: 'app-change-references',
    templateUrl: './change-references.component.html',
    styleUrls: ['./change-references.component.scss']
})
export class ChangeReferencesComponent {

    searchText = '';
    replaceText = '';
    matchWholeWord = false;
    useWildcards = false;
    replaceMode: 'tagName' | 'deviceName' = 'tagName';

    displayRows: DisplayRow[] = [];
    currentMatchIndex = -1;
    matchedIndices: number[] = [];

    private replacements: Map<string, string> = new Map();

    errorMessage = '';

    private refDeviceNames: string[] = [];
    private allDeviceNames: string[] = [];

    constructor(
        public dialogRef: MatDialogRef<ChangeReferencesComponent>,
        @Inject(MAT_DIALOG_DATA) public data: ChangeRefsData
    ) {
        this.buildDisplayRows();
        this.buildDeviceNameLists();
    }

    private buildDeviceNameLists() {
        const refNames = new Set<string>();
        for (const ref of this.data.references) {
            if (ref.deviceName) refNames.add(ref.deviceName);
        }
        this.refDeviceNames = [...refNames];
        this.allDeviceNames = this.data.devices.map(d => d.name).filter(n => !!n);
    }

    get filteredRefDeviceNames(): string[] {
        if (!this.searchText) return this.refDeviceNames;
        return this.refDeviceNames.filter(n => n.indexOf(this.searchText) >= 0);
    }

    get filteredAllDeviceNames(): string[] {
        if (!this.replaceText) return this.allDeviceNames;
        return this.allDeviceNames.filter(n => n.indexOf(this.replaceText) >= 0);
    }

    private buildDisplayRows() {
        this.displayRows = [];
        const grouped = new Map<string, ReferenceItem[]>();
        for (const ref of this.data.references) {
            if (!grouped.has(ref.gaugeId)) {
                grouped.set(ref.gaugeId, []);
            }
            grouped.get(ref.gaugeId).push(ref);
        }
        grouped.forEach((refs, gaugeId) => {
            this.displayRows.push({ isGroup: true, gaugeId, gaugeName: refs[0].gaugeName });
            for (const ref of refs) {
                this.displayRows.push({ isGroup: false, ref, matched: false, replaced: false });
            }
        });
    }

    onSearch() {
        this.matchedIndices = [];
        this.currentMatchIndex = -1;
        if (!this.searchText) {
            this.displayRows.forEach(row => { if (!row.isGroup) row.matched = false; });
            return;
        }
        const isDeviceMode = this.replaceMode === 'deviceName';
        for (let i = 0; i < this.displayRows.length; i++) {
            const row = this.displayRows[i];
            if (row.isGroup) continue;
            if (isDeviceMode) {
                row.matched = this.matchText(row.ref.deviceName, this.searchText, true);
            } else {
                row.matched = this.matchText(row.ref.tagName, this.searchText, false);
            }
            if (row.matched) {
                this.matchedIndices.push(i);
            }
        }
    }

    findNext() {
        this.errorMessage = '';
        if (!this.matchedIndices.length) {
            this.onSearch();
        }
        if (!this.matchedIndices.length) return;
        this.currentMatchIndex = (this.currentMatchIndex + 1) % this.matchedIndices.length;
    }

    onReplaceCurrent() {
        this.errorMessage = '';
        const error = this.replaceCurrent();
        if (error) {
            this.errorMessage = error;
        }
    }

    onReplaceAll() {
        this.errorMessage = '';
        const error = this.replaceAll();
        if (error) {
            this.errorMessage = error;
        }
    }

    replaceCurrent(): string | null {
        if (this.currentMatchIndex < 0 || !this.matchedIndices.length) return null;
        const rowIdx = this.matchedIndices[this.currentMatchIndex];
        const row = this.displayRows[rowIdx];
        if (!row.ref || row.replaced) {
            this.findNext();
            return null;
        }
        const error = this.resolveReplacement(row);
        if (error) return error;
        this.matchedIndices.splice(this.currentMatchIndex, 1);
        if (this.matchedIndices.length && this.currentMatchIndex >= this.matchedIndices.length) {
            this.currentMatchIndex = 0;
        }
        return null;
    }

    replaceAll(): string | null {
        this.onSearch();
        if (!this.matchedIndices.length) return null;

        // First pass: resolve all to check for errors
        const pendingReplacements: { row: DisplayRow, newId: string, newName: string }[] = [];
        for (const idx of this.matchedIndices) {
            const row = this.displayRows[idx];
            if (row.replaced) continue;
            const result = this.tryResolveReplacement(row);
            if (result.error) {
                return result.error;
            }
            pendingReplacements.push({ row, newId: result.newId, newName: result.newName });
        }

        // All resolved successfully, apply
        for (const pending of pendingReplacements) {
            pending.row.replaced = true;
            pending.row.newTagName = pending.newName;
            pending.row.matched = false;
            this.replacements.set(pending.row.ref.variableId, pending.newId);
        }
        this.matchedIndices = [];
        this.currentMatchIndex = -1;
        return null;
    }

    private resolveReplacement(row: DisplayRow): string | null {
        const result = this.tryResolveReplacement(row);
        if (result.error) {
            row.error = result.error;
            return result.error;
        }
        row.replaced = true;
        row.newTagName = result.newName;
        row.matched = false;
        row.error = null;
        this.replacements.set(row.ref.variableId, result.newId);
        return null;
    }

    private tryResolveReplacement(row: DisplayRow): { newId?: string, newName?: string, error?: string } {
        const ref = row.ref;
        const isDeviceMode = this.replaceMode === 'deviceName';

        if (isDeviceMode) {
            // Device replacement: find same tag name in new device
            const newDeviceName = this.replaceText;
            const newDevice = this.data.devices.find(d => d.name === newDeviceName);
            if (!newDevice) {
                return { error: `${newDeviceName}` };
            }
            const newTag = Object.values(newDevice.tags || {}).find((t: Tag) => t.name === ref.tagName) as Tag;
            if (!newTag) {
                return { error: `${newDeviceName} - ${ref.tagName}` };
            }
            return { newId: newTag.id, newName: `${newDeviceName} - ${newTag.name}` };
        } else {
            // Tag name replacement: partial text replace in tag name
            const newTagName = this.computeNewTagName(ref.tagName);
            // Find tag with new name in same device
            const device = this.data.devices.find(d => d.name === ref.deviceName);
            if (!device) {
                return { error: `${ref.deviceName} - ${newTagName}` };
            }
            const newTag = Object.values(device.tags || {}).find((t: Tag) => t.name === newTagName) as Tag;
            if (!newTag) {
                return { error: `${ref.deviceName} - ${newTagName}` };
            }
            return { newId: newTag.id, newName: newTag.name };
        }
    }

    private computeNewTagName(tagName: string): string {
        if (this.useWildcards) {
            const regex = this.buildRegex(this.searchText);
            return tagName.replace(regex, this.replaceText);
        }
        if (this.matchWholeWord) {
            if (tagName === this.searchText) return this.replaceText;
            return tagName;
        }
        return tagName.split(this.searchText).join(this.replaceText);
    }

    private matchText(text: string, search: string, exactDeviceMatch: boolean): boolean {
        if (!text || !search) return false;
        if (exactDeviceMatch) {
            return text === search;
        }
        if (this.useWildcards) {
            const regex = this.buildRegex(search);
            return regex.test(text);
        }
        if (this.matchWholeWord) {
            return text === search;
        }
        return text.indexOf(search) >= 0;
    }

    private buildRegex(pattern: string): RegExp {
        const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
        const regexStr = escaped.replace(/\*/g, '.*').replace(/\?/g, '.');
        if (this.matchWholeWord) {
            return new RegExp(`^${regexStr}$`);
        }
        return new RegExp(regexStr);
    }

    isCurrentMatch(index: number): boolean {
        if (this.currentMatchIndex < 0) return false;
        return this.matchedIndices[this.currentMatchIndex] === index;
    }

    onOkClick() {
        if (this.replacements.size === 0) {
            this.dialogRef.close(null);
            return;
        }
        const result: { srcId: string, destId: string }[] = [];
        this.replacements.forEach((destId, srcId) => {
            result.push({ srcId, destId });
        });
        this.dialogRef.close(result);
    }

    onCancelClick() {
        this.dialogRef.close(null);
    }
}
