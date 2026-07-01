import { Component, EventEmitter, OnInit, Inject, Output, ViewChild } from '@angular/core';
import { AbstractControl, UntypedFormBuilder, UntypedFormGroup, ValidationErrors, ValidatorFn, Validators } from '@angular/forms';
import { Device, ServerTagType, Tag } from '../../../_models/device';
import { TranslateService } from '@ngx-translate/core';
import { MAT_DIALOG_DATA as MAT_DIALOG_DATA, MatDialog, MatDialogRef as MatDialogRef } from '@angular/material/dialog';
import { TagPropertyOptionsComponent } from '../tag-property-options/tag-property-options.component';
import { ProjectService } from '../../../_services/project.service';
import { DeviceTagSelectionComponent, DeviceTagSelectionData } from '../../device-tag-selection/device-tag-selection.component';

@Component({
    selector: 'app-tag-property-edit-server',
    templateUrl: './tag-property-edit-server.component.html',
    styleUrls: ['./tag-property-edit-server.component.scss']
})
export class TagPropertyEditServerComponent implements OnInit {
    @Output() result = new EventEmitter<any>();
    @ViewChild('tagOptions', { static: true }) tagOptions: TagPropertyOptionsComponent;
    formGroup: UntypedFormGroup;
    tagType = ServerTagType;
    existingNames = [];
    error: string;
    isCalculated = false;

    constructor(private fb: UntypedFormBuilder,
        private translateService: TranslateService,
        private projectService: ProjectService,
        private dialog: MatDialog,
        public dialogRef: MatDialogRef<TagPropertyEditServerComponent>,
        @Inject(MAT_DIALOG_DATA) public data: TagProperty) { }

    ngOnInit() {
        this.formGroup = this.fb.group({
            deviceName: [this.data.device.name, Validators.required],
            tagName: [this.data.tag.name, [Validators.required, this.validateName()]],
            tagType: [this.data.tag.type, Validators.required],
            tagInit: [this.data.tag.init],
            tagDescription: [this.data.tag.description],
            tagAccess: [this.data.tag.access || 'rw'],
            tagExpression: [this.data.tag.expression || ''],
            tagBadQualityMode: [(this.data.tag.options && this.data.tag.options.badQualityMode) || 0]
        });
        this.formGroup.updateValueAndValidity();
        Object.keys(this.data.device.tags).forEach((key) => {
            let tag = this.data.device.tags[key];
            if (tag.id) {
                if (tag.id !== this.data.tag.id) {
                    this.existingNames.push(tag.name);
                }
            } else if (tag.name !== this.data.tag.name) {
                this.existingNames.push(tag.name);
            }
        });

        // Listen for type changes to toggle calculated mode
        this.formGroup.controls.tagType.valueChanges.subscribe((type: string) => {
            this.onTagTypeChange(type);
        });

        // Initialize state based on current type
        this.onTagTypeChange(this.data.tag.type);
    }

    onTagTypeChange(type: string) {
        this.isCalculated = (type === 'calculated');
        if (this.isCalculated) {
            this.formGroup.controls.tagAccess.setValue('ro');
            this.formGroup.controls.tagAccess.disable();
            this.formGroup.controls.tagInit.setValue('');
            this.formGroup.controls.tagInit.disable();
        } else {
            this.formGroup.controls.tagAccess.enable();
            this.formGroup.controls.tagInit.enable();
        }
    }

    /**
     * Open device tag selection dialog, then insert the selected tag reference
     * as ${DeviceName}.TagName into the expression textarea.
     */
    onBrowseTag() {
        const dialogRef = this.dialog.open(DeviceTagSelectionComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <DeviceTagSelectionData>{
                variableId: null,
                multiSelection: false,
                deviceFilter: [] // show all devices
            }
        });

        dialogRef.afterClosed().subscribe((result: DeviceTagSelectionData) => {
            if (result && result.variableId && result.deviceName) {
                // Resolve tag name from variableId
                const devices = this.projectService.getDevices();
                const device: any = Object.values(devices).find((d: any) => d.name === result.deviceName);
                if (device && device.tags) {
                    const tag: any = Object.values(device.tags).find((t: any) => t.id === result.variableId);
                    if (tag) {
                        const ref = `{${result.deviceName}.${tag.name}}`;
                        this.insertTagRef(ref);
                    }
                }
            }
        });
    }

    insertTagRef(ref: string) {
        const textarea = document.getElementById('expressionTextarea') as HTMLTextAreaElement;
        if (!textarea) {
            const current = this.formGroup.controls.tagExpression.value || '';
            this.formGroup.controls.tagExpression.setValue(current + ref);
            return;
        }
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const current = this.formGroup.controls.tagExpression.value || '';
        const newValue = current.substring(0, start) + ref + current.substring(end);
        this.formGroup.controls.tagExpression.setValue(newValue);
        setTimeout(() => {
            textarea.focus();
            textarea.selectionStart = textarea.selectionEnd = start + ref.length;
        }, 0);
    }

    validateName(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            this.error = null;
            const name = control?.value;
            if (this.existingNames.indexOf(name) !== -1) {
              return { name: this.translateService.instant('msg.device-tag-exist') };
            }
            if (name?.includes('@')) {
              return { name: this.translateService.instant('msg.device-tag-invalid-char') };
            }
            return null;
        };
    }

    onNoClick(): void {
        this.result.emit();
    }

    onOkClick(): void {
        const raw = this.formGroup.getRawValue();
        this.result.emit({ ...raw, ...this.tagOptions.getValues() });
    }
}

interface TagProperty {
    device: Device;
    tag: Tag;
}
