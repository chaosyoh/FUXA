import { Component, EventEmitter, Inject, OnInit, Output, ViewChild } from '@angular/core';
import { Device, Tag, TagType } from '../../../_models/device';
import { MAT_DIALOG_DATA as MAT_DIALOG_DATA, MatDialogRef as MatDialogRef } from '@angular/material/dialog';
import { AbstractControl, UntypedFormBuilder, UntypedFormGroup, ValidationErrors, ValidatorFn, Validators } from '@angular/forms';
import { TranslateService } from '@ngx-translate/core';
import { TagPropertyOptionsComponent } from '../tag-property-options/tag-property-options.component';

@Component({
    selector: 'app-tag-property-edit-s7',
    templateUrl: './tag-property-edit-s7.component.html',
    styleUrls: ['./tag-property-edit-s7.component.scss']
})
export class TagPropertyEditS7Component implements OnInit {

    @Output() result = new EventEmitter<any>();
    @ViewChild('tagOptions', { static: true }) tagOptions: TagPropertyOptionsComponent;
    formGroup: UntypedFormGroup;
    tagType = TagType;
    existingNames = [];
    error: string;

    constructor(private fb: UntypedFormBuilder,
        private translateService: TranslateService,
        public dialogRef: MatDialogRef<TagPropertyEditS7Component>,
        @Inject(MAT_DIALOG_DATA) public data: TagProperty) { }

    ngOnInit() {
        this.formGroup = this.fb.group({
            deviceName: [this.data.device.name, Validators.required],
            tagName: [this.data.tag.name, [Validators.required, this.validateName()]],
            tagType: [this.data.tag.type, Validators.required],
            tagAddress: [this.data.tag.address, Validators.required],
            tagDescription: [this.data.tag.description],
            tagAccess: [this.data.tag.access || 'rw']
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
        this.result.emit({ ...this.formGroup.getRawValue(), ...this.tagOptions.getValues() });
    }

}

interface TagProperty {
    device: Device;
    tag: Tag;
}
