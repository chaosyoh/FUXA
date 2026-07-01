import { Component, EventEmitter, Inject, OnInit, Output, ViewChild } from '@angular/core';
import { Device, RedisTagType, Tag } from '../../../_models/device';
import { AbstractControl, UntypedFormBuilder, UntypedFormGroup, ValidationErrors, ValidatorFn, Validators } from '@angular/forms';
import { TranslateService } from '@ngx-translate/core';
import { MatDialogRef as MatDialogRef, MAT_DIALOG_DATA as MAT_DIALOG_DATA } from '@angular/material/dialog';
import { TagPropertyOptionsComponent } from '../tag-property-options/tag-property-options.component';

@Component({
    selector: 'app-tag-property-edit-redis',
    templateUrl: './tag-property-edit-redis.component.html',
    styleUrls: ['./tag-property-edit-redis.component.scss']
})
export class TagPropertyEditRedisComponent implements OnInit {
    @Output() result = new EventEmitter<any>();
    @ViewChild('tagOptions', { static: true }) tagOptions: TagPropertyOptionsComponent;
    formGroup: UntypedFormGroup;
    tagType = RedisTagType;
    existingNames = [];
    error: string;

    constructor(private fb: UntypedFormBuilder,
        private translateService: TranslateService,
        public dialogRef: MatDialogRef<TagPropertyEditRedisComponent>,
        @Inject(MAT_DIALOG_DATA) public data: TagPropertyRedisData) { }

    ngOnInit() {
        this.formGroup = this.fb.group({
            deviceName: [this.data.device.name, Validators.required],
            tagName: [this.data.tag.name, [Validators.required, this.validateName()]],
            tagType: [this.data.tag.type, Validators.required],
            tagAddress: [this.data.tag.address, [Validators.required]],
            tagDescription: [this.data.tag.description],
            tagAccess: [this.data.tag.access || 'rw'],
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

export interface TagPropertyRedisData {
    device: Device;
    tag: Tag;
}
