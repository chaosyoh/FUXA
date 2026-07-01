import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { Subscription } from 'rxjs';
import { Tag, TagDaq, TagDeadband, TagDeadbandModeType, TagScale, TagScaleModeType } from '../../../_models/device';
import { Script, ScriptMode, ScriptParam, ScriptParamType } from '../../../_models/script';
import { ProjectService } from '../../../_services/project.service';

class ScriptAndParam extends ScriptParam {
    scriptId: string;
}

@Component({
    selector: 'app-tag-property-options',
    templateUrl: './tag-property-options.component.html',
    styleUrls: ['./tag-property-options.component.scss']
})
export class TagPropertyOptionsComponent implements OnInit, OnDestroy {

    @Input() tag: Tag;
    @Input() isFuxaServer = false;

    formGroup: UntypedFormGroup;
    scaleModeType = TagScaleModeType;
    scripts: Script[];
    configedReadParams: { [key: string]: ScriptAndParam[] } = {};
    configedWriteParams: { [key: string]: ScriptParam[] } = {};

    private subscriptionLoad: Subscription;

    constructor(
        private fb: UntypedFormBuilder,
        private projectService: ProjectService
    ) {}

    ngOnInit() {
        this.loadScripts();
        this.subscriptionLoad = this.projectService.onLoadHmi.subscribe(() => {
            this.loadScripts();
        });

        this.formGroup = this.fb.group({
            interval: [{ value: 60, disabled: true }, [Validators.required, Validators.min(0)]],
            changed: [{ value: false, disabled: true }],
            enabled: [false],
            restored: [false],
            format: [null, [Validators.min(0)]],
            deadband: null,
            scaleMode: 'undefined',
            rawLow: null,
            rawHigh: null,
            scaledLow: null,
            scaledHigh: null,
            dateTimeFormat: null,
            scaleReadFunction: null,
            scaleWriteFunction: null,
            scaleReadExpression: null,
            scaleWriteExpression: null,
        });

        this.formGroup.controls.enabled.valueChanges.subscribe(enabled => {
            if (enabled) {
                this.formGroup.controls.interval.enable();
                this.formGroup.controls.changed.enable();
            } else {
                this.formGroup.controls.interval.disable();
                this.formGroup.controls.changed.disable();
            }
        });

        if (this.tag) {
            const values: any = {};
            if (this.tag.daq) {
                values.enabled = this.tag.daq.enabled;
                values.changed = this.tag.daq.changed;
                values.interval = this.tag.daq.interval;
                values.restored = this.tag.daq.restored;
            }
            if (this.tag.format != null) {
                values.format = this.tag.format;
            }
            if (this.tag.deadband) {
                values.deadband = this.tag.deadband.value;
            }
            if (this.tag.scale) {
                values.scaleMode = this.tag.scale.mode || 'undefined';
                values.rawLow = this.tag.scale.rawLow;
                values.rawHigh = this.tag.scale.rawHigh;
                values.scaledLow = this.tag.scale.scaledLow;
                values.scaledHigh = this.tag.scale.scaledHigh;
                values.dateTimeFormat = this.tag.scale.dateTimeFormat;
                values.scaleReadExpression = this.tag.scale.readExpression;
                values.scaleWriteExpression = this.tag.scale.writeExpression;
            }
            if (this.tag.scaleReadFunction) {
                values.scaleReadFunction = this.tag.scaleReadFunction;
                const script = this.scripts?.find(s => s.id === this.tag.scaleReadFunction);
                if (this.tag.scaleReadParams) {
                    const tagParams = JSON.parse(this.tag.scaleReadParams) as ScriptParam[];
                    this.initializeScriptParams(script, tagParams, this.configedReadParams);
                }
            }
            if (this.tag.scaleWriteFunction) {
                values.scaleWriteFunction = this.tag.scaleWriteFunction;
                const script = this.scripts?.find(s => s.id === this.tag.scaleWriteFunction);
                if (this.tag.scaleWriteParams) {
                    const tagParams = JSON.parse(this.tag.scaleWriteParams) as ScriptParam[];
                    this.initializeScriptParams(script, tagParams, this.configedWriteParams);
                }
            }
            this.formGroup.patchValue(values);
            if (this.isFuxaServer) {
                this.formGroup.controls.scaleMode.disable();
            }
            this.formGroup.updateValueAndValidity();
            this.onCheckScaleMode(this.formGroup.value.scaleMode);
        }

        this.formGroup.controls.scaleMode.valueChanges.subscribe(value => {
            this.onCheckScaleMode(value);
        });
    }

    ngOnDestroy() {
        if (this.subscriptionLoad) {
            this.subscriptionLoad.unsubscribe();
        }
    }

    onCheckScaleMode(value: string) {
        switch (value) {
            case 'linear':
                this.formGroup.controls.rawLow.setValidators(Validators.required);
                this.formGroup.controls.rawHigh.setValidators(Validators.required);
                this.formGroup.controls.scaledLow.setValidators(Validators.required);
                this.formGroup.controls.scaledHigh.setValidators(Validators.required);
                break;
            default:
                this.formGroup.controls.rawLow.clearValidators();
                this.formGroup.controls.rawHigh.clearValidators();
                this.formGroup.controls.scaledLow.clearValidators();
                this.formGroup.controls.scaledHigh.clearValidators();
                break;
        }
        this.formGroup.controls.rawLow.updateValueAndValidity();
        this.formGroup.controls.rawHigh.updateValueAndValidity();
        this.formGroup.controls.scaledLow.updateValueAndValidity();
        this.formGroup.controls.scaledHigh.updateValueAndValidity();
        this.formGroup.updateValueAndValidity();
    }

    isValid(): boolean {
        return this.formGroup.valid && !this.paramsInValid();
    }

    getValues(): TagPropertyOptionsResult {
        let readParamsStr: string;
        if (this.configedReadParams[this.formGroup.value.scaleReadFunction]) {
            readParamsStr = JSON.stringify(this.configedReadParams[this.formGroup.value.scaleReadFunction]);
        }
        let writeParamsStr: string;
        if (this.configedWriteParams[this.formGroup.value.scaleWriteFunction]) {
            writeParamsStr = JSON.stringify(this.configedWriteParams[this.formGroup.value.scaleWriteFunction]);
        }
        return {
            daq: new TagDaq(
                this.formGroup.value.enabled,
                this.formGroup.value.changed,
                this.formGroup.value.interval,
                this.formGroup.value.restored,
            ),
            format: this.formGroup.value.format,
            deadband: this.formGroup.value.deadband
                ? { value: this.formGroup.value.deadband, mode: TagDeadbandModeType.absolute }
                : undefined,
            scale: (this.formGroup.value.scaleMode !== 'undefined') ? {
                mode: this.formGroup.value.scaleMode,
                rawLow: this.formGroup.value.rawLow,
                rawHigh: this.formGroup.value.rawHigh,
                scaledLow: this.formGroup.value.scaledLow,
                scaledHigh: this.formGroup.value.scaledHigh,
                dateTimeFormat: this.formGroup.value.dateTimeFormat,
                readExpression: this.formGroup.value.scaleReadExpression,
                writeExpression: this.formGroup.value.scaleWriteExpression
            } : null,
            scaleReadFunction: this.formGroup.value.scaleReadFunction,
            scaleReadParams: readParamsStr,
            scaleWriteFunction: this.formGroup.value.scaleWriteFunction,
            scaleWriteParams: writeParamsStr,
        };
    }

    private paramsInValid(): boolean {
        if (this.formGroup.value.scaleReadFunction &&
            (this.configedReadParams[this.formGroup.value.scaleReadFunction] ?? []).some(p => !p.value)) {
            return true;
        }
        if (this.formGroup.value.scaleWriteFunction &&
            (this.configedWriteParams[this.formGroup.value.scaleWriteFunction] ?? []).some(p => !p.value)) {
            return true;
        }
        return false;
    }

    private loadScripts() {
        const filteredScripts = this.projectService.getScripts().filter(script => {
            if (script.parameters.length > 0 && script.mode === ScriptMode.SERVER) {
                if (script.parameters[0].name !== 'value' || script.parameters[0].type !== ScriptParamType.value) {
                    return false;
                }
                for (const param of script.parameters) {
                    if (param.type !== ScriptParamType.value) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        });
        for (const script of filteredScripts) {
            const paramCopy = [];
            for (let i = 1; i < script.parameters.length; i++) {
                const pc = new ScriptAndParam(script.parameters[i].name, script.parameters[i].type);
                pc.scriptId = script.id;
                pc.value = ('value' in script.parameters[i]) ? script.parameters[i].value : null;
                paramCopy.push(pc);
            }
            this.configedReadParams[script.id] = paramCopy;
            this.configedWriteParams[script.id] = paramCopy;
        }
        this.scripts = filteredScripts;
    }

    private initializeScriptParams(script: Script, tagParams: ScriptParam[], toUpdate: { [key: string]: ScriptParam[] }) {
        if (script) {
            let parametersChanged = false;
            if (tagParams.length !== script.parameters.length - 1) {
                parametersChanged = true;
            } else {
                for (const [index, param] of script.parameters.entries()) {
                    if (index === 0) { continue; }
                    if (!(tagParams.some(p => p.name === param.name))) {
                        parametersChanged = true;
                        break;
                    }
                }
            }
            if (!parametersChanged) {
                toUpdate[script.id] = tagParams;
            }
        }
    }
}

export interface TagPropertyOptionsResult {
    daq: TagDaq;
    format: number;
    deadband: TagDeadband;
    scale: TagScale;
    scaleReadFunction?: string;
    scaleReadParams?: string;
    scaleWriteFunction?: string;
    scaleWriteParams?: string;
}
