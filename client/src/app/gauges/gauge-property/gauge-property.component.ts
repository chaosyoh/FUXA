/* eslint-disable @angular-eslint/component-class-suffix */
import { Component, Inject, Input, AfterViewInit, ViewChild, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { MAT_DIALOG_DATA as MAT_DIALOG_DATA, MatDialog as MatDialog, MatDialogRef as MatDialogRef } from '@angular/material/dialog';
import { FlexHeadComponent } from './flex-head/flex-head.component';
import { FlexEventComponent } from './flex-event/flex-event.component';
import { FlexActionComponent } from './flex-action/flex-action.component';
import { GaugeProperty, GaugeSettings, View, WidgetProperty } from '../../_models/hmi';
import { Script } from '../../_models/script';
import { PropertyType } from './flex-input/flex-input.component';
import { PermissionData, PermissionDialogComponent } from './permission-dialog/permission-dialog.component';
import { SettingsService } from '../../_services/settings.service';
import { Device } from '../../_models/device';
import { HtmlButtonComponent } from '../controls/html-button/html-button.component';
import { HtmlButtonStyle, isSafeCssValue } from '../controls/html-button/html-button-presets';

@Component({
    selector: 'gauge-property',
    templateUrl: './gauge-property.component.html',
    styleUrls: ['./gauge-property.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class GaugePropertyComponent implements AfterViewInit {

    @Input() name: any;
    @ViewChild('flexhead', {static: false}) flexHead: FlexHeadComponent;
    @ViewChild('flexevent', {static: false}) flexEvent: FlexEventComponent;
    @ViewChild('flexaction', {static: false}) flexAction: FlexActionComponent;

    slideView = true;
    slideActionView = true;
    withBitmask = false;
    property: GaugeProperty | WidgetProperty;
    dialogType: GaugeDialogType = GaugeDialogType.RangeWithAlarm;
    eventsSupported: boolean;
    actionsSupported: any;
    views: View[];
    defaultValue: any;
    inputs: GaugeSettings[];
    scripts: Script[];

    // ===== HtmlButton 样式扩展（边框 / 圆角 / 内边距 / 自定义 CSS）=====
    // 预设主题已下线，不再注入 preset 字段
    customStyleEntries: { key: string; value: string }[] = [];

    constructor(public dialog: MatDialog,
                public dialogRef: MatDialogRef<GaugePropertyComponent>,
                private settingsService: SettingsService,
                private cdr: ChangeDetectorRef,
                @Inject(MAT_DIALOG_DATA) public data: GaugePropertyData | any) {
        this.dialogType = this.data.dlgType;
        this.eventsSupported = this.data.withEvents;
        this.actionsSupported = this.data.withActions;
        this.views = this.data.views;
        this.inputs = this.data.inputs;
        this.scripts = this.data.scripts;
        this.property = JSON.parse(JSON.stringify(this.data.settings.property));
        if (!this.property) {
            this.property = new GaugeProperty();
        }
        // 初始化 HtmlButton 样式结构（不含 preset）
        if (this.isHtmlButton()) {
            this.property.options = this.property.options || {};
            const style: HtmlButtonStyle = this.property.options.style || {};
            // 旧数据若存在 preset 字段则清除（功能已下线）
            if ((style as any).preset) { delete (style as any).preset; }
            style.border = style.border || { width: 0, style: 'none', color: '#000000' };
            style.borderRadius = style.borderRadius ?? '';
            style.padding = style.padding ?? '';
            // 基础外观默认值（背景色 / 文字色 由 tools_bottom_2 工具栏管理，不在此处暴露）
            style.fontSize = style.fontSize ?? '';
            style.fontWeight = style.fontWeight ?? '';
            style.boxShadow = style.boxShadow ?? '';
            style.customStyle = style.customStyle || {};
            this.property.options.style = style;
            this.customStyleEntries = Object.keys(style.customStyle).map(k => ({
                key: k,
                value: style.customStyle[k]
            }));
        }
    }

    ngAfterViewInit() {
        this.defaultValue = this.data.default;
        if (!this.isWidget() && this.data.withProperty !== false) { // else undefined
            if (this.dialogType === GaugeDialogType.Input) {
                this.flexHead.withProperty = PropertyType.input;
            } else if (this.dialogType === GaugeDialogType.ValueAndUnit) {
                this.flexHead.withProperty = PropertyType.output;
            } else {
                this.flexHead.defaultValue = this.defaultValue;
                this.flexHead.withProperty = PropertyType.range;
                if (this.dialogType === GaugeDialogType.ValueWithRef) {
                    this.flexHead.withProperty = PropertyType.text;
                } else if (this.dialogType === GaugeDialogType.Step) {
                    this.flexHead.withProperty = PropertyType.step;
                } else if (this.dialogType === GaugeDialogType.ImageList) {
                    this.flexHead.withProperty = PropertyType.imagestep;
                } else if (this.dialogType === GaugeDialogType.MinMax) {
                    this.flexHead.withProperty = PropertyType.minmax;
                }
            }
        }
        if (this.data.withBitmask) {
            this.withBitmask = this.data.withBitmask;
        }
        // Enable min/max variable binding UI only for the progress bar gauge.
        // Use the string literal of its TypeTag to avoid a circular import with gauge-progress.
        if (this.flexHead && this.data?.settings?.type === 'svg-ext-gauge_progress') {
            this.flexHead.withVariableBinding = true;
        }
    }

    onNoClick(): void {
        this.dialogRef.close();
    }

    onOkClick(): void {
        if (this.isWidget()) {
            this.data.settings.property = this.property;
        } else {
            this.data.settings.property = this.flexHead?.getProperty();
        }
        if (this.flexEvent) {
            this.data.settings.property.events = this.flexEvent.getEvents();
        }
        if (this.flexAction) {
            this.data.settings.property.actions = this.flexAction.getActions();
        }
        if (this.property.readonly) {
            this.property.readonly = true;
        } else {
            delete this.property.readonly;
        }
        // 保存 HtmlButton 样式 options.style 到最终 property
        if (this.isHtmlButton() && this.property.options?.style) {
            this.data.settings.property.options = this.data.settings.property.options || {};
            // 将 customStyle 键值对列表回填到对象
            const customStyle: { [k: string]: string } = {};
            this.customStyleEntries
                .filter(kv => kv.key && kv.value)
                .forEach(kv => { customStyle[kv.key.trim()] = kv.value.trim(); });
            this.property.options.style.customStyle = customStyle;
            this.data.settings.property.options.style = this.property.options.style;
        }
    }

    onAddInput() {
        this.flexHead.onAddInput();
    }

    onAddEvent() {
        this.flexEvent.onAddEvent();
    }

    onAddAction() {
        this.flexAction.onAddAction();
    }

    onRangeViewToggle() {
        this.flexHead.onRangeViewToggle(this.slideView);
    }

    onActionRangeViewToggle() {
        this.flexAction.onRangeViewToggle(this.slideActionView);
    }

    isToolboxToShow() {
        if (this.dialogType === GaugeDialogType.RangeWithAlarm || this.dialogType === GaugeDialogType.Range || this.dialogType === GaugeDialogType.Step ||
            this.dialogType === GaugeDialogType.RangeAndText || this.dialogType === GaugeDialogType.ImageList) {
            return this.data.withProperty !== false;
        }
        return false;
    }

    isRangeToShow() {
        if (this.dialogType === GaugeDialogType.RangeWithAlarm || this.dialogType === GaugeDialogType.Range || this.dialogType === GaugeDialogType.RangeAndText) {
            return true;
        }
        return false;
    }

    isTextToShow() {
        return this.data.languageTextEnabled || (this.dialogType === GaugeDialogType.RangeAndText);
    }

    isAlarmToShow() {
        if (this.dialogType === GaugeDialogType.RangeWithAlarm) {
            return true;
        }
        return false;
    }

    isReadonlyToShow() {
        if (this.dialogType === GaugeDialogType.Step) {
            return true;
        }
        return false;
    }
    onEditPermission() {
        let dialogRef = this.dialog.open(PermissionDialogComponent, {
            position: { top: '60px' },
            data: <PermissionData>{ permission: this.property.permission, permissionRoles: this.property.permissionRoles }
        });

        dialogRef.afterClosed().subscribe((result: PermissionData) => {
            if (result) {
                this.property.permission = result.permission;
                this.property.permissionRoles = result.permissionRoles;
            }
            this.cdr.detectChanges();
        });
    }

    isWidget() {
        return (this.property as WidgetProperty).type;
    }

    isRolePermission() {
        return this.settingsService.getSettings()?.userRole;
    }

    havePermission() {
        if (this.isRolePermission()) {
            return this.property.permissionRoles?.show?.length || this.property.permissionRoles?.enabled?.length;
        } else {
            return this.property.permission;
        }
    }

    // ===== HtmlButton 样式扩展辅助方法（不含预设主题）=====
    isHtmlButton(): boolean {
        return this.data?.settings?.type?.startsWith?.(HtmlButtonComponent.TypeTag) ?? false;
    }

    onAddCustomStyleEntry(): void {
        this.customStyleEntries.push({ key: '', value: '' });
        this.cdr.detectChanges();
    }

    onRemoveCustomStyleEntry(index: number): void {
        this.customStyleEntries.splice(index, 1);
        this.cdr.detectChanges();
    }

    /**
     * 从剪贴板/输入框粘贴 CSS 文本/对象到自定义 CSS 列表
     * 支持以下格式：
     *  1) JS 对象字面量： { backgroundColor: '#fff', 'border-radius': '4px' }
     *  2) CSS 声明列表：  background: #fff; color: red;
     *  3) 完整 CSS 规则：  .btn { background: #fff; color: red; }
     * 自动将 camelCase 转 kebab-case，遇到同名 key 进行覆盖。
     */
    async onPasteCustomStyle(): Promise<void> {
        let text = '';
        try {
            if (navigator?.clipboard?.readText) {
                text = await navigator.clipboard.readText();
            }
        } catch {
            // 浏览器拒绝读剪贴板时回退到 prompt
        }
        if (!text) {
            text = window.prompt('Paste CSS object / declarations:', '') || '';
        }
        if (!text.trim()) { return; }

        const parsed = this.parseCssLikeText(text);
        if (!parsed.length) { return; }

        const map = new Map<string, string>();
        // 先放入已有的，保留用户原始顺序
        this.customStyleEntries.forEach(kv => {
            const k = (kv.key || '').trim();
            if (k) { map.set(k, (kv.value || '').trim()); }
        });
        // 粘贴覆盖同名 key
        parsed.forEach(({ key, value }) => {
            if (isSafeCssValue(value)) {
                map.set(key, value);
            }
        });
        this.customStyleEntries = Array.from(map.entries()).map(([key, value]) => ({ key, value }));
        this.cdr.detectChanges();
    }

    /**
     * 解析多种格式的 CSS 文本，返回 [{key,value}] 列表
     * 注意：不支持嵌套规则、@-rules
     */
    private parseCssLikeText(raw: string): Array<{ key: string; value: string }> {
        let text = raw.trim();
        if (!text) { return []; }

        // 去除完整规则的选择器： .btn { ... }  或  { ... }
        const ruleMatch = text.match(/^[^{}]*\{([\s\S]*)\}[^{}]*$/);
        if (ruleMatch) {
            text = ruleMatch[1].trim();
        }

        // 用基于括号深度感知的拆分（同时支持 ; 和 ,，以兼容对象字面量）
        const segments = this.splitTopLevel(text, [';', ',']);
        const result: Array<{ key: string; value: string }> = [];
        for (const seg of segments) {
            const line = seg.trim();
            if (!line) { continue; }
            const colonIdx = line.indexOf(':');
            if (colonIdx <= 0) { continue; }
            let key = line.slice(0, colonIdx).trim();
            let value = line.slice(colonIdx + 1).trim();

            // 去掉 key 引号 'foo' / "foo"
            key = key.replace(/^['"]|['"]$/g, '').replace(/^['"]|['"]$/g, '');
            // 去掉 value 末尾的 , 或 ; 以及包裹引号
            value = value.replace(/[;,]\s*$/, '').trim();
            value = value.replace(/^['"]|['"]$/g, '').replace(/^['"]|['"]$/g, '');
            if (!key || !value) { continue; }
            // camelCase -> kebab-case
            key = key.replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase();
            result.push({ key, value });
        }
        return result;
    }

    /** 在不进入括号 / 引号 / 大括号 内部的情况下按分隔符拆分 */
    private splitTopLevel(text: string, separators: string[]): string[] {
        const out: string[] = [];
        let depth = 0;
        let inSingle = false;
        let inDouble = false;
        let buf = '';
        for (let i = 0; i < text.length; i++) {
            const ch = text[i];
            if (inSingle) {
                buf += ch;
                if (ch === '\'' && text[i - 1] !== '\\') { inSingle = false; }
                continue;
            }
            if (inDouble) {
                buf += ch;
                if (ch === '"' && text[i - 1] !== '\\') { inDouble = false; }
                continue;
            }
            if (ch === '\'') { inSingle = true; buf += ch; continue; }
            if (ch === '"') { inDouble = true; buf += ch; continue; }
            if (ch === '(' || ch === '{' || ch === '[') { depth++; buf += ch; continue; }
            if (ch === ')' || ch === '}' || ch === ']') { depth = Math.max(0, depth - 1); buf += ch; continue; }
            if (depth === 0 && separators.indexOf(ch) >= 0) {
                out.push(buf);
                buf = '';
                continue;
            }
            buf += ch;
        }
        if (buf.trim()) { out.push(buf); }
        return out;
    }
}

export enum GaugeDialogType {
    Range,
    RangeAndText,
    RangeWithAlarm,
    ValueAndUnit,
    ValueWithRef,
    Step,
    MinMax,
    Chart,
    Gauge,
    Pipe,
    Slider,
    Switch,
    Graph,
    Iframe,
    Table,
    Input,
    Panel,
    Video,
    Scheduler,
    ImageList,
    AlarmList
}

export interface GaugePropertyData {
    dlgType: GaugeDialogType;
    withEvents: boolean;
    withActions: any;
    withBitmask: boolean;
    views: View[];
    view: View;
    inputs: GaugeSettings[];
    scripts: Script[];
    settings: any;
    default: any;
    devices: Device[];
    title: string;
    names: string[];
    languageTextEnabled: boolean;
}
