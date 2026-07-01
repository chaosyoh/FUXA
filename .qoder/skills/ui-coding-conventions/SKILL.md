---
name: ui-coding-conventions
description: FUXA前端UI编码规范，涵盖布局模式、对话框模式、Angular Material使用约定、主题配置和CSS层级。当开发新UI功能、重构现有界面或修复样式问题时使用此Skill。
---

# UI 编码规范

## 布局模式

### 主布局

```
HeaderComponent（顶部导航）
├── SidenavComponent（侧边栏）
│   └── 路由内容区（Home/Editor/Device/View 等）
```

- **EditorComponent**：SVG 编辑器 + 右侧属性面板（滑出式）
- **HomeComponent**：HMI 视图渲染
- **DeviceComponent**：设备管理（列表/树视图）

### 响应式

- 主要面向**桌面/工业平板**，非移动端优先
- 触摸键盘：`NgxTouchKeyboardDirective`（`keyboardFullScreen` 模式）

## 对话框模式（MatDialog）

### 标准打开方式

```ts
this.dialog.open(XxxPropertyComponent, {
    position: { top: '60px' },
    disableClose: true,        // 表单类对话框禁止点击外部关闭
    data: <XxxData>{ ... }
});
```

### 标准对话框组件结构

```ts
@Component({
    selector: 'app-xxx-property',
    templateUrl: './xxx-property.component.html',
    styleUrls: ['./xxx-property.component.scss']
})
export class XxxPropertyComponent implements OnInit {
    constructor(
        public dialogRef: MatDialogRef<XxxPropertyComponent>,
        @Inject(MAT_DIALOG_DATA) public data: XxxData
    ) {}

    ngOnInit(): void { /* 初始化表单 */ }

    onNoClick(): void {
        this.dialogRef.close();
    }

    onOkClick(): void {
        this.dialogRef.close(this.data);
    }
}
```

### 对话框模板

```html
<h1 mat-dialog-title>{{'dlg.xxx-title' | translate}}</h1>
<div mat-dialog-content>
    <!-- 表单内容 -->
</div>
<div mat-dialog-actions class="dialog-action mt10">
    <button mat-raised-button (click)="onNoClick()">{{'general.cancel' | translate}}</button>
    <button mat-raised-button color="primary" (click)="onOkClick()">{{'general.ok' | translate}}</button>
</div>
```

### 确认对话框

```ts
this.dialog.open(ConfirmDialogComponent, {
    data: {
        name: '确认删除？',
        message: '此操作不可撤销'
    }
}).afterClosed().subscribe(result => {
    if (result) { /* 用户确认 */ }
});
```

### 权限对话框

```ts
this.dialog.open(PermissionDialogComponent, {
    data: <PermissionData>{ ... }
});
```

## Material 组件使用约定

### 表单字段

使用 `my-form-field` 包装类：

```html
<div class="my-form-field">
    <span>{{'dlg.property-name' | translate}}</span>
    <input matInput formControlName="name">
</div>
```

### 间距类

| 类名 | 作用 |
|------|------|
| `mt5` | margin-top: 5px |
| `mt10` | margin-top: 10px |
| `dialog-action` | 对话框操作按钮容器 |

### 表格

```ts
// 标准表格模式
displayedColumns: string[] = ['name', 'type', 'address', 'actions'];
dataSource = new MatTableDataSource<Tag>();

@ViewChild(MatSort) sort: MatSort;
@ViewChild(MatPaginator) paginator: MatPaginator;

ngAfterViewInit() {
    this.dataSource.sort = this.sort;
    this.dataSource.paginator = this.paginator;
}
```

- 使用 `CustomMatPaginatorIntl` 做分页器翻译
- 使用 `SelectionModel` 做多选
- `*ngIf` 内的 Paginator 需在 `ngAfterViewInit` 中用 `setTimeout` 延迟绑定

### 选择下拉

```html
<mat-select formControlName="type">
    <mat-option *ngFor="let opt of options" [value]="opt.value">
        {{opt.label | translate}}
    </mat-option>
</mat-select>
```

### 扩展面板（始终展开）

```html
<mat-expansion-panel [expanded]="true" hideToggle="true">
    <!-- 内容 -->
</mat-expansion-panel>
```

CSS：
```scss
mat-expansion-panel {
    pointer-events: none;  // 禁用 header 点击
    .action-button {
        pointer-events: auto;  // 恢复内部按钮可点击
    }
}
```

**注意**：避免使用 `[disabled]="true"`，会导致样式异常（文字变灰、背景变化）。

### Toast 通知

```ts
constructor(private toastr: ToastrService) {}

this.toastr.success('操作成功');
this.toastr.error('操作失败');
this.toastr.warning('警告信息');
```

## 主题配置

### 亮/暗色切换

通过 `.dark-theme` CSS 类切换（`theme.scss`）：

```scss
@use '@angular/material' as mat;

$dark-theme: mat.define-theme((
    color: (theme-type: dark)
));

.dark-theme {
    @include mat.all-component-themes($dark-theme);
}
```

### CSS 层级（优先级从低到高）

1. `client/src/styles.css` — 全局基础样式
2. `client/src/theme.scss` — Angular Material 主题
3. `client/src/material-overrides.scss` — Material 组件自定义覆盖
4. 组件级 `.component.scss` — 局部样式（ViewEncapsulation）

### Material 覆盖

`material-overrides.scss` 中覆盖 Material 组件默认样式：
- 对话框圆角、阴影
- 输入框下划线
- 按钮尺寸等

## Gauge 控件约定

### 静态方法

每个 Gauge 控件需实现：

```ts
static getEvents(): GaugeEvent[] { /* 返回支持的事件列表 */ }
static getUnit(): string { /* 返回单位 */ }
static getDigits(): number { /* 返回小数位数 */ }
```

### 事件类型

- `GaugeEventType`：事件类型枚举（click, mousedown, mouseup 等）
- `GaugeEventActionType`：动作类型枚举（setValue, openView, openDialog 等）

### 变量映射

使用 `VariableMapping` 支持 View 复用。

## Tag 属性编辑表单

所有 `tag-property-edit` 组件采用**双栏布局**：
- 对话框宽度：750px
- 左侧 `column-left`：基础属性字段
- 右侧 `column-right`：归档/缩放设置
- 两栏以竖线分隔

## 注意事项

- 使用 `UntypedFormGroup` / `UntypedFormBuilder`（项目统一使用 Untyped 版本）
- 所有用户可见文本使用 i18n 翻译管道
- 对话框宽度固定（如 750px），不使用百分比
- `*ngIf` 内的 `@ViewChild` 组件可能为 undefined，需用 `setTimeout` 延迟绑定
- 避免使用 Angular Material 的 `[disabled]` 属性控制面板展开

## 关键源文件

| 文件 | 内容 |
|------|------|
| `client/src/theme.scss` | 主题配置（~134 行）|
| `client/src/material-overrides.scss` | Material 组件覆盖样式 |
| `client/src/styles.css` | 全局基础样式 |
| `client/src/app/gui-helpers/confirm-dialog/` | 确认对话框组件 |
| `client/src/app/language/language-text-property/` | 表单字段模式参考 |
| `client/src/app/device/tag-property-edit/` | 双栏布局参考 |
