---
name: add-angular-component
description: 在FUXA前端项目中新增Angular组件的标准化流程，覆盖命名约定、组件分类、注册步骤和编码模式。当新增UI功能模块、属性面板、对话框或Gauge控件时使用此Skill。
---

# 新增 Angular 组件

## 项目约定

- **框架**：Angular 18 + Angular Material 18
- **选择器前缀**：`app-`（angular.json 中 `prefix: "app"`）
- **目录命名**：kebab-case（如 `device-property`）
- **类名命名**：PascalCase（如 `DevicePropertyComponent`）
- **三件套**：`.component.ts` / `.component.html` / `.component.scss`

## 组件分类

### 1. 页面组件（路由级）

放在功能目录下，注册路由：

```
client/src/app/<feature>/
├── <feature>.component.ts
├── <feature>.component.html
└── <feature>.component.scss
```

注册步骤：
1. 在 `app.module.ts` 的 `declarations` 中添加
2. 在 `app.routing.ts` 中添加路由：
```ts
{ path: '<feature>', component: <Feature>Component, canActivate: [AuthGuard] }
```

### 2. 对话框组件（MatDialog）

标准对话框组件结构：

```ts
@Component({
  selector: 'app-<name>-property',
  templateUrl: './<name>-property.component.html',
  styleUrls: ['./<name>-property.component.scss']
})
export class <Name>PropertyComponent {
  constructor(
    public dialogRef: MatDialogRef<<Name>PropertyComponent>,
    @Inject(MAT_DIALOG_DATA) public data: <Name>Data
  ) {}

  onNoClick(): void {
    this.dialogRef.close();
  }

  onOkClick(): void {
    this.dialogRef.close(this.data);
  }
}
```

标准对话框模板：
```html
<h1 mat-dialog-title>{{title}}</h1>
<div mat-dialog-content>
  <!-- 表单内容 -->
</div>
<div mat-dialog-actions class="dialog-action mt10">
  <button mat-raised-button (click)="onNoClick()">Cancel</button>
  <button mat-raised-button color="primary" (click)="onOkClick()">OK</button>
</div>
```

打开方式：
```ts
this.dialog.open(<Name>PropertyComponent, {
  position: { top: '60px' },
  disableClose: true,
  data: <data>
});
```

确认对话框：
```ts
this.dialog.open(ConfirmDialogComponent, {
  data: { name: '确认删除？', message: '...' }
}).afterClosed().subscribe(result => {
  if (result) { /* 用户确认 */ }
});
```

### 3. 属性面板组件

嵌入 Editor 侧边栏，使用 `@Input`/`@Output` 模式：

```ts
export class <Name>PropertyComponent {
  @Input() data: any;
  @Output() edit = new EventEmitter<any>();
}
```

### 4. Gauge（仪表）控件

位于 `client/src/app/gauges/`，需实现静态方法：

```ts
static getEvents(): GaugeEvent[] { return [...]; }
static getUnit(): string { return ''; }
static getDigits(): number { return 0; }
```

形状库位于 `client/src/assets/lib/svgeditor/shapes/`。

## 注册步骤

### 1. 声明组件

在 `client/src/app/app.module.ts` 的 `declarations` 数组中添加：

```ts
@NgModule({
  declarations: [
    // ... 现有组件
    NewComponent,
  ],
  // ...
})
```

### 2. 导入 Material 模块

确保 `material.module.ts` 中已导入所需模块（MatDialog, MatFormField, MatInput, MatSelect 等）。

### 3. 路由注册（页面组件）

在 `app.routing.ts` 中添加路由配置。

## 表单约定

使用 `UntypedFormGroup` / `UntypedFormBuilder`（项目统一使用 Untyped 版本）：

```ts
export class MyComponent implements OnInit {
  form: UntypedFormGroup;

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit() {
    this.form = this.fb.group({
      name: ['', Validators.required],
      value: [0, [Validators.required, Validators.min(0)]]
    });
  }
}
```

## 服务注入约定

| 服务 | 用途 |
|------|------|
| `ProjectService` | 项目数据管理、HMI 视图 |
| `HmiService` | 实时通信（Socket.IO/SignalR）|
| `SettingsService` | 应用设置 + 语言管理 |
| `TranslateService` | @ngx-translate/core 国际化 |
| `AuthService` | 认证 |
| `DeviceService` | 设备管理 |
| `GaugesManager` | 仪表/控件管理 |
| `ToastrService` | Toast 通知（ngx-toastr）|

## 样式约定

### CSS 工具类

| 类名 | 作用 |
|------|------|
| `my-form-field` | 表单字段包装容器 |
| `mt5` | margin-top: 5px |
| `mt10` | margin-top: 10px |
| `dialog-action` | 对话框操作按钮区域 |

### 表单字段模式

```html
<div class="my-form-field">
  <span>标签名称</span>
  <input matInput formControlName="name">
</div>
```

### 主题

- 亮/暗色通过 `.dark-theme` 类切换（`theme.scss`）
- 全局样式：`styles.css`
- 主题配置：`theme.scss`
- Material 覆盖：`material-overrides.scss`

## 检查清单

- [ ] 组件已添加到 `app.module.ts` declarations
- [ ] 路由已注册（页面组件）
- [ ] Material 模块已导入
- [ ] 国际化键已添加到 `en.json` 和 `zh-cn.json`
- [ ] 样式遵循项目约定（my-form-field, mt5/mt10, dialog-action）
- [ ] 服务注入正确

## 关键源文件

| 文件 | 内容 |
|------|------|
| `client/src/app/app.module.ts` | 所有组件声明 + 模块导入 |
| `client/src/app/app.routing.ts` | 路由配置 |
| `client/src/app/material.module.ts` | Angular Material 模块集合 |
| `client/src/theme.scss` | 主题配置 |
| `client/src/styles.css` | 全局样式 |
| `client/src/material-overrides.scss` | Material 组件样式覆盖 |
