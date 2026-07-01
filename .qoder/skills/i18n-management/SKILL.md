---
name: i18n-management
description: FUXA项目多语言翻译管理的完整指南，包括添加翻译键、新增语言支持和项目级Language Texts系统。当新增翻译键、添加语言支持、修复翻译缺失或管理用户自定义文本时使用此Skill。
---

# 多语言管理

## i18n 架构

- **框架**：@ngx-translate/core + @ngx-translate/http-loader
- **翻译文件**：`client/src/assets/i18n/<lang-code>.json`
- **加载器**：`createTranslateLoader()`（`app.module.ts` 第 246-248 行）
- **默认语言**：`en`
- **回退语言**：`en`（`settings.service.ts` 第 31-33 行）

## 添加翻译键

### 步骤

1. 在 `client/src/assets/i18n/en.json` 中添加英文键值（**必须**）
2. 在 `client/src/assets/i18n/zh-cn.json` 中添加对应中文翻译
3. 在其他语言文件中添加对应翻译

### 键命名约定

使用模块前缀 + 嵌套结构：

| 前缀 | 模块 |
|------|------|
| `dlg.` | 对话框 |
| `general.` | 通用 |
| `msg.` | 消息/提示 |
| `table.` | 表格 |
| `editor.` | 编辑器 |
| `device.` | 设备 |
| `alarms.` | 报警 |
| `scripts.` | 脚本 |
| `reports.` | 报表 |
| `settings.` | 设置 |

示例：
```json
{
  "dlg": {
    "device-property-name": "Device Name"
  },
  "general": {
    "save": "Save",
    "cancel": "Cancel"
  }
}
```

### 模板中使用

```html
<!-- 管道方式 -->
{{'general.save' | translate}}

<!-- 属性绑定 -->
<span [innerHTML]="'msg.confirm-delete' | translate"></span>
```

```ts
// 组件中使用
this.translateService.instant('general.save');
```

## 添加新语言

### 步骤

1. 创建翻译文件：`client/src/assets/i18n/<lang-code>.json`
   - 语言代码格式：2字母（en, de, fr）或带地区（zh-cn, pt-br）
2. 复制 `en.json` 作为模板，翻译所有键值
3. 在语言选择 UI 中添加选项：
   - 在 `LanguageTypePropertyComponent` 的 `languageType` 数组中添加

## 项目级多语言文本（Language Texts）

FUXA 还有一套**独立于 i18n JSON**的用户自定义翻译文本系统：

### 模型

```
LanguageText {
  id: string
  name: string           // 键名
  group: string          // 分组
  value: string          // 默认值
  translations: {        // 各语言翻译
    [langId]: value
  }
}
```

### 管理组件

| 组件 | 路径 | 用途 |
|------|------|------|
| `LanguageTextListComponent` | `client/src/app/language/` | 文本列表管理 |
| `LanguageTextPropertyComponent` | `client/src/app/language/` | 单个文本编辑 |
| `LanguageTypePropertyComponent` | `client/src/app/language/` | 语言类型配置 |

### API

通过 `ProjectService` 管理：
- `ProjectService.getLanguages()` — 获取语言列表
- `ProjectService.getTexts()` — 获取文本列表
- `ProjectService.saveTexts()` — 保存文本

运行时使用 `LanguageService.getTranslation()` 获取翻译。

## 特殊组件国际化

### MatPaginator

使用 `CustomMatPaginatorIntl`（`paginator-intl.ts`）自定义分页器翻译。

### 工作日/月份

使用 `general.weekdays` 和 `general.months` 翻译键。

### 用户组标签

`UserGroups.Groups` 动态翻译。

## 注意事项

- `en.json` 和 `zh-cn.json` 行数可能不同，中文文件可能缺少部分键
- 新增功能时必须同时更新所有语言文件
- 翻译键不能包含空格，使用连字符分隔
- i18n JSON 文件较大（en.json ~2000 行），编辑时注意格式正确
- `dist/` 目录下也有对应的翻译文件，需同步处理

## 关键源文件

| 文件 | 内容 |
|------|------|
| `client/src/assets/i18n/en.json` | 英文翻译（~2039 行）|
| `client/src/assets/i18n/zh-cn.json` | 中文翻译（~1823 行）|
| `client/src/app/_services/settings.service.ts` | 语言初始化（第 29-34 行）|
| `client/src/app/app.module.ts` | TranslateLoader 配置（第 246-248 行）|
| `client/src/app/language/` | Language Texts 管理组件 |
| `client/src/app/gui-helpers/paginator-intl.ts` | MatPaginator 国际化 |
