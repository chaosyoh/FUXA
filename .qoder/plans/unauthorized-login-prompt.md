# 未登录用户点击授权控件弹出登录对话框

## Context

当前 FUXA 中，未登录用户点击需要授权的控件（如设置了 Administrator 才能 enabled 的按钮），没有任何反应。这是因为服务端在发送项目数据前，已将无权限控件的 `events` 清空。用户希望改为弹出登录对话框，登录成功后项目重新加载，控件变为可用。

## 实现思路

**核心区分**：服务端能区分"未登录"（`userPermission` 为 falsy）和"已登录但权限不足"（`userPermission` 有值但不在允许列表中）。对未登录用户，保留事件并标记 `_requiresLogin`；对已登录无权限用户，保持原有行为（清空事件）。

**两层拦截**：
- **SVG 控件**（按钮、形状等）：事件保留到客户端，在 `runEvents` 中检查 `_requiresLogin` 标记后弹出登录
- **HTML 控件**（Switch、Input 等在 foreignObject 内的控件）：在 foreignObject 上标记 `data-requires-login`，客户端用捕获阶段事件监听器拦截交互

## Task 1: 修改服务端权限过滤逻辑

**文件**：`server/runtime/project/index.js` — `_filterProjectPermission` 函数（~第 961 行）

### 1.1 在函数开头添加登录状态判断

在第 964 行 `const projectPermission = ...` 之后添加：
```javascript
const isNotLoggedIn = !userPermission && settings.secureEnabled;
```

### 1.2 修改视图元素的 `else if (!itemPermission.enabled)` 分支

将原有的"清空事件 + 禁用控件"逻辑改为条件分支：

```javascript
} else if (!itemPermission.enabled) {
    if (isNotLoggedIn) {
        // 未登录：保留事件，标记需要登录
        item.property._requiresLogin = true;
        // 在 foreignObject 上标记 data-requires-login（不禁用内部控件）
        const indexInContent = view.svgcontent.indexOf(item.id);
        if (indexInContent >= 0) {
            var splitted = utils.domStringSplitter(view.svgcontent, 'foreignobject', indexInContent);
            if (splitted.tagcontent && splitted.tagcontent.length) {
                var foTag = '<foreignobject';
                var foPos = splitted.tagcontent.toLowerCase().indexOf(foTag);
                if (foPos !== -1) {
                    foPos += foTag.length;
                    splitted.tagcontent = splitted.tagcontent.slice(0, foPos) +
                        ' data-requires-login="true"' + splitted.tagcontent.slice(foPos);
                    view.svgcontent = splitted.before + splitted.tagcontent + splitted.after;
                }
            }
        }
    } else {
        // 已登录无权限：原有逻辑不变
        item.property.events = [];
        const indexInContent = view.svgcontent.indexOf(item.id);
        if (indexInContent >= 0) {
            var splitted = utils.domStringSplitter(view.svgcontent, 'foreignobject', indexInContent);
            if (splitted.tagcontent && splitted.tagcontent.length) {
                var disabled = utils.domStringSetAttribute(splitted.tagcontent, ['select', 'input', 'button'], 'disabled');
                view.svgcontent = splitted.before + disabled + splitted.after;
            }
        }
    }
}
```

**安全性**：仅对未登录用户保留事件。事件参数（variableId、action 类型）不含敏感信息，且后端写入操作（setValue、runScript 等）有独立权限验证。

## Task 2: 客户端添加登录拦截

**文件**：`client/src/app/fuxa-view/fuxa-view.component.ts`

### 2.1 添加 import（文件顶部 ~第 36 行之后）

```typescript
import { AuthService } from '../_services/auth.service';
import { LoginComponent } from '../login/login.component';
import { LoginOverlayColorType } from '../_models/hmi';
```

### 2.2 构造函数注入 AuthService（~第 90-100 行）

在现有参数列表末尾添加 `private authService: AuthService`。

### 2.3 添加成员变量（~第 88 行 `zIndexCounter` 之后）

```typescript
private loginDialogOpen = false;
```

### 2.4 修改 `runEvents` 方法（第 571 行）

在 disabled 检查之后、事件分发循环之前，插入登录检查：

```typescript
public runEvents(self: any, ga: GaugeSettings, ev: any, events: any) {
    if (self.mapGaugeStatus?.[ga.id]?.disabled) { return; }
    // ★ 未登录用户点击需授权控件 → 弹出登录
    if (self.checkRequiresLogin(ga)) { return; }
    // ... 原有事件分发逻辑不变
}
```

### 2.5 添加辅助方法（在 `runEvents` 之后）

```typescript
private checkRequiresLogin(ga: GaugeSettings): boolean {
    if (!ga?.property?._requiresLogin) { return false; }
    if (!this.projectService.isSecurityEnabled()) { return false; }
    if (this.authService.getUser()) { return false; }
    this.openLoginDialog();
    return true;
}

private openLoginDialog(): void {
    if (this.loginDialogOpen) { return; }
    this.loginDialogOpen = true;
    const hmi = this.projectService.getHmi();
    let dialogConfig: any = { data: {}, disableClose: true, autoFocus: false };
    if (hmi?.layout?.loginoverlaycolor && hmi.layout.loginoverlaycolor !== LoginOverlayColorType.none) {
        dialogConfig.backdropClass = hmi.layout.loginoverlaycolor === LoginOverlayColorType.black
            ? 'backdrop-black' : 'backdrop-white';
    }
    this.fuxaDialog.open(LoginComponent, dialogConfig).afterClosed().subscribe(() => {
        this.loginDialogOpen = false;
    });
}
```

### 2.6 添加 foreignObject 捕获阶段监听器

为 HTML 控件（foreignObject 内）添加全局捕获监听，在 `loadHmi` 中 SVG 内容加载后绑定：

```typescript
private bindRequiresLoginHandler(): void {
    const container = this.dataContainer?.nativeElement;
    if (!container) { return; }
    container.addEventListener('click', (ev: MouseEvent) => {
        const target = ev.target as HTMLElement;
        if (target?.closest?.('[data-requires-login="true"]')) {
            ev.stopPropagation();
            ev.preventDefault();
            this.openLoginDialog();
        }
    }, true);  // useCapture = true，在所有其他处理器之前拦截
}
```

在 `loadHmi` 方法中第 218 行 `this.dataContainer.nativeElement.innerHTML = ...` 之后调用：
```typescript
this.bindRequiresLoginHandler();
```

## 涉及的文件

| 文件 | 修改内容 |
|------|---------|
| `server/runtime/project/index.js` | `_filterProjectPermission` 函数增加未登录分支 |
| `client/src/app/fuxa-view/fuxa-view.component.ts` | 添加 AuthService 注入、登录拦截逻辑、捕获阶段监听器 |

## 验证步骤

1. 启动开发环境（`npm start` 启动服务端，`ng serve` 启动前端）
2. 在 App Settings 中启用 Authentication（`secureEnabled = true`）
3. 创建一个按钮，设置权限为 Administrator（enabled）
4. **不登录**，以访客身份访问视图
5. 点击按钮 → 应弹出登录对话框
6. 取消登录 → 对话框关闭，再次点击按钮 → 再次弹出
7. 登录为 admin → 登录成功后项目重载 → 按钮变为可点击
8. 登出后，以低权限用户登录 → 点击按钮 → 无反应（原有行为）
9. 测试 HTML 控件（Switch、Input）在 foreignObject 内的相同场景
