/**
 * HtmlButton 样式扩展设计
 * 阶段1：预设主题 | 阶段2：边框/圆角/内边距 | 阶段3：自由 customStyle
 *
 * 数据载体：GaugeProperty.options.style (HtmlButtonStyle)
 * 序列化：自动随 GaugeSettings 持久化到后端，无需 schema 变更
 */

export enum HtmlButtonPreset {
    default = 'default',
    flat = 'flat',
    outlined = 'outlined',
    raised = 'raised',
    rounded = 'rounded',
    industrial = 'industrial',
    danger = 'danger',
    warning = 'warning',
    success = 'success',
}

export interface BorderStyle {
    width: number;                                       // px
    style: 'solid' | 'dashed' | 'dotted' | 'double' | 'none';
    color: string;                                       // #rrggbb
}

export interface HtmlButtonStyle {
    preset?: HtmlButtonPreset;                           // 阶段1
    border?: BorderStyle;                                // 阶段2
    borderRadius?: string;                               // 阶段2 (e.g. '4px' | '50%' | '8px 16px')
    padding?: string;                                    // 阶段2 (e.g. '6px 16px')
    fontSize?: string;                                   // 基础外观：字号 (e.g. '14px' | '1rem')
    fontWeight?: string;                                 // 基础外观：字重 (normal/bold/100-900)
    boxShadow?: string;                                  // 基础外观：阴影
    customStyle?: { [cssProperty: string]: string };     // 阶段3
}

/**
 * 共享基础布局：让 button 完全填充 SVG foreignObject 边界，
 * 并通过 flex 居中文本，避免预设 padding 导致文字相对外框偏移
 *
 * 注意：boxShadow 重置为 none，避免编辑器引入的 .btn 类自带
 * `box-shadow: 0 1px 4px rgba(0,0,0,.6)` 在用户自定义边框时
 * 让"下边框"看起来比上/左/右更粗（实际是阴影叠加导致）
 */
const BASE_LAYOUT: Partial<CSSStyleDeclaration> = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    width: '100%',
    height: '100%',
    boxSizing: 'border-box',
    textAlign: 'center',
    boxShadow: 'none',
};

/**
 * 预设主题样式映射
 * key 为 HtmlButtonPreset 枚举值，value 为 CSSStyleDeclaration 部分键值
 */
export const BUTTON_PRESETS: Record<HtmlButtonPreset, Partial<CSSStyleDeclaration>> = {
    [HtmlButtonPreset.default]: { ...BASE_LAYOUT },

    [HtmlButtonPreset.flat]: {
        ...BASE_LAYOUT,
        border: 'none',
        borderRadius: '2px',
        boxShadow: 'none',
        padding: '6px 16px',
        fontWeight: '500',
        textTransform: 'uppercase',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.outlined]: {
        ...BASE_LAYOUT,
        backgroundColor: 'transparent',
        border: '1px solid currentColor',
        borderRadius: '4px',
        padding: '5px 15px',
        fontWeight: '500',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.raised]: {
        ...BASE_LAYOUT,
        border: 'none',
        borderRadius: '4px',
        padding: '6px 16px',
        boxShadow: '0 3px 1px -2px rgba(0,0,0,.2), 0 2px 2px 0 rgba(0,0,0,.14), 0 1px 5px 0 rgba(0,0,0,.12)',
        fontWeight: '500',
        textTransform: 'uppercase',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.rounded]: {
        ...BASE_LAYOUT,
        border: 'none',
        borderRadius: '999px',
        padding: '8px 24px',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.industrial]: {
        ...BASE_LAYOUT,
        border: '2px solid #333',
        borderRadius: '2px',
        padding: '4px 12px',
        backgroundColor: '#4a5568',
        color: '#ffffff',
        fontWeight: 'bold',
        textShadow: '1px 1px 0 #000',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.danger]: {
        ...BASE_LAYOUT,
        backgroundColor: '#dc3545',
        color: '#ffffff',
        border: 'none',
        borderRadius: '4px',
        padding: '6px 16px',
        fontWeight: '600',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.warning]: {
        ...BASE_LAYOUT,
        backgroundColor: '#ffc107',
        color: '#212529',
        border: 'none',
        borderRadius: '4px',
        padding: '6px 16px',
        fontWeight: '600',
        cursor: 'pointer',
    },

    [HtmlButtonPreset.success]: {
        ...BASE_LAYOUT,
        backgroundColor: '#28a745',
        color: '#ffffff',
        border: 'none',
        borderRadius: '4px',
        padding: '6px 16px',
        fontWeight: '600',
        cursor: 'pointer',
    },
};

/**
 * 危险 CSS 值过滤（阶段3 XSS 防护）
 * 拒绝 expression()、javascript: 协议、url() 中的脚本
 */
const DANGEROUS_CSS_PATTERN = /expression\s*\(|javascript\s*:|url\s*\(\s*['"]?\s*javascript/i;

export function isSafeCssValue(value: string): boolean {
    if (typeof value !== 'string') {return false;}
    return !DANGEROUS_CSS_PATTERN.test(value);
}

/**
 * 应用按钮样式到 DOM 元素
 * 优先级：预设 < 边框/圆角/内边距 < customStyle
 *
 * @param htmlButton 目标 button HTMLElement (前缀 B-HXB_)
 * @param style HtmlButtonStyle 配置对象
 */
export function applyButtonStyle(htmlButton: HTMLElement, style: HtmlButtonStyle): void {
    if (!htmlButton || !style) {return;}

    // 1) 预设主题
    if (style.preset && BUTTON_PRESETS[style.preset]) {
        const presetStyle = BUTTON_PRESETS[style.preset];
        Object.keys(presetStyle).forEach(key => {
            (htmlButton.style as any)[key] = (presetStyle as any)[key];
        });
    }

    // 2) 边框（始终处理，确保从“启用”切回“关闭”时也能清除上一次的内联样式）
    if (style.border && style.border.style && style.border.style !== 'none' && style.border.width > 0) {
        htmlButton.style.borderWidth = `${style.border.width}px`;
        htmlButton.style.borderStyle = style.border.style;
        htmlButton.style.borderColor = style.border.color;
        // 用户显式自定义边框时，强制清除编辑器 .btn 类自带的底部 box-shadow，
        // 否则下边框看起来比上/左/右更粗（实际是阴影叠加）
        // 若预设已显式声明 boxShadow，会在第 1) 步覆盖；这里只在用户未通过预设设置时兜底
        if (!style.preset || !(BUTTON_PRESETS[style.preset] as any).boxShadow) {
            htmlButton.style.boxShadow = 'none';
        }
    } else {
        // 用户取消边框：清空所有相关内联样式，避免上次设置残留
        htmlButton.style.borderWidth = '';
        htmlButton.style.borderStyle = '';
        htmlButton.style.borderColor = '';
        htmlButton.style.border = '';
    }

    // 3) 圆角
    if (style.borderRadius && isSafeCssValue(style.borderRadius)) {
        htmlButton.style.borderRadius = style.borderRadius;
    } else {
        htmlButton.style.borderRadius = '';
    }

    // 4) 内边距
    if (style.padding && isSafeCssValue(style.padding)) {
        htmlButton.style.padding = style.padding;
    } else {
        htmlButton.style.padding = '';
    }

    // 5) 基础外观：字号 / 字重 / 阴影
    //    背景色 / 文字色 由 tools_bottom_2 的 color-fill / color-stroke 通过
    //    HtmlButtonComponent.initElementColor 直接写入 inline style，避免与之冲突，
    //    样式面板不再提供这两个字段。
    if (style.fontSize && isSafeCssValue(style.fontSize)) {
        htmlButton.style.fontSize = style.fontSize;
    } else {
        htmlButton.style.fontSize = '';
    }
    if (style.fontWeight && isSafeCssValue(style.fontWeight)) {
        htmlButton.style.fontWeight = style.fontWeight;
    } else {
        htmlButton.style.fontWeight = '';
    }
    if (style.boxShadow && isSafeCssValue(style.boxShadow)) {
        htmlButton.style.boxShadow = style.boxShadow;
    } else if (!(style.border && style.border.style && style.border.style !== 'none' && style.border.width > 0)) {
        // 仅当未启用边框时，回到 .btn 类自带的默认阴影（清空内联即继承类样式）
        htmlButton.style.boxShadow = '';
    }

    // 6) 自定义 CSS（最高优先级）
    // 先清掉上一次通过 customStyle 设置过的 key，避免删除条目后仍生效
    const prevKeys: string[] = (htmlButton as any).__fuxaAppliedCustomKeys || [];
    prevKeys.forEach(k => htmlButton.style.removeProperty(k));
    const newKeys: string[] = [];
    if (style.customStyle) {
        Object.keys(style.customStyle).forEach(cssKey => {
            const cssVal = style.customStyle[cssKey];
            if (isSafeCssValue(cssVal)) {
                htmlButton.style.setProperty(cssKey, cssVal);
                newKeys.push(cssKey);
            }
        });
    }
    (htmlButton as any).__fuxaAppliedCustomKeys = newKeys;
}

/**
 * 主题预览元数据，供属性对话框下拉选择使用
 */
export const PRESET_PREVIEW_LIST: Array<{
    value: HtmlButtonPreset;
    label: string;
    translationKey: string;
}> = [
    { value: HtmlButtonPreset.default,    label: 'Default',    translationKey: 'gauges.preset-default' },
    { value: HtmlButtonPreset.flat,       label: 'Flat',       translationKey: 'gauges.preset-flat' },
    { value: HtmlButtonPreset.outlined,   label: 'Outlined',   translationKey: 'gauges.preset-outlined' },
    { value: HtmlButtonPreset.raised,     label: 'Raised',     translationKey: 'gauges.preset-raised' },
    { value: HtmlButtonPreset.rounded,    label: 'Rounded',    translationKey: 'gauges.preset-rounded' },
    { value: HtmlButtonPreset.industrial, label: 'Industrial', translationKey: 'gauges.preset-industrial' },
    { value: HtmlButtonPreset.danger,     label: 'Danger',     translationKey: 'gauges.preset-danger' },
    { value: HtmlButtonPreset.warning,    label: 'Warning',    translationKey: 'gauges.preset-warning' },
    { value: HtmlButtonPreset.success,    label: 'Success',    translationKey: 'gauges.preset-success' },
];
