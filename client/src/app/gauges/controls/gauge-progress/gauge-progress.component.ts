import { Component } from '@angular/core';
import { GaugeBaseComponent } from '../../gauge-base/gauge-base.component';
import { GaugeAction, GaugeActionsType, GaugeActionStatus, GaugePropertyColor, GaugeSettings, Variable, GaugeRangeProperty, GaugeStatus, GaugeProperty } from '../../../_models/hmi';
import { Utils } from '../../../_helpers/utils';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';

declare var SVG: any;

@Component({
    selector: 'gauge-progress',
    templateUrl: './gauge-progress.component.html',
    styleUrls: ['./gauge-progress.component.css']
})
export class GaugeProgressComponent extends GaugeBaseComponent {


    static TypeTag = 'svg-ext-gauge_progress';
    static LabelTag = 'HtmlProgress';
    static prefixA = 'A-GXP_';
    static prefixB = 'B-GXP_';
    static prefixH = 'H-GXP_';
    static prefixMax = 'M-GXP_';
    static prefixMin = 'm-GXP_';
    static prefixValue = 'V-GXP_';
    static barColor = '#3F4964';

    static actionsType = { hide: GaugeActionsType.hide, show: GaugeActionsType.show, blink: GaugeActionsType.blink, color: GaugeActionsType.color, enable: GaugeActionsType.enable, disable: GaugeActionsType.disable };

    constructor() {
        super();
    }

    static getSignals(pro: any) {
        let res: string[] = [];
        if (pro.variableId) {
            res.push(pro.variableId);
        }
        if (pro?.ranges?.length) {
            pro.ranges.forEach(r => {
                if (r?.minVariableId) { res.push(r.minVariableId); }
                if (r?.maxVariableId) { res.push(r.maxVariableId); }
            });
        }
        if (pro?.actions && pro.actions.length) {
            pro.actions.forEach(act => {
                res.push(act.variableId);
            });
        }
        return res;
    }

    static getDialogType(): GaugeDialogType {
        return GaugeDialogType.Range;
    }

    static getActions(type: string) {
        return this.actionsType;
    }

    static processValue(ga: GaugeSettings, svgele: any, sig: Variable, gaugeStatus: GaugeStatus) {
        try {
            if (svgele.node?.children?.length === 3) {
                // The bar's value must always come from the Value-bound variable (ga.property.variableId),
                // not from whichever variable triggered this update (e.g. an action-bound variable).
                const valueVarId = ga.property?.variableId;
                let rawValue: any = (valueVarId && gaugeStatus.variablesValue && !Utils.isNullOrUndefined(gaugeStatus.variablesValue[valueVarId]))
                    ? gaugeStatus.variablesValue[valueVarId]
                    : (sig.id === valueVarId ? sig.value : undefined);
                let value = parseFloat(rawValue);
                const min = ga.property?.ranges?.reduce((lastMin, item) => item.min < lastMin.min ? item : lastMin,
                    ga.property?.ranges?.length ? ga.property.ranges[0] : undefined)?.min ?? 0;
                const max = ga.property?.ranges?.reduce((lastMax, item) => item.max > lastMax.max ? item : lastMax,
                    ga.property?.ranges?.length ? ga.property.ranges[0] : undefined)?.max ?? 100;
                // Allow the first range to dynamically override the bar's global min/max via bound variables.
                // Falls back to the static min/max when no variable is bound or the value is unavailable/invalid.
                let dynMin = min;
                let dynMax = max;
                const firstRange = ga.property?.ranges?.[0];
                if (firstRange?.minVariableId && gaugeStatus.variablesValue
                    && !Utils.isNullOrUndefined(gaugeStatus.variablesValue[firstRange.minVariableId])) {
                    const v = parseFloat(gaugeStatus.variablesValue[firstRange.minVariableId]);
                    if (!Number.isNaN(v)) { dynMin = v; }
                }
                if (firstRange?.maxVariableId && gaugeStatus.variablesValue
                    && !Utils.isNullOrUndefined(gaugeStatus.variablesValue[firstRange.maxVariableId])) {
                    const v = parseFloat(gaugeStatus.variablesValue[firstRange.maxVariableId]);
                    if (!Number.isNaN(v)) { dynMax = v; }
                }
                const gap = <GaugeRangeProperty>ga.property?.ranges?.find(item => value >= item.min && value <= item.max);
                let rectBase = Utils.searchTreeStartWith(svgele.node, this.prefixA);
                let heightBase = parseFloat(rectBase.getAttribute('height'));
                let yBase = parseFloat(rectBase.getAttribute('y'));
                let rect = Utils.searchTreeStartWith(svgele.node, this.prefixB);
                if (rectBase && rect) {
                    if (value > dynMax) { value = dynMax; }
                    if (value < dynMin) { value = dynMin; }
                    let k = (heightBase - 0) / (dynMax - dynMin);
                    // Filler height reflects value/(max-min) ratio independently of whether a range is hit,
                    // so the bar still shows current value when ranges are empty or value falls outside any range.
                    let vtoy = (dynMax > dynMin) ? k * (value - dynMin) : 0;
                    if (!Number.isNaN(vtoy)) {
                        rect.setAttribute('y', yBase + heightBase - vtoy);
                        rect.setAttribute('height', vtoy);
                        // Keep the min/max labels in sync with the (possibly variable-bound) dynamic range.
                        const htmlMin = Utils.searchTreeStartWith(svgele.node, this.prefixMin);
                        if (htmlMin) { htmlMin.innerHTML = dynMin.toString(); }
                        const htmlMax = Utils.searchTreeStartWith(svgele.node, this.prefixMax);
                        if (htmlMax) { htmlMax.innerHTML = dynMax.toString(); }
                        // When a blink action is active on the bar rect, blink owns fill/stroke updates.
                        const blinkActive = !!gaugeStatus.actionRef?.timer && gaugeStatus.actionRef?.type === GaugeActionsType.blink;
                        if (gap?.color && !blinkActive) {
                            rect.setAttribute('fill', gap.color);
                        }
                        if (gap?.stroke && !blinkActive) {
                            rect.setAttribute('stroke', gap.stroke);
                        }
                        if (gap?.text) {
                            let htmlValue = Utils.searchTreeStartWith(svgele.node, this.prefixValue);
                            if (htmlValue) {
                                htmlValue.innerHTML = value;
                                if (gap.text) {
                                    htmlValue.innerHTML += ' ' + gap.text;
                                }
                                htmlValue.style.top = (heightBase - vtoy - 7).toString() + 'px';
                            }
                        } else {
                            let htmlValue = Utils.searchTreeStartWith(svgele.node, this.prefixValue);
                            if (htmlValue) {
                                htmlValue.innerHTML += 'ER';
                            }
                        }
                    }
                }
                // check actions
                if (ga.property?.actions) {
                    const propertyColor = new GaugePropertyColor();
                    if (gap?.color) {
                        propertyColor.fill = gap.color;
                    }
                    if (gap?.stroke) {
                        propertyColor.stroke = gap.stroke;
                    }
                    ga.property.actions.forEach(act => {
                        if (act.variableId === sig.id) {
                            // action.range 的命中判断必须基于 action 绑定变量的值 (sig.value)，
                            // 而不是 Value 绑定变量的值 (value)。
                            const actValue = parseFloat(sig.value);
                            GaugeProgressComponent.processAction(act, svgele, rect, actValue, gaugeStatus, propertyColor);
                        }
                    });
                }
            }
        } catch (err) {
            console.error(err);
        }
    }

    static initElement(ga: GaugeSettings, isview: boolean = false): HTMLElement {
        let ele = document.getElementById(ga.id);
        if (ele) {
            ele?.setAttribute('data-name', ga.name);
            if (!ga.property) {
                ga.property = new GaugeProperty();
                let ip: GaugeRangeProperty = new GaugeRangeProperty();
                ip.type = this.getDialogType();
                ip.min = 0;
                ip.max = 100;
                ip.style = [true, true];
                ip.color = '#3F4964';
                ga.property.ranges = [ip];
            }
            if (ga.property.ranges?.length > 0) {
                let gap: GaugeRangeProperty = ga.property.ranges[0];
                // label min
                let htmlLabel = Utils.searchTreeStartWith(ele, this.prefixMin);
                if (htmlLabel) {
                    htmlLabel.innerHTML = gap.min.toString();
                    htmlLabel.style.display = (gap.style[0]) ? 'block' : 'none';
                }
                // label max
                htmlLabel = Utils.searchTreeStartWith(ele, this.prefixMax);
                if (htmlLabel) {
                    htmlLabel.innerHTML = gap.max.toString();
                    htmlLabel.style.display = (gap.style[0]) ? 'block' : 'none';
                }
                // value
                let htmlValue = Utils.searchTreeStartWith(ele, this.prefixValue);
                if (htmlValue) {
                    htmlValue.style.display = (gap.style[1]) ? 'block' : 'none';
                }
                // bar color
                let rect = Utils.searchTreeStartWith(ele, this.prefixB);
                if (rect) {
                    rect.setAttribute('fill', gap.color);
                }
            }
        }
        return ele;
    }

    static initElementColor(bkcolor, color, ele) {
        let rectArea = Utils.searchTreeStartWith(ele, this.prefixA);
        if (rectArea) {
            if (bkcolor) {
                rectArea.setAttribute('fill', bkcolor);
            }
            if (color) {
                rectArea.setAttribute('stroke', color);
            }
        }
        rectArea = Utils.searchTreeStartWith(ele, this.prefixB);
        if (rectArea) {
            if (color) {
                rectArea.setAttribute('stroke', color);
            }
        }
    }

    static getFillColor(ele) {
        let rectArea = Utils.searchTreeStartWith(ele, this.prefixA);
        if (rectArea) {
            return rectArea.getAttribute('fill');
        }
    }

    static getStrokeColor(ele) {
        let rectArea = Utils.searchTreeStartWith(ele, this.prefixA);
        if (rectArea) {
            return rectArea.getAttribute('stroke');
        }
    }

    static getDefaultValue() {
        return { color: this.barColor };
    }

    static getMinByAttribute(arr, attr) {
        return arr?.reduce((min, obj) => obj[attr] < min[attr] ? obj : min);
    }

    static processAction(act: GaugeAction, svgele: any, rect: any, value: any, gaugeStatus: GaugeStatus, propertyColor?: GaugePropertyColor) {
        if (this.actionsType[act.type] === this.actionsType.hide) {
            const hideKey = act.variableId + '|' + act.range.min + '|' + act.range.max;
            if (!gaugeStatus.activeHiders) { gaugeStatus.activeHiders = {}; }
            if (act.range.min <= value && act.range.max >= value) {
                gaugeStatus.activeHiders[hideKey] = true;
                let element = SVG.adopt(svgele.node);
                this.runActionHide(element, act.type, gaugeStatus);
            } else {
                delete gaugeStatus.activeHiders[hideKey];
                if (!Object.keys(gaugeStatus.activeHiders).length) {
                    let element = SVG.adopt(svgele.node);
                    this.runActionShow(element, act.type, gaugeStatus);
                }
            }
        } else if (this.actionsType[act.type] === this.actionsType.show) {
            if (act.range.min <= value && act.range.max >= value) {
                let element = SVG.adopt(svgele.node);
                this.runActionShow(element, act.type, gaugeStatus);
            }
        } else if (this.actionsType[act.type] === this.actionsType.blink) {
            const inRange = (act.range.min <= value && act.range.max >= value);
            // Blink the filler rect (prefixB) only, so the bar height still reflects value/max ratio
            // and only the filler color flashes as an alert.
            GaugeProgressComponent.runRectBlink(rect, act, gaugeStatus, inRange, propertyColor);
        } else if (this.actionsType[act.type] === this.actionsType.color) {
            // Static color action: simply overwrite filler color once when in range,
            // no timer/interval involved — cheaper than blink.
            const inRange = (act.range.min <= value && act.range.max >= value);
            GaugeProgressComponent.runRectColor(rect, act, gaugeStatus, inRange, propertyColor);
        } else if (this.actionsType[act.type] === this.actionsType.disable) {
            if (act.range.min <= value && act.range.max >= value) {
                gaugeStatus.disabled = true;
                svgele.node.style.pointerEvents = 'none';
                svgele.node.style.opacity = '0.5';
                svgele.node.style.cursor = 'not-allowed';
            } else {
                gaugeStatus.disabled = false;
                svgele.node.style.pointerEvents = '';
                svgele.node.style.opacity = '1';
                svgele.node.style.cursor = '';
            }
        } else if (this.actionsType[act.type] === this.actionsType.enable) {
            if (act.range.min <= value && act.range.max >= value) {
                gaugeStatus.disabled = false;
                svgele.node.style.pointerEvents = '';
                svgele.node.style.opacity = '1';
                svgele.node.style.cursor = '';
            }
        }
    }

    private static runRectColor(rect: any, act: GaugeAction, gaugeStatus: GaugeStatus, toEnable: boolean, propertyColor?: GaugePropertyColor) {
        if (!rect) { return; }
        if (!gaugeStatus.actionRef) {
            gaugeStatus.actionRef = new GaugeActionStatus(act.type);
        }
        gaugeStatus.actionRef.type = act.type;
        const actId = GaugeBaseComponent.getBlinkActionId(act);
        const fillA = act.options?.fillA;
        const strokeA = act.options?.strokeA;

        if (toEnable) {
            // Already applied the same color action — do nothing
            if (gaugeStatus.actionRef.spool?.actId === actId && !gaugeStatus.actionRef.timer) {
                if (fillA) { rect.setAttribute('fill', fillA); }
                if (strokeA) { rect.setAttribute('stroke', strokeA); }
                return;
            }
            GaugeBaseComponent.clearAnimationTimer(gaugeStatus.actionRef);
            gaugeStatus.actionRef.spool = {
                bk: propertyColor?.fill || rect.getAttribute('fill'),
                clr: propertyColor?.stroke || rect.getAttribute('stroke'),
                actId: actId
            };
            if (fillA) { rect.setAttribute('fill', fillA); }
            if (strokeA) { rect.setAttribute('stroke', strokeA); }
        } else {
            if (!gaugeStatus.actionRef.spool || gaugeStatus.actionRef.spool.actId === actId) {
                const fill = propertyColor?.fill || gaugeStatus.actionRef.spool?.bk;
                const stroke = propertyColor?.stroke || gaugeStatus.actionRef.spool?.clr;
                if (fill) { rect.setAttribute('fill', fill); }
                if (stroke) { rect.setAttribute('stroke', stroke); }
                gaugeStatus.actionRef.spool = null;
            }
        }
    }

    private static runRectBlink(rect: any, act: GaugeAction, gaugeStatus: GaugeStatus, toEnable: boolean, propertyColor?: GaugePropertyColor) {
        if (!rect) { return; }
        if (!gaugeStatus.actionRef) {
            gaugeStatus.actionRef = new GaugeActionStatus(act.type);
        }
        gaugeStatus.actionRef.type = act.type;
        const actId = GaugeBaseComponent.getBlinkActionId(act);
        const fillA = act.options?.fillA;
        const fillB = act.options?.fillB;
        const strokeA = act.options?.strokeA;
        const strokeB = act.options?.strokeB;
        const interval = Number(act.options?.interval) || 1000;

        if (toEnable) {
            if (gaugeStatus.actionRef.timer && gaugeStatus.actionRef.spool?.actId === actId) {
                return;
            }
            GaugeBaseComponent.clearAnimationTimer(gaugeStatus.actionRef);
            // Save filler color to restore on stop (prefer current range color if any)
            gaugeStatus.actionRef.spool = {
                bk: propertyColor?.fill || rect.getAttribute('fill'),
                clr: propertyColor?.stroke || rect.getAttribute('stroke'),
                actId: actId
            };
            let blinkStatus = false;
            gaugeStatus.actionRef.timer = setInterval(() => {
                blinkStatus = !blinkStatus;
                try {
                    const fill = blinkStatus ? fillA : fillB;
                    const stroke = blinkStatus ? strokeA : strokeB;
                    if (fill) { rect.setAttribute('fill', fill); }
                    if (stroke) { rect.setAttribute('stroke', stroke); }
                } catch (err) {
                    console.error(err);
                }
            }, interval);
        } else {
            if (!gaugeStatus.actionRef.spool || gaugeStatus.actionRef.spool.actId === actId) {
                if (gaugeStatus.actionRef.timer) {
                    clearInterval(gaugeStatus.actionRef.timer);
                    gaugeStatus.actionRef.timer = null;
                }
                const fill = propertyColor?.fill || gaugeStatus.actionRef.spool?.bk;
                const stroke = propertyColor?.stroke || gaugeStatus.actionRef.spool?.clr;
                if (fill) { rect.setAttribute('fill', fill); }
                if (stroke) { rect.setAttribute('stroke', stroke); }
            }
        }
    }
}
