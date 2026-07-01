import { Component } from '@angular/core';
import { GaugeBaseComponent } from '../../gauge-base/gauge-base.component';
import { GaugeSettings, Variable, GaugeStatus, GaugeAction, Event, GaugeActionsType } from '../../../_models/hmi';
import { Utils } from '../../../_helpers/utils';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';

declare var SVG: any;

@Component({
    selector: 'html-select',
    templateUrl: './html-select.component.html',
    styleUrls: ['./html-select.component.css']
})
export class HtmlSelectComponent extends GaugeBaseComponent {


    static TypeTag = 'svg-ext-html_select';
    static LabelTag = 'HtmlSelect';
    static prefix = 'S-HXS_';

    static actionsType = { hide: GaugeActionsType.hide, show: GaugeActionsType.show, enable: GaugeActionsType.enable, disable: GaugeActionsType.disable };

    constructor() {
        super();
    }

    static getSignals(pro: any) {

        let res: string[] = [];
        if (pro.variableId) {
            res.push(pro.variableId);
        }
        if (pro.actions && pro.actions.length) {
            pro.actions.forEach(act => {
                res.push(act.variableId);
            });
        }
        return res;
    }

    static getDialogType(): GaugeDialogType {
        return GaugeDialogType.Step;
    }

    static getActions(type: string) {
        return this.actionsType;
    }

    static getHtmlEvents(ga: GaugeSettings): Event {
        let ele = document.getElementById(ga.id);
        if (ele) {
            let element = Utils.searchTreeStartWith(ele, this.prefix);
            if (element) {
                // Readonly span does not emit change events
                if (element.tagName === 'SPAN' && element.getAttribute('data-readonly') === 'true') {
                    return null;
                }
                let event = new Event();
                event.dom = element;
                event.type = 'change';
                event.ga = ga;
                return event;
            }
        }
        return null;
    }

    static processValue(ga: GaugeSettings, svgele: any, sig: Variable, gaugeStatus: GaugeStatus) {
        try {
            let element = Utils.searchTreeStartWith(svgele.node, this.prefix);
            if (element) {
                let val = parseFloat(sig.value);
                if (Number.isNaN(val)) {
                    val = Number(sig.value);
                } else {
                    val = parseFloat(val.toFixed(5));
                }
                if (ga.property.variableId === sig.id) {
                    // Handle readonly span rendering
                    if (element.tagName === 'SPAN' && element.getAttribute('data-readonly') === 'true') {
                        try {
                            const ranges = JSON.parse(element.getAttribute('data-ranges') || '[]');
                            const matched = ranges.find(r => r.value === String(val));
                            element.textContent = matched ? matched.text : String(val);
                            if (matched?.bg) { element.style.background = matched.bg; }
                            if (matched?.fg) { element.style.color = matched.fg; }
                        } catch (e) {
                            element.textContent = String(val);
                        }
                    } else {
                        // Normal select rendering
                        element.value = val;
                        let range = ga.property.ranges.find(e => e.min == val);
                        if (range) {
                            element.style.background = range.color;
                            element.style.color = range.stroke;
                        }
                    }
                }
                // check actions
                if (ga.property.actions) {
                    ga.property.actions.forEach(act => {
                        if (act.variableId === sig.id) {
                            HtmlSelectComponent.processAction(act, svgele, element, val, gaugeStatus);
                        }
                    });
                }
            }
        } catch (err) {
            console.error(err);
        }
    }

    static initElement(ga: GaugeSettings, isview: boolean = false): HTMLElement {
        let select = null;
        let ele = document.getElementById(ga.id);
        if (ele) {
            ele?.setAttribute('data-name', ga.name);
            select = Utils.searchTreeStartWith(ele, this.prefix);
            if (select) {
                // If readonly in view mode, replace <select> with <span> for text rendering
                if (ga.property?.readonly && isview) {
                    const span = document.createElement('span');
                    span.id = select.id;
                    span.className = select.className;
                    span.style.cssText = select.style.cssText;
                    span.style['display'] = 'inline-block';
                    span.style['width'] = '100%';
                    span.style['height'] = '100%';
                    span.style['line-height'] = 'inherit';
                    span.style['text-align'] = select.style['text-align'] || 'center';
                    span.style['pointer-events'] = 'none';
                    span.setAttribute('data-readonly', 'true');
                    // Store ranges for value→text/color mapping
                    const rangesData = (ga.property?.ranges || []).map(r => ({
                        value: String(r.min),
                        text: r.text || String(r.min),
                        bg: r.color || '',
                        fg: r.stroke || ''
                    }));
                    span.setAttribute('data-ranges', JSON.stringify(rangesData));
                    span.textContent = '';
                    select.parentNode.replaceChild(span, select);
                    return span;
                }
                select.style['appearance'] = 'menulist';
                let align = select.style['text-align'];
                if (align) {
                    select.style['text-align-last'] = align;
                }
                select.innerHTML = '';
                if (!isview) {
                    let option = document.createElement('option');
                    option.disabled = true;
                    option.selected = true;
                    option.innerHTML = 'Choose...';
                    select.appendChild(option);
                } else {
                    ga.property?.ranges?.forEach(element => {
                        let option = document.createElement('option');
                        option.value = element.min;
                        if (element.text) {
                            option.text = element.text;
                        }
                        select.appendChild(option);
                    });
                }
            }
        }
        return select;
    }

    static initElementColor(bkcolor, color, ele) {
        let element = Utils.searchTreeStartWith(ele, this.prefix);
        if (element) {
            if (bkcolor) {
                element.style.backgroundColor = bkcolor;
            }
            if (color) {
                element.style.color = color;
            }
        }
    }

    static getFillColor(ele) {
        if (ele.children && ele.children[0]) {
            let element = Utils.searchTreeStartWith(ele, this.prefix);
            if (element) {
                return element.style.backgroundColor;
            }
        }
        return ele.getAttribute('fill');
    }

    static getStrokeColor(ele) {
        if (ele.children && ele.children[0]) {
            let element = Utils.searchTreeStartWith(ele, this.prefix);
            if (element) {
                return element.style.color;
            }
        }
        return ele.getAttribute('stroke');
    }

    static processAction(act: GaugeAction, svgele: any, select: any, value: any, gaugeStatus: GaugeStatus) {
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
}
