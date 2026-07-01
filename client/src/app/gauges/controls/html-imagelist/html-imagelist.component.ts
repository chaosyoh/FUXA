import { Component } from '@angular/core';
import { GaugeBaseComponent } from '../../gauge-base/gauge-base.component';
import { GaugeSettings, Variable, GaugeStatus, GaugeAction, GaugeActionsType } from '../../../_models/hmi';
import { Utils } from '../../../_helpers/utils';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';
import { ShapesComponent } from '../../shapes/shapes.component';

declare var SVG: any;

@Component({
    selector: 'html-imagelist',
    templateUrl: './html-imagelist.component.html',
    styleUrls: ['./html-imagelist.component.css']
})
export class HtmlImageListComponent extends GaugeBaseComponent {

    static TypeTag = 'svg-ext-own_ctrl-imagelist';
    static LabelTag = 'HtmlImageList';
    static prefixD = 'D-OXC_';

    static actionsType = {
        hide: GaugeActionsType.hide,
        show: GaugeActionsType.show,
        blink: GaugeActionsType.blink,
        stop: GaugeActionsType.stop,
        enable: GaugeActionsType.enable,
        disable: GaugeActionsType.disable
    };

    constructor() {
        super();
    }

    static getDialogType(): GaugeDialogType {
        return GaugeDialogType.ImageList;
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

    static getActions(type: string) {
        return this.actionsType;
    }

    static isBitmaskSupported(): boolean {
        return true;
    }

    static initElement(ga: GaugeSettings, isview: boolean): HTMLElement {
        let ele = document.getElementById(ga.id);
        if (ele) {
            ele?.setAttribute('data-name', ga.name);
            // Make background transparent for PNG images with alpha
            let bgRect = ele.querySelector('rect');
            if (bgRect) {
                bgRect.setAttribute('fill', 'none');
            }
            let container = Utils.searchTreeStartWith(ele, this.prefixD);
            if (container) {
                container.innerHTML = '';
                let image = document.createElement('img');
                image.style['width'] = '100%';
                image.style['height'] = '100%';
                image.style['border'] = 'none';
                image.style['object-fit'] = 'contain';
                // Show first image as default
                if (ga.property && ga.property.ranges && ga.property.ranges.length > 0) {
                    let defaultRange = ga.property.ranges[0];
                    if (defaultRange.address) {
                        image.setAttribute('src', defaultRange.address);
                    }
                    if (defaultRange.text) {
                        image.setAttribute('title', defaultRange.text);
                    }
                }
                container.appendChild(image);
            }
        }
        return ele;
    }

    static detectChange(gab: GaugeSettings, isview: boolean): HTMLElement {
        return HtmlImageListComponent.initElement(gab, isview);
    }

    static processValue(ga: GaugeSettings, svgele: any, sig: Variable, gaugeStatus: GaugeStatus) {
        try {
            if (svgele.node) {
                let value = parseFloat(sig.value);
                if (Number.isNaN(value)) {
                    value = Number(sig.value);
                } else {
                    value = parseFloat(value.toFixed(5));
                }
                if (ga.property) {
                    // Match value to image
                    if (ga.property.variableId === sig.id && ga.property.ranges) {
                        let container = Utils.searchTreeStartWith(svgele.node, HtmlImageListComponent.prefixD);
                        if (container) {
                            let img = container.querySelector('img');
                            if (img) {
                                let matched = ga.property.ranges.find(r => r.min == value);
                                if (matched && matched.address) {
                                    img.setAttribute('src', matched.address);
                                    if (matched.text) {
                                        img.setAttribute('title', matched.text);
                                    }
                                } else if (ga.property.ranges.length > 0 && ga.property.ranges[0].address) {
                                    // Fallback to first image (default)
                                    img.setAttribute('src', ga.property.ranges[0].address);
                                    if (ga.property.ranges[0].text) {
                                        img.setAttribute('title', ga.property.ranges[0].text);
                                    }
                                }
                            }
                        }
                    }
                    // Process actions
                    if (ga.property.actions) {
                        ga.property.actions.forEach(act => {
                            if (act.variableId === sig.id) {
                                ShapesComponent.processAction(act, svgele, value, gaugeStatus);
                            }
                        });
                    }
                }
            }
        } catch (err) {
            console.error(err);
        }
    }
}
