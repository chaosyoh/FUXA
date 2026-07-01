import { Component, ComponentFactoryResolver, ViewContainerRef } from '@angular/core';
import { GaugeAction, GaugeActionsType, GaugeSettings, GaugeStatus, Hmi, Variable } from '../../../_models/hmi';
import { GaugeBaseComponent } from '../../gauge-base/gauge-base.component';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';
import { Utils } from '../../../_helpers/utils';
import { FuxaViewComponent } from '../../../fuxa-view/fuxa-view.component';
import { GaugesManager } from '../../gauges.component';
import { ProjectService } from '../../../_services/project.service';

declare var SVG: any;

@Component({
    selector: 'app-panel',
    templateUrl: './panel.component.html',
    styleUrls: ['./panel.component.css']
})
export class PanelComponent extends GaugeBaseComponent {
    static TypeTag = 'svg-ext-own_ctrl-panel';
    static LabelTag = 'Panel';
    static prefixD = 'D-OXC_';

    static actionsType = { hide: GaugeActionsType.hide, show: GaugeActionsType.show, enable: GaugeActionsType.enable, disable: GaugeActionsType.disable };
    static hmi: Hmi;

    constructor(private projectService: ProjectService) {
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
        return GaugeDialogType.Panel;
    }

    static processValue(ga: GaugeSettings, svgele: any, sig: Variable, gaugeStatus: GaugeStatus, gauge?: FuxaViewComponent) {
        try {
            if (ga.property?.variableId === sig.id) {
                const view = PanelComponent.hmi.views.find(x => x.name === sig.value);
                if (view) {
                    gauge?.loadHmi(view, true);
                    if (ga?.property?.scaleMode) {
                        Utils.resizeViewExt('.view-container', ga?.id, ga?.property?.scaleMode);
                    }
                }
            }
            if (ga.property?.actions) {
                ga.property.actions.forEach(act => {
                    if (act.variableId === sig.id) {
                        let value = parseFloat(sig.value);
                        if (Number.isNaN(value)) {
                            value = Number(sig.value);
                        }
                        PanelComponent.processAction(act, svgele, value, gaugeStatus);
                    }
                });
            }
        } catch (err) {
            console.error(err);
        }
    }

    static processAction(act: GaugeAction, svgele: any, value: any, gaugeStatus: GaugeStatus) {
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

    static initElement(gaugeSettings: GaugeSettings,
                       resolver: ComponentFactoryResolver,
                       viewContainerRef: ViewContainerRef,
                       gaugeManager: GaugesManager,
                       hmi: Hmi,
                       isview?: boolean,
                       parent?: FuxaViewComponent): FuxaViewComponent {
        if (hmi) {
            PanelComponent.hmi = hmi;
        }
        let ele = document.getElementById(gaugeSettings.id);
        if (ele) {
            ele?.setAttribute('data-name', gaugeSettings.name);
            let svgPanelContainer = Utils.searchTreeStartWith(ele, this.prefixD);
            if (svgPanelContainer) {
                const factory = resolver.resolveComponentFactory(FuxaViewComponent);
                const componentRef = viewContainerRef.createComponent(factory);
                componentRef.instance.gaugesManager = gaugeManager;

                componentRef.changeDetectorRef.detectChanges();
                const loaderComponentElement = componentRef.location.nativeElement;
                svgPanelContainer.innerHTML = '';
                svgPanelContainer.appendChild(loaderComponentElement);

                componentRef.instance['myComRef'] = componentRef;
                componentRef.instance.parent = parent;
                if (!isview) {
                    let span = document.createElement('span');
                    span.innerHTML = 'Panel';
                    svgPanelContainer.appendChild(span);
                    return null;
                }
                PanelComponent.processValue(gaugeSettings,
                                            null,
                                            <Variable> {
                                                value: gaugeSettings.property.viewName
                                            },
                                            null,
                                            componentRef.instance);
                componentRef.instance['name'] = gaugeSettings.name;
                return componentRef.instance;
            }
        }
    }
}
