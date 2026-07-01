import { Injectable, ViewContainerRef, ComponentFactoryResolver } from '@angular/core';

import { GaugeSettings, GaugeStatus, GaugeAlarmListProperty, AlarmListOptions } from '../../../_models/hmi';
import { Utils } from '../../../_helpers/utils';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';
import { AlarmListGaugeComponent } from './alarm-list/alarm-list.component';

@Injectable()
export class HtmlAlarmListComponent {
    static TypeTag = 'svg-ext-own_ctrl-alarm-list';
    static LabelTag = 'AlarmList';
    static prefixD = 'D-OXC_';

    static getSignals(): string[] {
        return null;
    }

    static getDialogType(): GaugeDialogType {
        return GaugeDialogType.AlarmList;
    }

    static processValue(): void {
        // Self-polling component, no external signal processing needed
    }

    static initElement(gab: GaugeSettings, resolver: ComponentFactoryResolver, viewContainerRef: ViewContainerRef, isview: boolean): AlarmListGaugeComponent {
        let ele = document.getElementById(gab.id);
        if (ele) {
            ele?.setAttribute('data-name', gab.name);
            let htmlContainer = Utils.searchTreeStartWith(ele, this.prefixD);
            if (htmlContainer) {
                let factory = resolver.resolveComponentFactory(AlarmListGaugeComponent);
                const componentRef = viewContainerRef.createComponent(factory);

                if (!gab.property) {
                    gab.property = <GaugeAlarmListProperty>{
                        groups: [],
                        columns: ['ontime', 'text', 'type', 'group', 'status', 'ack'],
                        options: AlarmListGaugeComponent.DefaultOptions()
                    };
                }

                htmlContainer.innerHTML = '';
                (<AlarmListGaugeComponent>componentRef.instance).isEditor = !isview;
                (<AlarmListGaugeComponent>componentRef.instance).property = gab.property;
                (<AlarmListGaugeComponent>componentRef.instance).id = gab.id;

                componentRef.changeDetectorRef.detectChanges();
                htmlContainer.appendChild(componentRef.location.nativeElement);

                componentRef.instance['myComRef'] = componentRef;
                componentRef.instance['name'] = gab.name;
                return componentRef.instance;
            }
        }
        return null;
    }

    static detectChange(gab: GaugeSettings, res: any, ref: any): AlarmListGaugeComponent {
        return HtmlAlarmListComponent.initElement(gab, res, ref, false);
    }
}
