import { Component } from '@angular/core';
import { GaugeBaseComponent } from '../../gauge-base/gauge-base.component';
import { GaugeDialogType } from '../../gauge-property/gauge-property.component';
import { GaugeAction, GaugeActionsType, GaugeSettings, GaugeStatus, Variable } from '../../../_models/hmi';
import { Utils } from '../../../_helpers/utils';
import { EndPointApi } from '../../../_helpers/endpointapi';
import mpegts from 'mpegts.js';

@Component({
    selector: 'app-html-video',
    templateUrl: './html-video.component.html',
    styleUrls: ['./html-video.component.css']
})
export class HtmlVideoComponent extends GaugeBaseComponent {
    static TypeTag = 'svg-ext-own_ctrl-video';
    static LabelTag = 'HtmlVideo';
    static prefixD = 'D-OXC_';

    static actionsType = {
        stop: GaugeActionsType.stop,
        start: GaugeActionsType.start,
        pause: GaugeActionsType.pause,
        reset: GaugeActionsType.reset,
        enable: GaugeActionsType.enable,
        disable: GaugeActionsType.disable
    };

    /** Active mpegts.js player instances keyed by gauge ID */
    private static flvPlayers = new Map<string, any>();
    /** Current URL per gauge (needed because mpegts.js turns video.currentSrc into a blob: URL) */
    private static currentUrls = new Map<string, string>();
    /** Per-gauge FLV config for reconnect (reset action) */
    private static flvConfigs = new Map<string, { url: string; disableAudio: boolean }>();

    /** Whether fullscreen CSS has been injected */
    private static fsStyleInjected = false;

    constructor() {
        super();
    }

    static getSignals(pro: any) {
        let res: string[] = [];
        if (pro.variableId) {
            res.push(pro.variableId);
        }
        if (pro.actions) {
            pro.actions.forEach(act => {
                res.push(act.variableId);
            });
        }
        return res;
    }

    static getActions(type: string) {
        return this.actionsType;
    }

    static getDialogType(): GaugeDialogType {
        return GaugeDialogType.Video;
    }

    static processValue(ga: GaugeSettings, svgele: any, sig: Variable, gaugeStatus?: GaugeStatus) {
        try {
            if (svgele?.node?.children?.length >= 1) {
                const parentIframe = Utils.searchTreeStartWith(svgele.node, this.prefixD);
                const video = parentIframe.querySelector('video');
                if (!video) {
                    return;
                }
                if (sig.id === ga.property.variableId) {
                    const newSrc = EndPointApi.resolveUrl(String(sig.value ?? '').trim());
                    const lastUrl = HtmlVideoComponent.currentUrls.get(ga.id);
                    const same = newSrc === lastUrl;
                    if (!same) {
                        video.pause();
                        const streamType = ga.property?.options?.streamType ?? 'auto';
                        const disableAudio = !!ga.property?.options?.disableAudio;
                        HtmlVideoComponent.setupVideoSource(video, newSrc, ga.id, streamType, disableAudio);
                    }
                    const image = parentIframe.querySelector('img');
                    if (image) {
                        if (sig.value) {
                            image.style.display = 'none';
                        } else {
                            image.style.display = 'block';
                        }
                    }
                } else {
                    let value = Utils.toFloatOrNumber(sig.value);
                    if (ga.property.actions) {
                        ga.property.actions.forEach(act => {
                            if (act.variableId === sig.id) {
                                HtmlVideoComponent.processAction(act, svgele, video, value, gaugeStatus, ga.id);
                            }
                        });
                    }
                }
            }
        } catch (err) {
            console.error(err);
        }
    }

    static initElement(gaugeSettings: GaugeSettings, isView: boolean = false): HTMLElement {
        let ele = document.getElementById(gaugeSettings.id);
        if (!ele) {
            return null;
        }
        ele?.setAttribute('data-name', gaugeSettings.name);
        let svgVideoContainer = Utils.searchTreeStartWith(ele, this.prefixD);
        if (svgVideoContainer) {
            svgVideoContainer.innerHTML = '';
            const options = gaugeSettings.property?.options;
            const rawSrc  = options?.address;
            const initImage = options?.initImage;
            const videoSrc = EndPointApi.resolveUrl(rawSrc);
            const hasVideo = !!rawSrc;

            // Apply container border styles (consistent with alarm-list control)
            svgVideoContainer.style.overflow = 'hidden';
            svgVideoContainer.style.boxSizing = 'border-box';
            if (options?.showBorder) {
                svgVideoContainer.style.border = '1px solid #e0e0e0';
                svgVideoContainer.style.borderRadius = '4px';
                svgVideoContainer.style.boxShadow = '0 2px 6px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.06)';
                svgVideoContainer.style.background = '#fff';
            } else {
                svgVideoContainer.style.border = '';
                svgVideoContainer.style.borderRadius = '';
                svgVideoContainer.style.boxShadow = '';
                svgVideoContainer.style.background = '';
            }

            if (initImage && !hasVideo) {
                const img = document.createElement('img');
                img.src = initImage;
                img.style.width = '100%';
                img.style.height = '100%';
                img.style.objectFit = 'contain';
                svgVideoContainer.appendChild(img);
            }
            let video = document.createElement('video');
            video.setAttribute('playsinline', 'true');
            video.muted = true;
            video.style.width = '100%';
            video.style.height = '100%';
            video.style.objectFit = 'contain';
            video.style.display = 'block';
            const showControls = !!gaugeSettings.property?.options?.showControls;
            if (showControls) {
                video.setAttribute('controls', '');
            }

            // For FLV live streams with native controls, hide progress bar and time display
            const streamType = options?.streamType ?? 'auto';
            const isLiveStream = streamType === 'flv' ||
                (streamType === 'auto' && hasVideo && this.isFlvUrl(videoSrc));
            if (isLiveStream && showControls) {
                video.classList.add('live-stream');
            }

            // Wrapper for fullscreen support (position: relative anchor)
            const wrapper = document.createElement('div');
            wrapper.style.position = 'relative';
            wrapper.style.width = '100%';
            wrapper.style.height = '100%';
            wrapper.style.overflow = 'hidden';
            wrapper.appendChild(video);

            if (isView) {
                HtmlVideoComponent.ensureFullscreenStyle();
                // Only add custom fullscreen button when native controls are hidden
                if (!showControls) {
                    HtmlVideoComponent.addFullscreenButton(wrapper, video);
                }
            }

            svgVideoContainer.appendChild(wrapper);
            if (hasVideo && isView) {
                const disableAudio = !!options?.disableAudio;
                HtmlVideoComponent.setupVideoSource(video, videoSrc, gaugeSettings.id, streamType, disableAudio);
            }
        }
        return svgVideoContainer;
    }

    static processAction(act: GaugeAction, svgele: any, video: any, value: any, gaugeStatus?: GaugeStatus, gaugeId?: string) {
        let actValue = GaugeBaseComponent.checkBitmask(act.bitmask, value);
        if (this.actionsType[act.type] === this.actionsType.start) {
            if (act.range.min <= actValue && act.range.max >= actValue) {
                video.play().catch(err => console.error('Video play failed:', err));
            }
        } else if (this.actionsType[act.type] === this.actionsType.pause) {
            if (act.range.min <= actValue && act.range.max >= actValue) {
                video.pause();
            }
        } else if (this.actionsType[act.type] === this.actionsType.stop) {
            if (act.range.min <= actValue && act.range.max >= actValue) {
                video.pause();
            }
        } else if (this.actionsType[act.type] === this.actionsType.reset) {
            if (act.range.min <= actValue && act.range.max >= actValue) {
                if (gaugeId && this.flvPlayers.has(gaugeId)) {
                    // FLV live stream: destroy and recreate to reconnect to live edge
                    const config = this.flvConfigs.get(gaugeId);
                    this.destroyFlvPlayer(gaugeId);
                    if (config) {
                        const newPlayer = this.createFlvPlayer(video, config.url, config.disableAudio);
                        if (newPlayer) {
                            this.flvPlayers.set(gaugeId, newPlayer);
                            this.flvConfigs.set(gaugeId, config);
                            newPlayer.play().catch(err => console.error('FLV play after reset failed:', err));
                        }
                    }
                } else {
                    video.pause();
                    video.currentTime = 0;
                }
            }
        } else if (this.actionsType[act.type] === this.actionsType.disable) {
            if (gaugeStatus) {
                if (act.range.min <= actValue && act.range.max >= actValue) {
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
            }
        } else if (this.actionsType[act.type] === this.actionsType.enable) {
            if (act.range.min <= actValue && act.range.max >= actValue && gaugeStatus) {
                gaugeStatus.disabled = false;
                svgele.node.style.pointerEvents = '';
                svgele.node.style.opacity = '1';
                svgele.node.style.cursor = '';
            }
        }
    }

    static detectChange(gaugeSettings: GaugeSettings, res: any, ref: any) {
        return HtmlVideoComponent.initElement(gaugeSettings, false);
    }

    static getMimeTypeFromUrl(url: string): string {
        const ext = url.split('.').pop()?.toLowerCase();
        switch (ext) {
            case 'mp4': return 'video/mp4';
            case 'webm': return 'video/webm';
            case 'ogg':
            case 'ogv': return 'video/ogg';
            case 'flv': return 'video/x-flv';
            default: return 'video/mp4';
        }
    }

    /**
     * Determines whether the given URL points to an FLV stream.
     * Checks pathname ending with .flv or ws/wss protocol.
     */
    static isFlvUrl(url: string): boolean {
        if (!url) { return false; }
        try {
            const parsed = new URL(url, window.location.origin);
            if (parsed.pathname.endsWith('.flv')) { return true; }
            if (parsed.protocol === 'ws:' || parsed.protocol === 'wss:') { return true; }
            return false;
        } catch {
            return url.toLowerCase().includes('.flv');
        }
    }

    /**
     * Creates and returns an mpegts.js player attached to the given video element.
     */
    private static createFlvPlayer(video: HTMLVideoElement, url: string, disableAudio: boolean = false): any {
        if (!mpegts.isSupported()) {
            console.error('[html-video] mpegts.js: MediaSource API is not supported in this browser');
            return null;
        }
        const player = mpegts.createPlayer(
            { type: 'flv', url, isLive: true, hasAudio: !disableAudio },
            {
                enableWorker: false,
                enableStashBuffer: true,
                stashInitialSize: 384,
                autoCleanupSourceBuffer: true,
                autoCleanupMaxBackwardDuration: 30,
                autoCleanupMinBackwardDuration: 15,
                liveBufferLatencyChasing: true,
            }
        );
        player.attachMediaElement(video);
        player.load();
        player.on(mpegts.Events.ERROR, (errorType, errorDetail, errorInfo) => {
            console.error(`[html-video] FLV error: type=${errorType}, detail=${errorDetail}`, errorInfo);
        });
        return player;
    }

    /**
     * Safely destroys an mpegts.js player instance for the given gauge ID.
     */
    private static destroyFlvPlayer(gaugeId: string): void {
        const existing = this.flvPlayers.get(gaugeId);
        if (existing) {
            try {
                existing.pause();
                existing.unload();
                existing.detachMediaElement();
                existing.destroy();
            } catch (e) {
                console.warn('[html-video] Error destroying FLV player:', e);
            }
            this.flvPlayers.delete(gaugeId);
        }
        this.currentUrls.delete(gaugeId);
        this.flvConfigs.delete(gaugeId);
    }

    /**
     * Sets up video playback for the given URL. Uses mpegts.js for FLV streams,
     * or standard HTML5 <source> element for regular video files.
     * Tears down any existing FLV player before setting up the new source.
     */
    private static setupVideoSource(video: HTMLVideoElement, url: string, gaugeId: string, streamType: 'auto' | 'flv' | 'standard' = 'auto', disableAudio: boolean = false): void {
        // Always tear down any existing FLV player first
        this.destroyFlvPlayer(gaugeId);

        // Clear existing <source> children
        while (video.firstChild) {
            video.removeChild(video.firstChild);
        }
        video.removeAttribute('src');

        if (!url) {
            return;
        }

        const useFlv = streamType === 'flv' || (streamType === 'auto' && this.isFlvUrl(url));

        if (useFlv) {
            const flvPlayer = this.createFlvPlayer(video, url, disableAudio);
            if (flvPlayer) {
                this.flvPlayers.set(gaugeId, flvPlayer);
                this.currentUrls.set(gaugeId, url);
                this.flvConfigs.set(gaugeId, { url, disableAudio });
            }
        } else {
            const source = document.createElement('source');
            source.src = url;
            source.type = this.getMimeTypeFromUrl(url);
            video.appendChild(source);
            video.load();
            this.currentUrls.set(gaugeId, url);
        }
    }

    /**
     * Injects the fullscreen button CSS once into the document head.
     */
    private static ensureFullscreenStyle(): void {
        if (this.fsStyleInjected) { return; }
        const style = document.createElement('style');
        style.textContent = `
            .html-video-fs-btn {
                position: absolute;
                top: 6px;
                right: 6px;
                width: 32px;
                height: 32px;
                border-radius: 50%;
                background: rgba(0,0,0,0.55);
                color: #fff;
                border: none;
                cursor: pointer;
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 10;
                padding: 0;
                transition: background 0.2s;
            }
            .html-video-fs-btn:hover {
                background: rgba(0,0,0,0.75);
            }
            .html-video-fs-btn svg {
                width: 20px;
                height: 20px;
                fill: #fff;
            }
            /* Hide progress bar and time for live streams with native controls */
            video.live-stream::-webkit-media-controls-timeline { display: none; }
            video.live-stream::-webkit-media-controls-current-time-display { display: none; }
            video.live-stream::-webkit-media-controls-time-remaining-display { display: none; }
        `;
        document.head.appendChild(style);
        this.fsStyleInjected = true;
    }

    /**
     * SVG path data for fullscreen / fullscreen-exit icons (Material Design).
     */
    private static readonly FS_ICON_ENTER =
        'M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z';
    private static readonly FS_ICON_EXIT =
        'M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z';

    /**
     * Creates the fullscreen toggle button and wires it to the video element.
     * Button is hidden by default and shown only when the video is playing.
     */
    private static addFullscreenButton(wrapper: HTMLElement, video: HTMLVideoElement): void {
        const btn = document.createElement('button');
        btn.className = 'html-video-fs-btn';
        btn.setAttribute('type', 'button');
        btn.innerHTML = `<svg viewBox="0 0 24 24"><path d="${this.FS_ICON_ENTER}"/></svg>`;

        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            HtmlVideoComponent.toggleFullscreen(wrapper, btn);
        });

        wrapper.appendChild(btn);

        // Update icon when fullscreen state changes
        const onFsChange = () => {
            const isFs = !!(document.fullscreenElement || (document as any).webkitFullscreenElement);
            const path = btn.querySelector('path');
            if (path) {
                path.setAttribute('d', isFs ? HtmlVideoComponent.FS_ICON_EXIT : HtmlVideoComponent.FS_ICON_ENTER);
            }
        };
        document.addEventListener('fullscreenchange', onFsChange);
        document.addEventListener('webkitfullscreenchange', onFsChange);

        // Show button only when video is playing
        video.addEventListener('playing', () => { btn.style.display = 'flex'; });
        video.addEventListener('pause', () => { btn.style.display = 'none'; });
        video.addEventListener('ended', () => { btn.style.display = 'none'; });
        video.addEventListener('error', () => { btn.style.display = 'none'; });

        // If video is already playing, show the button
        if (!video.paused && !video.ended && video.readyState > 0) {
            btn.style.display = 'flex';
        }
    }

    /**
     * Toggles fullscreen on the given wrapper element.
     */
    private static toggleFullscreen(wrapper: HTMLElement, btn: HTMLElement): void {
        const isFs = !!(document.fullscreenElement || (document as any).webkitFullscreenElement);
        if (isFs) {
            if (document.exitFullscreen) {
                document.exitFullscreen();
            } else if ((document as any).webkitExitFullscreen) {
                (document as any).webkitExitFullscreen();
            }
        } else {
            const el = wrapper as any;
            if (el.requestFullscreen) {
                el.requestFullscreen();
            } else if (el.webkitRequestFullscreen) {
                el.webkitRequestFullscreen();
            }
        }
    }

    /**
     * Destroys all active FLV player instances. Should be called when the view is torn down.
     */
    static destroyAll(): void {
        this.flvPlayers.forEach((player, id) => {
            try {
                player.pause();
                player.unload();
                player.detachMediaElement();
                player.destroy();
            } catch (e) { /* ignore */ }
        });
        this.flvPlayers.clear();
        this.currentUrls.clear();
        this.flvConfigs.clear();
    }
}
