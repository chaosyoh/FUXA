import { Component, DoCheck, EventEmitter, Input, IterableDiffer, IterableDiffers, OnChanges, Output, SimpleChanges } from '@angular/core';
import { View, ViewType, ViewFolder } from '../../_models/hmi';
import { TranslateService } from '@ngx-translate/core';
import { ConfirmDialogComponent, ConfirmDialogData } from '../../gui-helpers/confirm-dialog/confirm-dialog.component';
import { MatDialog as MatDialog } from '@angular/material/dialog';
import { ProjectService } from '../../_services/project.service';
import { ViewPropertyComponent, ViewPropertyType } from '../view-property/view-property.component';
import * as FileSaver from 'file-saver';
import { EditNameComponent, EditNameData } from '../../gui-helpers/edit-name/edit-name.component';
import { Utils } from '../../_helpers/utils';

@Component({
    selector: 'app-editor-views-list',
    templateUrl: './editor-views-list.component.html',
    styleUrls: ['./editor-views-list.component.scss']
})
export class EditorViewsListComponent implements OnChanges, DoCheck {

    @Input() views: View[] = [];
    @Input() viewFolders: ViewFolder[] = [];
    @Input('select') set select(view: View) {
        this.currentView = view;
    };
    @Output() selected: EventEmitter<View> = new EventEmitter<View>();
    @Output() viewPropertyChanged: EventEmitter<View> = new EventEmitter<View>();
    @Output() cloneView: EventEmitter<View> = new EventEmitter<View>();
    @Output() viewFoldersChanged: EventEmitter<ViewFolder[]> = new EventEmitter<ViewFolder[]>();

    currentView: View = null;
    expandedFolders: Set<string> = new Set();

    /** Search keyword for filtering views by name (case-insensitive substring match). Empty = no filter. */
    searchText = '';

    cardViewType = ViewType.cards;
    svgViewType = ViewType.svg;
    mapsViewType = ViewType.maps;

    //#region Pre-computed caches (recalculated only when inputs / search change)
    /** Folders that have no parent, sorted */
    rootFolders: ViewFolder[] = [];
    /** parentId -> sorted child folders */
    childFoldersMap: Map<string, ViewFolder[]> = new Map();
    /** folderId (or '__root__' for null) -> sorted views in that folder */
    viewsByFolderMap: Map<string, View[]> = new Map();
    /** Set of folder ids that contain (recursively) a matching view (only while searching) */
    matchingFolderIds: Set<string> = new Set();
    private readonly ROOT_KEY = '__root__';
    //#endregion

    private viewsDiffer: IterableDiffer<View>;
    private foldersDiffer: IterableDiffer<ViewFolder>;

    constructor(private projectService: ProjectService,
        private translateService: TranslateService,
        public dialog: MatDialog,
        private iterableDiffers: IterableDiffers,
    ) {
        this.viewsDiffer = this.iterableDiffers.find([]).create<View>((_, v) => v?.id);
        this.foldersDiffer = this.iterableDiffers.find([]).create<ViewFolder>((_, f) => f?.id);
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes['views'] || changes['viewFolders']) {
            this.rebuildCaches();
        }
    }

    /**
     * Detects in-place mutations (push/splice) on @Input arrays whose reference does not change,
     * so the pre-computed caches stay in sync without us forcing OnPush on parent.
     */
    ngDoCheck(): void {
        const viewsChanged = !!this.viewsDiffer.diff(this.views);
        const foldersChanged = !!this.foldersDiffer.diff(this.viewFolders);
        if (viewsChanged || foldersChanged) {
            this.rebuildCaches();
        }
    }

    //#region Search
    onSearchChange(value: string) {
        this.searchText = (value || '').trim();
        this.rebuildCaches();
    }

    onClearSearch() {
        this.searchText = '';
        this.rebuildCaches();
    }

    /** Whether the search filter is currently active */
    private get isSearching(): boolean {
        return !!this.searchText;
    }

    private viewMatches(view: View, needle: string): boolean {
        if (!needle) { return true; }
        return (view?.name || '').toLowerCase().includes(needle);
    }
    //#endregion

    //#region Cache builder
    /**
     * Build all derived structures from `views` / `viewFolders` / `searchText` in a single pass.
     * Called only on input change or search change, so the template can read pre-computed fields
     * directly without triggering filter/sort on every change-detection cycle.
     */
    private rebuildCaches(): void {
        const folders = this.viewFolders || [];
        const views = this.views || [];
        const needle = this.searchText.toLowerCase();
        const folderSort = (a: ViewFolder, b: ViewFolder) =>
            (a.order || 0) - (b.order || 0) || (a.name || '').localeCompare(b.name || '');
        const viewSort = (a: View, b: View) => (a.name || '').localeCompare(b.name || '');

        // Group folders by parentId
        const childFoldersMap = new Map<string, ViewFolder[]>();
        const rootFolders: ViewFolder[] = [];
        for (const f of folders) {
            if (!f.parentId) {
                rootFolders.push(f);
            } else {
                const arr = childFoldersMap.get(f.parentId);
                if (arr) { arr.push(f); } else { childFoldersMap.set(f.parentId, [f]); }
            }
        }
        rootFolders.sort(folderSort);
        childFoldersMap.forEach(arr => arr.sort(folderSort));

        // Group views by folderId
        const viewsByFolderMap = new Map<string, View[]>();
        for (const v of views) {
            if (needle && !this.viewMatches(v, needle)) { continue; }
            const key = v.folderId || this.ROOT_KEY;
            const arr = viewsByFolderMap.get(key);
            if (arr) { arr.push(v); } else { viewsByFolderMap.set(key, [v]); }
        }
        viewsByFolderMap.forEach(arr => arr.sort(viewSort));

        // Compute matching folder set when searching
        const matchingFolderIds = new Set<string>();
        if (needle) {
            // Mark folders that directly contain matching views
            const directHit = new Set<string>();
            viewsByFolderMap.forEach((_, key) => {
                if (key !== this.ROOT_KEY) { directHit.add(key); }
            });
            // Bubble up to ancestors
            const folderById = new Map(folders.map(f => [f.id, f] as [string, ViewFolder]));
            const propagate = (id: string) => {
                if (matchingFolderIds.has(id)) { return; }
                matchingFolderIds.add(id);
                const parent = folderById.get(id)?.parentId;
                if (parent) { propagate(parent); }
            };
            directHit.forEach(id => propagate(id));
        }

        // Initialise expandedFolders from folder.expanded flag (preserve user toggles)
        for (const f of folders) {
            if (f.expanded !== false && !this.expandedFolders.has(f.id)) {
                this.expandedFolders.add(f.id);
            }
        }

        this.rootFolders = needle ? rootFolders.filter(f => matchingFolderIds.has(f.id)) : rootFolders;
        if (needle) {
            const filteredChild = new Map<string, ViewFolder[]>();
            childFoldersMap.forEach((arr, parentId) => {
                const kept = arr.filter(f => matchingFolderIds.has(f.id));
                if (kept.length) { filteredChild.set(parentId, kept); }
            });
            this.childFoldersMap = filteredChild;
        } else {
            this.childFoldersMap = childFoldersMap;
        }
        this.viewsByFolderMap = viewsByFolderMap;
        this.matchingFolderIds = matchingFolderIds;
    }

    /** Public accessors used by template */
    getChildFolders(parentId: string): ViewFolder[] {
        return this.childFoldersMap.get(parentId) || [];
    }
    getViewsInFolder(folderId: string | null): View[] {
        return this.viewsByFolderMap.get(folderId || this.ROOT_KEY) || [];
    }
    //#endregion

    //#region Tree structure helpers
    isFolderExpanded(folder: ViewFolder): boolean {
        if (this.isSearching) {
            // While searching always expand folders that contain results
            return this.matchingFolderIds.has(folder.id);
        }
        return this.expandedFolders.has(folder.id);
    }

    toggleFolder(folder: ViewFolder, event?: MouseEvent) {
        if (event) {
            event.stopPropagation();
        }
        if (this.expandedFolders.has(folder.id)) {
            this.expandedFolders.delete(folder.id);
            folder.expanded = false;
        } else {
            this.expandedFolders.add(folder.id);
            folder.expanded = true;
        }
        this.viewFoldersChanged.emit(this.viewFolders);
    }

    hasChildren(folder: ViewFolder): boolean {
        return this.getChildFolders(folder.id).length > 0 || this.getViewsInFolder(folder.id).length > 0;
    }

    /** trackBy for *ngFor — keeps DOM stable across re-renders */
    trackByFolderId = (_: number, f: ViewFolder) => f?.id;
    trackByViewId = (_: number, v: View) => v?.id;
    //#endregion

    //#region Folder CRUD
    onCreateSubfolder(parentFolder: ViewFolder) {
        let existNames = (this.viewFolders || []).map(f => f.name);
        let dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData>{
                title: this.translateService.instant('editor.folder-add-subfolder'),
                name: '',
                exist: existNames
            }
        });
        dialogRef.afterClosed().subscribe(result => {
            if (result && result.name) {
                let folder = new ViewFolder();
                folder.id = Utils.getShortGUID('vf_');
                folder.name = result.name;
                folder.parentId = parentFolder.id;
                this.viewFolders.push(folder);
                this.expandedFolders.add(parentFolder.id);
                parentFolder.expanded = true;
                this.rebuildCaches();
                this.viewFoldersChanged.emit(this.viewFolders);
            }
        });
    }

    onRenameFolder(folder: ViewFolder) {
        let existNames = (this.viewFolders || []).filter(f => f.id !== folder.id).map(f => f.name);
        let dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData>{
                title: this.translateService.instant('editor.folder-rename'),
                name: folder.name,
                exist: existNames
            }
        });
        dialogRef.afterClosed().subscribe(result => {
            if (result && result.name) {
                folder.name = result.name;
                this.rebuildCaches();
                this.viewFoldersChanged.emit(this.viewFolders);
            }
        });
    }

    onDeleteFolder(folder: ViewFolder) {
        let dialogRef = this.dialog.open(ConfirmDialogComponent, {
            position: { top: '60px' },
            data: <ConfirmDialogData>{ msg: this.translateService.instant('msg.folder-remove', { value: folder.name }) }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result) {
                // Move child views to parent folder
                let viewsInFolder = this.views.filter(v => v.folderId === folder.id);
                for (let view of viewsInFolder) {
                    view.folderId = folder.parentId || undefined;
                    this.projectService.setView(view, false);
                }
                // Move child folders to parent
                let childFolders = this.viewFolders.filter(f => f.parentId === folder.id);
                for (let child of childFolders) {
                    child.parentId = folder.parentId;
                }
                // Remove the folder
                let idx = this.viewFolders.indexOf(folder);
                if (idx >= 0) {
                    this.viewFolders.splice(idx, 1);
                }
                this.rebuildCaches();
                this.viewFoldersChanged.emit(this.viewFolders);
            }
        });
    }
    //#endregion

    //#region View operations (existing)
    onSelectView(view: View, force = true) {
        if (!force && this.currentView?.id === view?.id) {
            return;
        }
        this.currentView = view;
        this.selected.emit(this.currentView);
    }

    isViewActive(view: View) {
        return !!view && this.currentView?.id === view.id;
    }

    onDeleteView(view) {
        let dialogRef = this.dialog.open(ConfirmDialogComponent, {
            position: { top: '60px' },
            data: <ConfirmDialogData>{ msg: this.translateService.instant('msg.view-remove', { value: view.name }) }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result && this.views) {
                let toselect = null;
                for (var i = 0; i < this.views.length; i++) {
                    if (this.views[i].id === view.id) {
                        this.views.splice(i, 1);
                        if (i > 0 && i < this.views.length) {
                            toselect = this.views[i];
                        }
                        break;
                    }
                }
                this.currentView = null;
                if (toselect) {
                    this.onSelectView(toselect);
                } else if (this.views.length > 0) {
                    this.onSelectView(this.views[0]);
                }
                this.rebuildCaches();
                this.projectService.removeView(view);
            }
        });
    }

    onRenameView(view) {
        let exist = this.views.filter((v) => v.id !== view.id).map((v) => v.name);
        let dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData>{
                title: this.translateService.instant('dlg.docname-title'),
                name: view.name,
                exist: exist
            }
        });
        dialogRef.afterClosed().subscribe(result => {
            if (result && result.name) {
                view.name = result.name;
                this.rebuildCaches();
                this.projectService.setView(view, false);
            }
        });
    }

    onPropertyView(view) {
        let dialogRef = this.dialog.open(ViewPropertyComponent, {
            position: { top: '60px' },
            disableClose: true,
            data: <ViewPropertyType>{
                name: view.name,
                type: view.type || ViewType.svg,
                profile: view.profile,
                property: view.property,
                folderId: view.folderId,
                viewFolders: this.viewFolders || []
            }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result?.profile) {
                if (result.profile.height) { view.profile.height = parseInt(result.profile.height); }
                if (result.profile.width) { view.profile.width = parseInt(result.profile.width); }
                if (result.profile.margin >= 0) { view.profile.margin = parseInt(result.profile.margin); }
                view.profile.bkcolor = result.profile.bkcolor;
                if (result.property?.events) {
                    view.property ??= { events: [], actions: [] };
                    view.property.events = result.property.events;
                }
                view.folderId = result.folderId;
                this.rebuildCaches();
                this.viewPropertyChanged.emit(view);
                this.onSelectView(view);
            }
        });
    }

    onCloneView(view: View) {
        this.cloneView.emit(view);
    }

    onExportView(view: View) {
        let filename = `${view.name}.json`;
        let content = JSON.stringify(view);
        let blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
        FileSaver.saveAs(blob, filename);
    }

    onCleanView(view: View) {
        const changed = this.projectService.cleanView(view);
        if (changed) {
            this.onSelectView(view);
        }
    }
    //#endregion
}
