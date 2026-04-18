const STATE = {
    page: 1,
    limit: 25,
    groups: [],
    totalGroups: 0,
    totalPages: 0,
    loading: false,
    resolvingPage: false,
    batchProgressText: '',
    focusedIndex: -1,
    pendingSingleClick: null,
};

const BATCH_RESOLVE_CONCURRENCY = 6;

const groupsContainer = document.getElementById('groups-container');
const loadingIndicator = document.getElementById('loading-indicator');
const prevBtn = document.getElementById('prev-page');
const nextBtn = document.getElementById('next-page');
const pageInfo = document.getElementById('page-info');
const pageNumbers = document.getElementById('page-numbers');
const pageSizeSelect = document.getElementById('page-size');
const resolvePageBtn = document.getElementById('resolve-page');
const batchStatus = document.getElementById('batch-status');
const totalGroupsEl = document.getElementById('total-groups');
const potentialSavingsEl = document.getElementById('pot-space');
const DEFAULT_PAGE = STATE.page;
const DEFAULT_LIMIT = STATE.limit;
const allowedPageSizes = new Set(
    Array.from(pageSizeSelect.options, (option) => Number.parseInt(option.value, 10))
        .filter((value) => Number.isInteger(value) && value > 0)
);

const visibleGroups = () => STATE.groups.filter(Boolean);
const allItemsForGroup = (group) => [group.master, ...group.duplicates];
const isSelected = (group, path) => group.selectedPaths.includes(path);

const parsePositiveInt = (value, fallback) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
};

const sanitizeLimit = (value) => {
    const parsed = parsePositiveInt(value, DEFAULT_LIMIT);
    return allowedPageSizes.has(parsed) ? parsed : DEFAULT_LIMIT;
};

const readPaginationFromURL = () => {
    const params = new URLSearchParams(window.location.search);
    return {
        page: parsePositiveInt(params.get('page'), DEFAULT_PAGE),
        limit: sanitizeLimit(params.get('limit')),
    };
};

const buildDuplicatesURL = (page, limit) => {
    const params = new URLSearchParams({
        page: String(page),
        limit: String(limit),
    });
    return `/api/duplicates?${params.toString()}`;
};

const buildGroupArchiveURL = (masterPath) => {
    const params = new URLSearchParams({
        masterPath,
    });
    return `/api/group-archive?${params.toString()}`;
};

const buildBrowserURL = (page, limit) => {
    const url = new URL(window.location.href);
    url.searchParams.set('page', String(page));
    url.searchParams.set('limit', String(limit));
    return `${url.pathname}?${url.searchParams.toString()}${url.hash}`;
};

const syncPaginationURL = (page, limit, historyMode = 'replace') => {
    if (historyMode === 'none') return;

    const nextURL = buildBrowserURL(page, limit);
    const currentURL = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (nextURL === currentURL) {
        return;
    }

    const historyMethod = historyMode === 'push' ? 'pushState' : 'replaceState';
    window.history[historyMethod]({ page, limit }, '', nextURL);
};

const formatBytes = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k)));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

const determineWinner = (master, duplicates) => {
    const all = [master, ...duplicates];
    all.sort((a, b) => {
        const resA = (a.width || 0) * (a.height || 0);
        const resB = (b.width || 0) * (b.height || 0);

        if (resA !== resB) return resB - resA;
        if (a.size !== b.size) return b.size - a.size;
        return a.path.localeCompare(b.path);
    });
    return all[0].path;
};

const normalizeGroup = (group) => {
    const autoSelectedPath = determineWinner(group.master, group.duplicates);
    return {
        ...group,
        autoSelectedPath,
        selectedPaths: [autoSelectedPath],
    };
};

const selectionSummary = (group) => {
    const candidateCount = allItemsForGroup(group).length;
    const selectedCount = group.selectedPaths.length;
    return `${candidateCount} candidates • ${selectedCount} kept`;
};

const buildResolvePayload = (group) => {
    const keepPaths = [...group.selectedPaths];
    const allPaths = allItemsForGroup(group).map((item) => item.path);
    return {
        keepPaths,
        keepPath: keepPaths[0],
        deletePaths: allPaths.filter((path) => !keepPaths.includes(path)),
        masterPath: group.master.path,
    };
};

const summarizeResolvePayload = (payload) => ({
    masterPath: payload.masterPath,
    keepCount: payload.keepPaths.length,
    deleteCount: payload.deletePaths.length,
    keepPaths: payload.keepPaths,
    deletePaths: payload.deletePaths,
});

const formatResolveErrorSummary = (error) => {
    const parts = [error.message || 'resolve failed'];
    if (error.requestId) {
        parts.push(`request id ${error.requestId}`);
    }
    if (Number.isInteger(error.status)) {
        parts.push(`HTTP ${error.status}`);
    }
    return parts.join(' | ');
};

const postResolve = async (group) => {
    const payload = buildResolvePayload(group);
    console.info('[resolve] request', summarizeResolvePayload(payload));

    const response = await fetch('/api/resolve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const requestId = response.headers.get('X-Resolve-Request-Id') || '';

    if (!response.ok) {
        const message = await response.text();
        const error = new Error(message || 'resolve failed');
        error.requestId = requestId;
        error.status = response.status;
        error.payload = payload;
        console.error('[resolve] failed', {
            requestId,
            status: response.status,
            ...summarizeResolvePayload(payload),
            message: error.message,
        });
        throw error;
    }

    if (payload.keepPaths.length > 1) {
        console.info('[resolve] success', {
            requestId,
            ...summarizeResolvePayload(payload),
        });
    }

    return {
        requestId,
        payload,
    };
};

const downloadGroupArchive = (group) => {
    const link = document.createElement('a');
    link.href = buildGroupArchiveURL(group.master.path);
    link.rel = 'noopener';
    document.body.appendChild(link);
    link.click();
    link.remove();
};

const clearRenderedGroups = () => {
    Array.from(groupsContainer.children).forEach((child) => {
        if (child.id !== 'loading-indicator') {
            child.remove();
        }
    });
};

const setLoadingState = (loading) => {
    loadingIndicator.style.display = loading ? 'block' : 'none';
};

const renderEmptyState = (message) => {
    const empty = document.createElement('div');
    empty.className = 'loading-state glass-panel empty-state';
    empty.textContent = message;
    groupsContainer.appendChild(empty);
};

const updateGroupSelectionUI = (groupIndex) => {
    const group = STATE.groups[groupIndex];
    const groupEl = document.getElementById(`group-${groupIndex}`);
    if (!group || !groupEl) return;

    groupEl.querySelector('.group-summary').textContent = selectionSummary(group);

    const downloadBtn = groupEl.querySelector('.download-btn');
    const resolveBtn = groupEl.querySelector('.resolve-btn');
    if (downloadBtn) {
        downloadBtn.disabled = STATE.resolvingPage;
    }
    resolveBtn.disabled = STATE.resolvingPage || group.selectedPaths.length === 0;
    resolveBtn.textContent = group.selectedPaths.length > 1
        ? `Resolve Keeping ${group.selectedPaths.length}`
        : 'Resolve Group';

    groupEl.querySelectorAll('.image-card').forEach((card) => {
        const selected = isSelected(group, card.dataset.path);
        card.classList.toggle('selected', selected);
        card.classList.toggle('auto-selected', selected && group.selectedPaths.length === 1 && card.dataset.path === group.autoSelectedPath);
    });
};

const renderGroups = () => {
    clearRenderedGroups();

    const tplGroup = document.getElementById('group-template').content;
    const tplImage = document.getElementById('image-card-template').content;

    if (STATE.groups.length === 0) {
        renderEmptyState('No duplicate groups on this page.');
        return;
    }

    STATE.groups.forEach((group, index) => {
        if (!group) return;

        const groupNode = document.importNode(tplGroup, true);
        const groupEl = groupNode.querySelector('.duplicate-group');
        const titleEl = groupNode.querySelector('.group-title');
        const summaryEl = groupNode.querySelector('.group-summary');
        const grid = groupNode.querySelector('.images-grid');
        const downloadBtn = groupNode.querySelector('.download-btn');
        const resolveBtn = groupNode.querySelector('.resolve-btn');

        groupEl.id = `group-${index}`;
        titleEl.textContent = `Cluster ${(STATE.page - 1) * STATE.limit + index + 1}`;
        summaryEl.textContent = selectionSummary(group);

        const groupItems = allItemsForGroup(group);
        const groupItemCount = groupItems.length;
        const isCompactGroup = groupItemCount <= 2;

        groupEl.classList.toggle('group-compact', isCompactGroup);
        grid.classList.toggle('layout-pair', isCompactGroup);
        grid.classList.toggle('layout-crowded', groupItemCount >= 7);

        groupItems.forEach((img, itemIndex) => {
            const imgNode = document.importNode(tplImage, true);
            const card = imgNode.querySelector('.image-card');
            const imageEl = imgNode.querySelector('img');

            card.dataset.path = img.path;
            card.dataset.isMaster = img.isMaster;
            card.id = `card-${index}-${itemIndex}`;

            imageEl.src = `/image?path=${encodeURIComponent(img.path)}`;
            imageEl.alt = img.path;

            imgNode.querySelector('.resolution').textContent = img.width ? `${img.width}x${img.height}` : 'Unknown Res';
            imgNode.querySelector('.size').textContent = formatBytes(img.size);
            imgNode.querySelector('.date').textContent = img.createTime || 'Unknown Date';

            const parts = img.path.split('/');
            imgNode.querySelector('.path').textContent = parts.slice(Math.max(0, parts.length - 3)).join('/');

            card.addEventListener('click', () => queueSingleSelection(index, img.path));
            card.addEventListener('dblclick', (event) => {
                event.preventDefault();
                handleMultiSelect(index, img.path);
            });
            grid.appendChild(imgNode);
        });

        downloadBtn.addEventListener('click', () => downloadGroupArchive(group));
        resolveBtn.addEventListener('click', () => resolveGroup(index));
        groupsContainer.appendChild(groupNode);
        updateGroupSelectionUI(index);
    });
};

const updateStats = () => {
    totalGroupsEl.textContent = STATE.totalGroups;

    const savings = visibleGroups().reduce((sum, group) => {
        return sum + allItemsForGroup(group).reduce((groupSum, item) => {
            return groupSum + (isSelected(group, item.path) ? 0 : (item.size || 0));
        }, 0);
    }, 0);

    potentialSavingsEl.textContent = formatBytes(savings);
};

const buildPageModel = () => {
    if (STATE.totalPages <= 7) {
        return Array.from({ length: STATE.totalPages }, (_, idx) => idx + 1);
    }

    const pages = [1];
    const start = Math.max(2, STATE.page - 1);
    const end = Math.min(STATE.totalPages - 1, STATE.page + 1);

    if (start > 2) pages.push('ellipsis-left');
    for (let page = start; page <= end; page += 1) {
        pages.push(page);
    }
    if (end < STATE.totalPages - 1) pages.push('ellipsis-right');
    pages.push(STATE.totalPages);
    return pages;
};

const renderPageNumbers = () => {
    pageNumbers.replaceChildren();

    if (STATE.totalPages <= 1) {
        return;
    }

    buildPageModel().forEach((item) => {
        if (typeof item !== 'number') {
            const ellipsis = document.createElement('span');
            ellipsis.className = 'page-ellipsis';
            ellipsis.textContent = '…';
            pageNumbers.appendChild(ellipsis);
            return;
        }

        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'page-number';
        button.textContent = String(item);
        button.disabled = STATE.loading || STATE.resolvingPage;
        if (item === STATE.page) {
            button.classList.add('active');
            button.disabled = true;
        }
        button.addEventListener('click', () => fetchGroups(item, { historyMode: 'push' }));
        pageNumbers.appendChild(button);
    });
};

const updatePagination = () => {
    prevBtn.disabled = STATE.loading || STATE.resolvingPage || STATE.page <= 1;
    nextBtn.disabled = STATE.loading || STATE.resolvingPage || STATE.totalPages === 0 || STATE.page >= STATE.totalPages;

    pageInfo.textContent = STATE.totalPages > 0
        ? `Page ${STATE.page} of ${STATE.totalPages}`
        : 'No pages';

    pageSizeSelect.value = String(STATE.limit);
    pageSizeSelect.disabled = STATE.loading || STATE.resolvingPage;
    renderPageNumbers();

    const currentCount = visibleGroups().length;
    resolvePageBtn.disabled = STATE.loading || STATE.resolvingPage || currentCount === 0;
    resolvePageBtn.textContent = STATE.resolvingPage
        ? 'Resolving Current Page...'
        : `Resolve ${currentCount} Groups on This Page`;

    if (STATE.resolvingPage && STATE.batchProgressText) {
        batchStatus.textContent = STATE.batchProgressText;
        return;
    }

    batchStatus.textContent = STATE.totalGroups > 0
        ? `Showing ${currentCount} of ${STATE.totalGroups} groups`
        : 'No groups loaded';
};

const focusGroup = (index) => {
    if (index < 0 || !STATE.groups[index]) {
        STATE.focusedIndex = -1;
        return;
    }

    STATE.focusedIndex = index;
    document.querySelectorAll('.duplicate-group').forEach((groupEl) => {
        if (groupEl.id === `group-${index}`) {
            groupEl.style.boxShadow = '0 0 0 1px var(--accent), 0 8px 32px var(--accent-glow)';
            groupEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } else {
            groupEl.style.boxShadow = '0 4px 30px rgba(0, 0, 0, 0.1)';
        }
    });
};

const autoAdvanceFocus = () => {
    const nextIndex = STATE.groups.findIndex((group, idx) => idx > STATE.focusedIndex && group);
    if (nextIndex >= 0) {
        focusGroup(nextIndex);
        return;
    }

    for (let idx = STATE.focusedIndex - 1; idx >= 0; idx -= 1) {
        if (STATE.groups[idx]) {
            focusGroup(idx);
            return;
        }
    }

    STATE.focusedIndex = -1;
};

const applySingleSelection = (groupIndex, path) => {
    const group = STATE.groups[groupIndex];
    if (!group) return;

    group.selectedPaths = [path];
    updateGroupSelectionUI(groupIndex);
    updateStats();
    focusGroup(groupIndex);
};

const applyMultiSelection = (groupIndex, path) => {
    const group = STATE.groups[groupIndex];
    if (!group) return;

    if (isSelected(group, path)) {
        if (group.selectedPaths.length === 1) {
            focusGroup(groupIndex);
            return;
        }
        group.selectedPaths = group.selectedPaths.filter((selectedPath) => selectedPath !== path);
    } else {
        group.selectedPaths = [...group.selectedPaths, path];
    }

    updateGroupSelectionUI(groupIndex);
    updateStats();
    focusGroup(groupIndex);
};

const queueSingleSelection = (groupIndex, path) => {
    if (STATE.pendingSingleClick !== null) {
        window.clearTimeout(STATE.pendingSingleClick);
    }

    STATE.pendingSingleClick = window.setTimeout(() => {
        STATE.pendingSingleClick = null;
        applySingleSelection(groupIndex, path);
    }, 220);
};

const handleMultiSelect = (groupIndex, path) => {
    if (STATE.pendingSingleClick !== null) {
        window.clearTimeout(STATE.pendingSingleClick);
        STATE.pendingSingleClick = null;
    }
    applyMultiSelection(groupIndex, path);
};

const markGroupResolved = (groupIndex) => {
    const groupEl = document.getElementById(`group-${groupIndex}`);
    if (groupEl) {
        groupEl.style.transform = 'scale(0.98)';
        groupEl.style.opacity = '0';
        setTimeout(() => groupEl.remove(), 180);
    }

    STATE.groups[groupIndex] = null;
    updateStats();
    updatePagination();

    if (STATE.focusedIndex === groupIndex) {
        autoAdvanceFocus();
    }
};

const fetchGroups = async (page, { historyMode = 'replace' } = {}) => {
    if (STATE.loading) return;

    const requestedPage = parsePositiveInt(page, DEFAULT_PAGE);

    STATE.loading = true;
    updatePagination();
    setLoadingState(true);
    clearRenderedGroups();

    try {
        const response = await fetch(buildDuplicatesURL(requestedPage, STATE.limit));
        if (!response.ok) {
            throw new Error(`request failed with status ${response.status}`);
        }

        const data = await response.json();
        STATE.page = parsePositiveInt(data.page, requestedPage);
        STATE.limit = sanitizeLimit(data.limit);
        STATE.totalGroups = data.total || 0;
        STATE.totalPages = data.totalPages || 0;
        STATE.groups = (data.groups || []).map(normalizeGroup);
        syncPaginationURL(STATE.page, STATE.limit, historyMode === 'none' ? 'replace' : historyMode);

        renderGroups();
        updateStats();
        updatePagination();

        if (STATE.groups.length > 0) {
            focusGroup(0);
        } else {
            STATE.focusedIndex = -1;
        }
    } catch (error) {
        console.error('Failed to load groups', error);
        clearRenderedGroups();
        renderEmptyState('Failed to load data. Check browser console for details.');
    } finally {
        STATE.loading = false;
        setLoadingState(false);
        updatePagination();
    }
};

const resolveGroup = async (groupIndex) => {
    const group = STATE.groups[groupIndex];
    if (!group || STATE.resolvingPage) return;

    try {
        await postResolve(group);
        markGroupResolved(groupIndex);

        if (visibleGroups().length === 0) {
            await fetchGroups(STATE.page);
        }
    } catch (error) {
        console.error('Resolve error', {
            error,
            requestId: error.requestId || '',
            status: error.status,
            payload: error.payload || buildResolvePayload(group),
        });
        alert(`Failed to resolve group: ${formatResolveErrorSummary(error)}`);
    }
};

const resolveCurrentPage = async () => {
    const groups = STATE.groups
        .map((group, index) => (group ? { group, index } : null))
        .filter(Boolean);
    if (groups.length === 0 || STATE.resolvingPage) return;

    const confirmed = window.confirm(`Resolve ${groups.length} groups on page ${STATE.page}?`);
    if (!confirmed) return;

    STATE.resolvingPage = true;
    STATE.batchProgressText = `Resolving 0/${groups.length} groups on page ${STATE.page} with concurrency ${Math.min(BATCH_RESOLVE_CONCURRENCY, groups.length)}`;
    updatePagination();
    console.info('[resolve page] start', {
        page: STATE.page,
        groupCount: groups.length,
        concurrency: Math.min(BATCH_RESOLVE_CONCURRENCY, groups.length),
    });

    let resolved = 0;
    let failed = 0;
    let nextTask = 0;
    const failures = [];

    const updateBatchProgress = () => {
        STATE.batchProgressText = `Resolved ${resolved}/${groups.length} groups on page ${STATE.page}`;
        if (failed > 0) {
            STATE.batchProgressText += `, ${failed} failed`;
        }
        updatePagination();
    };

    try {
        const concurrency = Math.min(BATCH_RESOLVE_CONCURRENCY, groups.length);
        const workers = Array.from({ length: concurrency }, async () => {
            while (nextTask < groups.length) {
                const currentTask = groups[nextTask];
                nextTask += 1;

                try {
                    await postResolve(currentTask.group);
                    markGroupResolved(currentTask.index);
                    resolved += 1;
                } catch (error) {
                    failed += 1;
                    failures.push({
                        path: currentTask.group.master.path,
                        message: error.message,
                        requestId: error.requestId || '',
                        status: error.status,
                        keepCount: error.payload?.keepPaths?.length || currentTask.group.selectedPaths.length,
                    });
                    console.error('Batch resolve error', {
                        masterPath: currentTask.group.master.path,
                        requestId: error.requestId || '',
                        status: error.status,
                        keepCount: error.payload?.keepPaths?.length || currentTask.group.selectedPaths.length,
                        error,
                    });
                }

                updateBatchProgress();
            }
        });

        await Promise.all(workers);
    } finally {
        STATE.resolvingPage = false;
        STATE.batchProgressText = '';
        console.info('[resolve page] finished', {
            page: STATE.page,
            resolved,
            failed,
        });
        await fetchGroups(STATE.page);
    }

    if (failures.length > 0) {
        const firstFailure = failures[0];
        const suffix = failures.length > 1
            ? ` First failure: ${firstFailure.path} (${firstFailure.message}; request id ${firstFailure.requestId || 'n/a'}; HTTP ${firstFailure.status || 'n/a'}; keep ${firstFailure.keepCount})`
            : ` Failure: ${firstFailure.path} (${firstFailure.message}; request id ${firstFailure.requestId || 'n/a'}; HTTP ${firstFailure.status || 'n/a'}; keep ${firstFailure.keepCount})`;
        alert(`Resolved ${resolved} groups, ${failed} failed.${suffix}`);
    }
};

document.addEventListener('keydown', (event) => {
    if (event.shiftKey && event.key === 'Enter') {
        event.preventDefault();
        resolveCurrentPage();
        return;
    }

    if (event.key === 'Enter' && STATE.focusedIndex >= 0) {
        event.preventDefault();
        resolveGroup(STATE.focusedIndex);
    }
});

prevBtn.addEventListener('click', () => fetchGroups(Math.max(1, STATE.page - 1), { historyMode: 'push' }));
nextBtn.addEventListener('click', () => fetchGroups(STATE.page + 1, { historyMode: 'push' }));
pageSizeSelect.addEventListener('change', (event) => {
    STATE.limit = sanitizeLimit(event.target.value);
    fetchGroups(1, { historyMode: 'push' });
});
resolvePageBtn.addEventListener('click', () => resolveCurrentPage());

window.addEventListener('DOMContentLoaded', () => {
    const initialPagination = readPaginationFromURL();
    STATE.page = initialPagination.page;
    STATE.limit = initialPagination.limit;
    updatePagination();
    fetchGroups(initialPagination.page, { historyMode: 'replace' });
});

window.addEventListener('popstate', () => {
    const pagination = readPaginationFromURL();
    STATE.page = pagination.page;
    STATE.limit = pagination.limit;
    updatePagination();
    fetchGroups(pagination.page, { historyMode: 'none' });
});
