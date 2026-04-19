const reviewState = {
    path: '',
    summary: {},
    eventCounts: {},
    events: [],
    activeFilter: 'all',
};

const logPathInput = document.getElementById('log-path');
const loadLogBtn = document.getElementById('load-log');
const logStatus = document.getElementById('log-status');
const eventFilters = document.getElementById('event-filters');
const summaryMode = document.getElementById('summary-mode');
const summaryGrid = document.getElementById('summary-grid');
const eventList = document.getElementById('event-list');
const eventTemplate = document.getElementById('review-event-template');

const statTotal = document.getElementById('stat-total');
const statChanged = document.getElementById('stat-changed');
const statRehome = document.getElementById('stat-rehome');
const statStandalone = document.getElementById('stat-standalone');

const EVENT_LABELS = {
    all: 'All',
    rehome: 'Rehome',
    standalone: 'Standalone',
    missing_thumbnail: 'Missing Thumbnail',
    validation_failed: 'Validation Failed',
    prepare_rehome_failed: 'Prepare Rehome Failed',
    skip_master: 'Skip Master',
    skip_rehome_candidate: 'Skip Candidate',
    rehome_validation_failed: 'Rehome Validation Failed',
};

const formatFieldLabel = (key) => key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());

const pathTail = (value) => {
    if (!value) return '';
    const parts = value.split('/');
    return parts.slice(Math.max(0, parts.length - 3)).join('/');
};

const buildPreviewCandidates = (event) => {
    const candidates = [];
    const seen = new Set();
    const add = (label, value) => {
        if (!value || seen.has(value)) return;
        seen.add(value);
        candidates.push({ label, path: value });
    };

    switch (event.event) {
    case 'rehome':
        add('Thumbnail', event.thumbnailPath || event.path);
        add('Source Master', event.sourceMaster || event.masterPath);
        add('Target Master', event.targetMaster);
        break;
    case 'standalone':
        add('Standalone', event.path);
        add('Source Master', event.sourceMaster || event.masterPath);
        break;
    case 'missing_thumbnail':
        add('Missing Thumbnail', event.thumbnailPath || event.path);
        add('Master', event.masterPath || event.sourceMaster || event.targetMaster);
        break;
    case 'validation_failed':
    case 'prepare_rehome_failed':
    case 'rehome_validation_failed':
        add('Candidate', event.thumbnailPath || event.path);
        add('Source Master', event.sourceMaster || event.masterPath);
        add('Target Master', event.targetMaster);
        break;
    default:
        add('Image', event.path || event.thumbnailPath);
        add('Master', event.masterPath || event.sourceMaster);
        add('Target Master', event.targetMaster);
        break;
    }

    return candidates;
};

const updateStats = () => {
    const total = reviewState.events.length;
    const changed = reviewState.events.filter((event) => event.changed).length;
    statTotal.textContent = String(total);
    statChanged.textContent = String(changed);
    statRehome.textContent = String(reviewState.eventCounts.rehome || 0);
    statStandalone.textContent = String(reviewState.eventCounts.standalone || 0);
};

const renderFilters = () => {
    const filterKeys = ['all', ...Object.keys(reviewState.eventCounts).sort()];
    eventFilters.innerHTML = '';

    filterKeys.forEach((filterKey) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `filter-chip${reviewState.activeFilter === filterKey ? ' active' : ''}`;
        const count = filterKey === 'all'
            ? reviewState.events.length
            : (reviewState.eventCounts[filterKey] || 0);
        button.textContent = `${EVENT_LABELS[filterKey] || filterKey} (${count})`;
        button.addEventListener('click', () => {
            reviewState.activeFilter = filterKey;
            renderFilters();
            renderEvents();
        });
        eventFilters.appendChild(button);
    });
};

const renderSummary = () => {
    const mode = reviewState.summary.mode || 'unknown';
    summaryMode.textContent = mode;
    summaryMode.className = `badge ${mode === 'apply' ? 'master-badge' : 'neutral-badge'}`;

    summaryGrid.innerHTML = '';
    const orderedKeys = [
        'groups_scanned',
        'groups_changed',
        'thumbnails_scanned',
        'removed',
        'rehomed',
        'standalone_created',
        'missing_removed',
        'standalone_deleted',
        'validation_failures',
        'skipped_groups',
    ];

    orderedKeys.forEach((key) => {
        const value = reviewState.summary[key];
        if (value === undefined) return;

        const card = document.createElement('div');
        card.className = 'summary-item';
        card.innerHTML = `
            <span class="summary-item-value">${value}</span>
            <span class="summary-item-label">${formatFieldLabel(key)}</span>
        `;
        summaryGrid.appendChild(card);
    });
};

const badgeClassForEvent = (event) => {
    if (event === 'rehome' || event === 'standalone') return 'master-badge';
    if (event === 'missing_thumbnail') return 'auto-badge';
    return 'neutral-badge';
};

const describeEvent = (event) => {
    switch (event.event) {
    case 'rehome':
        return `${event.thumbnailPath || event.path} -> ${event.targetMaster || 'unknown target'}`;
    case 'standalone':
        return event.path || 'standalone candidate';
    case 'missing_thumbnail':
        return event.thumbnailPath || 'missing thumbnail';
    case 'validation_failed':
        return event.thumbnailPath || event.path || 'validation failure';
    default:
        return event.path || event.thumbnailPath || event.masterPath || event.raw;
    }
};

const buildFieldEntries = (event) => {
    const fields = { ...event.fields };
    if (fields.has_dhash !== undefined) {
        delete fields.has_phash;
    }

    const preferredOrder = [
        'action',
        'mode',
        'thumbnail_path',
        'path',
        'master_path',
        'source_master',
        'target_master',
        'rehome_reason',
        'dimensions',
        'size',
        'create_time',
        'has_dhash',
        'has_phash',
        'standalone_deleted',
        'error',
    ];

    const entries = [];
    preferredOrder.forEach((key) => {
        if (fields[key] !== undefined) {
            entries.push([key, fields[key]]);
        }
    });

    Object.keys(fields)
        .sort()
        .forEach((key) => {
            if (preferredOrder.includes(key) || key === 'event') return;
            entries.push([key, fields[key]]);
        });

    return entries;
};

const renderEvents = () => {
    eventList.innerHTML = '';

    const visibleEvents = reviewState.activeFilter === 'all'
        ? reviewState.events
        : reviewState.events.filter((event) => event.event === reviewState.activeFilter);

    if (visibleEvents.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'glass-panel loading-state empty-state';
        empty.textContent = 'No events match the current filter.';
        eventList.appendChild(empty);
        return;
    }

    visibleEvents.forEach((event) => {
        const node = eventTemplate.content.firstElementChild.cloneNode(true);
        node.querySelector('.review-event-title').textContent = EVENT_LABELS[event.event] || event.event;
        node.querySelector('.review-event-subtitle').textContent = [event.timestamp, event.source].filter(Boolean).join(' • ') || `line ${event.line}`;
        node.querySelector('.review-event-path').textContent = describeEvent(event);
        node.querySelector('.review-event-raw').textContent = event.raw;

        const badges = node.querySelector('.review-event-badges');
        const eventBadge = document.createElement('span');
        eventBadge.className = `badge ${badgeClassForEvent(event.event)}`;
        eventBadge.textContent = event.event;
        badges.appendChild(eventBadge);

        if (event.reason) {
            const reasonBadge = document.createElement('span');
            reasonBadge.className = 'badge neutral-badge';
            reasonBadge.textContent = event.reason;
            badges.appendChild(reasonBadge);
        }

        const previewsContainer = node.querySelector('.review-event-previews');
        const previews = buildPreviewCandidates(event);
        previews.forEach((preview) => {
            const card = document.createElement('a');
            card.className = 'review-preview-card';
            card.href = `/image?path=${encodeURIComponent(preview.path)}`;
            card.target = '_blank';
            card.rel = 'noreferrer';

            const image = document.createElement('img');
            image.loading = 'lazy';
            image.src = card.href;
            image.alt = preview.path;
            image.addEventListener('error', () => {
                card.classList.add('unavailable');
            });

            const meta = document.createElement('div');
            meta.className = 'review-preview-meta';
            meta.innerHTML = `
                <span class="review-preview-label">${preview.label}</span>
                <span class="review-preview-path">${pathTail(preview.path)}</span>
            `;

            card.appendChild(image);
            card.appendChild(meta);
            previewsContainer.appendChild(card);
        });

        const fieldsContainer = node.querySelector('.review-event-fields');
        buildFieldEntries(event).forEach(([key, value]) => {
            const item = document.createElement('div');
            item.className = 'review-field';
            item.innerHTML = `
                <span class="review-field-key">${formatFieldLabel(key)}</span>
                <span class="review-field-value">${value}</span>
            `;
            fieldsContainer.appendChild(item);
        });

        eventList.appendChild(node);
    });
};

const setStatus = (message, isError = false) => {
    logStatus.textContent = message;
    logStatus.classList.toggle('error-text', isError);
};

const buildLogURL = () => {
    const url = new URL('/api/cleangroups-log', window.location.origin);
    const path = logPathInput.value.trim();
    if (path) {
        url.searchParams.set('path', path);
    }
    return url.toString();
};

const loadLog = async () => {
    loadLogBtn.disabled = true;
    setStatus('Loading cleangroups log...');

    try {
        const response = await fetch(buildLogURL());
        const text = await response.text();
        if (!response.ok) {
            throw new Error(text || 'Failed to load log');
        }

        const data = JSON.parse(text);
        reviewState.path = data.path || '';
        reviewState.summary = data.summary || {};
        reviewState.eventCounts = data.eventCounts || {};
        reviewState.events = data.events || [];
        reviewState.activeFilter = 'all';

        if (!logPathInput.value.trim() && reviewState.path) {
            logPathInput.value = reviewState.path;
        }

        updateStats();
        renderFilters();
        renderSummary();
        renderEvents();
        setStatus(`Loaded ${reviewState.events.length} events from ${reviewState.path || 'cleangroups log'}`);
    } catch (error) {
        reviewState.summary = {};
        reviewState.eventCounts = {};
        reviewState.events = [];
        updateStats();
        renderFilters();
        renderSummary();
        renderEvents();
        setStatus(error.message || 'Failed to load log', true);
    } finally {
        loadLogBtn.disabled = false;
    }
};

loadLogBtn.addEventListener('click', loadLog);
logPathInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
        event.preventDefault();
        loadLog();
    }
});

loadLog();
