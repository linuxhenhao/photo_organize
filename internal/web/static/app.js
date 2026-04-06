const STATE = {
    page: 1,
    limit: 10,
    groups: [],
    loading: false,
    focusedIndex: -1 // Track keyboard focus
};

// Elements
const groupsContainer = document.getElementById('groups-container');
const loadingIndicator = document.getElementById('loading-indicator');
const prevBtn = document.getElementById('prev-page');
const nextBtn = document.getElementById('next-page');
const pageInfo = document.getElementById('page-info');

// Utility
const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

const determineWinner = (master, duplicates) => {
    let all = [master, ...duplicates];
    
    // Sort by resolution (if available), then by size
    all.sort((a, b) => {
        const resA = (a.width || 0) * (a.height || 0);
        const resB = (b.width || 0) * (b.height || 0);
        
        if (resA > resB) return -1;
        if (resA < resB) return 1;
        
        if (a.size > b.size) return -1;
        if (a.size < b.size) return 1;
        return 0;
    });
    
    return all[0].path; // Best item
};

// API
const fetchGroups = async (page) => {
    if (STATE.loading) return;
    STATE.loading = true;
    
    loadingIndicator.style.display = 'block';
    if(groupsContainer.children.length > 1) {
        // Clear all but loading
        Array.from(groupsContainer.children).forEach(c => {
            if (c.id !== 'loading-indicator') c.remove();
        });
    }

    try {
        const res = await fetch(`/api/duplicates?page=${page}&limit=${STATE.limit}`);
        const data = await res.json();
        
        STATE.groups = data.groups || [];
        STATE.page = data.page;
        
        renderGroups();
        updateStats();
        updatePagination(data.groups.length === STATE.limit);
        
        // Auto focus first group if exists
        STATE.focusedIndex = STATE.groups.length > 0 ? 0 : -1;
        if (STATE.focusedIndex >= 0) {
            focusGroup(STATE.focusedIndex);
        }

    } catch (e) {
        console.error('Failed to load groups', e);
        groupsContainer.innerHTML = `<div class="loading-state" style="color:var(--danger)">Failed to load data. Ensure server is running.</div>`;
    } finally {
        STATE.loading = false;
        loadingIndicator.style.display = 'none';
    }
};

const resolveGroup = async (groupIndex, keepPath) => {
    const group = STATE.groups[groupIndex];
    if (!group) return;

    const allPaths = [group.master.path, ...group.duplicates.map(d => d.path)];
    const deletePaths = allPaths.filter(p => p !== keepPath);

    try {
        const req = await fetch('/api/resolve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                keepPath: keepPath,
                deletePaths: deletePaths,
                masterPath: group.master.path
            })
        });

        if (req.ok) {
            // Remove from DOM
            const el = document.getElementById(`group-${groupIndex}`);
            if (el) {
                el.style.transform = 'scale(0.95)';
                el.style.opacity = '0';
                setTimeout(() => el.remove(), 200);
            }
            
            // Focus next
            if (STATE.focusedIndex === groupIndex) {
                STATE.groups[groupIndex] = null; // Mark dead
                autoAdvanceFocus();
            }
        } else {
            alert('Failed to resolve group');
        }
    } catch (e) {
        console.error('Resolve error', e);
    }
};

// DOM Manipulation
const renderGroups = () => {
    const tplGroup = document.getElementById('group-template').content;
    const tplImage = document.getElementById('image-card-template').content;

    STATE.groups.forEach((group, index) => {
        if (!group) return; // Skip dead groups

        const groupNode = document.importNode(tplGroup, true);
        const groupEl = groupNode.querySelector('.duplicate-group');
        groupEl.id = `group-${index}`;
        
        const grid = groupNode.querySelector('.images-grid');
        const resolveBtn = groupNode.querySelector('.resolve-btn');

        let selectedPath = determineWinner(group.master, group.duplicates);
        groupEl.dataset.selected = selectedPath;

        const allImages = [group.master, ...group.duplicates];
        allImages.forEach(img => {
            const imgNode = document.importNode(tplImage, true);
            const card = imgNode.querySelector('.image-card');
            
            card.dataset.path = img.path;
            card.dataset.isMaster = img.isMaster;
            card.id = `card-${index}-${btoa(img.path).replace(/=/g, '')}`;

            if (img.path === selectedPath) {
                card.classList.add('selected');
                card.classList.add('auto-selected');
            }

            imgNode.querySelector('img').src = `/image?path=${encodeURIComponent(img.path)}`;
            
            const resText = img.width ? `${img.width}x${img.height}` : 'Unknown Res';
            imgNode.querySelector('.resolution').textContent = resText;
            imgNode.querySelector('.size').textContent = formatBytes(img.size);
            imgNode.querySelector('.date').textContent = img.createTime;
            
            // Truncate path for display
            const parts = img.path.split('/');
            imgNode.querySelector('.path').textContent = parts.slice(Math.max(0, parts.length - 3)).join('/');

            // Click handler
            card.addEventListener('click', () => {
                selectItem(index, img.path);
            });

            grid.appendChild(imgNode);
        });

        resolveBtn.addEventListener('click', () => {
            resolveGroup(index, groupEl.dataset.selected);
        });

        groupsContainer.appendChild(groupNode);
    });
};

const selectItem = (groupIndex, path) => {
    const groupEl = document.getElementById(`group-${groupIndex}`);
    if (!groupEl) return;
    
    groupEl.dataset.selected = path;
    
    const cards = groupEl.querySelectorAll('.image-card');
    cards.forEach(c => {
        if (c.dataset.path === path) {
            c.classList.add('selected');
            c.classList.remove('auto-selected'); // Manual override
        } else {
            c.classList.remove('selected');
        }
    });

    focusGroup(groupIndex);
};

const updateStats = () => {
    const total = document.getElementById('total-groups');
    const saving = document.getElementById('pot-space');
    
    let activeGroups = STATE.groups.filter(g => g !== null);
    total.textContent = activeGroups.length;

    let potSaving = 0;
    activeGroups.forEach(g => {
        const all = [g.master, ...g.duplicates];
        const winnerPath = determineWinner(g.master, g.duplicates);
        all.forEach(i => {
            if (i.path !== winnerPath) {
                potSaving += i.size;
            }
        });
    });

    saving.textContent = formatBytes(potSaving);
};

const updatePagination = (hasNext) => {
    prevBtn.disabled = STATE.page <= 1;
    nextBtn.disabled = !hasNext;
    pageInfo.textContent = `Page ${STATE.page}`;
};

// Keyboard Navigation
const focusGroup = (index) => {
    STATE.focusedIndex = index;
    const allGroups = document.querySelectorAll('.duplicate-group');
    allGroups.forEach(g => {
        if (g.id === `group-${index}`) {
            g.style.boxShadow = '0 0 0 1px var(--accent), 0 8px 32px var(--accent-glow)';
            g.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } else {
            g.style.boxShadow = '0 4px 30px rgba(0, 0, 0, 0.1)';
        }
    });
};

const autoAdvanceFocus = () => {
    let next = STATE.focusedIndex + 1;
    while (next < STATE.limit && !STATE.groups[next]) {
        next++;
    }
    
    if (next < STATE.limit && STATE.groups[next]) {
        focusGroup(next);
    } else {
        // Find previous if no next
        let prev = STATE.focusedIndex - 1;
        while (prev >= 0 && !STATE.groups[prev]) {
            prev--;
        }
        if (prev >= 0 && STATE.groups[prev]) {
            focusGroup(prev);
        } else {
            STATE.focusedIndex = -1;
            // Maybe auto load next page?
        }
    }
};

document.addEventListener('keydown', (e) => {
    if (STATE.focusedIndex < 0) return;
    const groupEl = document.getElementById(`group-${STATE.focusedIndex}`);
    if (!groupEl) return;

    if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
        e.preventDefault();
        const cards = Array.from(groupEl.querySelectorAll('.image-card'));
        const currentIndex = cards.findIndex(c => c.classList.contains('selected'));
        
        let newIndex = currentIndex;
        if (e.key === 'ArrowRight') {
            newIndex = Math.min(cards.length - 1, currentIndex + 1);
        } else {
            newIndex = Math.max(0, currentIndex - 1);
        }
        
        if (newIndex !== currentIndex) {
            selectItem(STATE.focusedIndex, cards[newIndex].dataset.path);
        }
    } else if (e.key === 'Enter') {
        e.preventDefault();
        const selectedPath = groupEl.dataset.selected;
        if (selectedPath) {
            resolveGroup(STATE.focusedIndex, selectedPath);
        }
    }
});

// Event Listeners
prevBtn.addEventListener('click', () => fetchGroups(STATE.page - 1));
nextBtn.addEventListener('click', () => fetchGroups(STATE.page + 1));

// Init
window.addEventListener('DOMContentLoaded', () => {
    fetchGroups(1);
});
