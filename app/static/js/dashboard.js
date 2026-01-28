/**
 * FPL Data Dashboard Logic
 * Features: Canonical Trends, Understat Fusion, Decoupled Search
 */

let state = {
    view: 'overview',
    season: '2024-25',
    team_id: '',
    token: localStorage.getItem('fpl_token'),
    charts: {},
    search: {
        name: '',
        pos: '',
        sort: 'total_points'
    }
};

// API Gateway
async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Authorization': `Bearer ${state.token}`,
            'Content-Type': 'application/json',
        }
    };
    if (body) options.body = JSON.stringify(body);

    const url = new URL(`${window.location.origin}${endpoint}`);
    if (method === 'GET' && !url.searchParams.has('season') && state.season) {
        url.searchParams.set('season', state.season);
    }

    const response = await fetch(url.toString(), options);
    if (response.status === 401) {
        localStorage.removeItem('fpl_token');
        window.location.href = '/';
        return null;
    }
    return response.json();
}

/** --- View Orchestration --- **/

function setView(viewId) {
    state.view = viewId;
    document.querySelectorAll('.nav-item').forEach(i => i.classList.toggle('active', i.dataset.view === viewId));
    document.querySelectorAll('.view-section').forEach(s => s.classList.toggle('d-none', s.id !== `view-${viewId}`));

    // UI Helpers
    document.getElementById('searchContainer').classList.toggle('d-none', viewId !== 'discovery');

    const titles = {
        overview: ['Dashboard Overview', 'Key Performance Indicators'],
        trends: ['Performance Trends', 'Gap-free volatility tracking'],
        discovery: ['Player Discovery', 'Global lookup across all teams'],
        teams: ['Squad Analytics', 'Team productivity matrix'],
        standings: ['League Standings', 'Performance-based table']
    };

    if (titles[viewId]) {
        document.getElementById('viewTitle').textContent = titles[viewId][0];
        document.getElementById('viewSubtitle').textContent = titles[viewId][1];
    }

    refreshData();
}

/** --- Data Pipelines --- **/

async function refreshData() {
    const loader = document.getElementById('loadingState');
    const content = document.getElementById('dashboardContent');

    loader.classList.remove('d-none');
    content.classList.add('d-none');

    try {
        if (state.view === 'overview') await loadOverview();
        if (state.view === 'trends') await loadTrends();
        if (state.view === 'discovery') await loadDiscovery();
        if (state.view === 'teams') await loadTeams();
        if (state.view === 'standings') await loadStandings();
    } catch (e) {
        console.error("Data Link Failure:", e);
    } finally {
        loader.classList.add('d-none');
        content.classList.remove('d-none');
    }
}

async function loadOverview() {
    const q = state.team_id ? `?team_id=${state.team_id}` : '';
    const [summary, trends, dist] = await Promise.all([
        apiCall(`/dashboard/summary${q}`),
        apiCall(`/dashboard/trends${q}`),
        apiCall(`/dashboard/distributions`)
    ]);

    updateKPIs(summary);
    renderTrendChart(trends);
    renderPosDist(dist);
}

async function loadTrends() {
    const q = state.team_id ? `?team_id=${state.team_id}` : '';
    const trends = await apiCall(`/dashboard/trends${q}`);
    renderVolatilityChart(trends);
}

async function loadDiscovery() {
    const results = await apiCall('/dashboard/search/players', 'POST', {
        name: state.search.name,
        position: state.search.pos,
        season: state.season,
        sort_by: state.search.sort
    });

    const body = document.querySelector('#discoveryTable tbody');
    body.innerHTML = results.map(p => `
        <tr class="cursor-pointer" onclick="showMetrics(${p.player_id})">
            <td><div class="fw-bold">${p.player_name}</div></td>
            <td><span class="text-secondary small fw-medium">${p.team_name}</span></td>
            <td><span class="metric-value text-info">${p.total_points}</span></td>
            <td><span class="text-warning">${(p.total_points / 38).toFixed(1)}</span></td>
            <td><button class="btn btn-sm btn-outline-primary">Metrics</button></td>
        </tr>
    `).join('');
}

async function loadTeams() {
    const container = document.getElementById('teamSquadContainer');
    const hint = document.getElementById('teamSelectionHint');

    if (!state.team_id) {
        container.classList.add('d-none');
        hint.classList.remove('d-none');
        return;
    }

    hint.classList.add('d-none');
    container.classList.remove('d-none');

    const res = await apiCall(`/dashboard/teams/${state.team_id}/squad`);
    document.getElementById('teamTotalPoints').textContent = res.players.reduce((s, p) => s + p.total_points, 0).toLocaleString();

    const body = document.querySelector('#squadTable tbody');
    body.innerHTML = res.players.map(p => `
        <tr onclick="showMetrics(${p.player_id})" class="cursor-pointer">
            <td><div>${p.name}</div><span class="badge bg-secondary" style="font-size:0.6rem">${p.position}</span></td>
            <td><span class="metric-value">${p.total_points}</span></td>
            <td><small>${p.goals}G / ${p.assists}A</small></td>
            <td>£${p.now_cost.toFixed(1)}m</td>
            <td><span class="metric-value text-info">${p.minutes.toLocaleString()}</span></td>
            <td>${p.pts_per_90 ? p.pts_per_90.toFixed(2) : '0.00'}</td>
        </tr>
    `).join('');
}

async function loadStandings() {
    const res = await apiCall('/dashboard/standings');
    const body = document.querySelector('#standingsTable tbody');
    body.innerHTML = res.standings.map(s => `
        <tr>
            <td class="fw-bold text-center">${s.rank}</td>
            <td class="text-start">${s.team_name}</td>
            <td>${s.played}</td>
            <td><small>${s.wins}-${s.draws}-${s.losses}</small></td>
            <td>${s.goal_diff > 0 ? '+' : ''}${s.goal_diff}</td>
            <td class="fw-bold text-info">${s.points}</td>
            <td class="row-understat text-info">
                <span class="metric-value">${s.xG_for !== null ? s.xG_for.toFixed(1) : '—'}</span>
                <span class="text-muted"> / </span>
                <span class="metric-value text-danger">${s.xG_against !== null ? s.xG_against.toFixed(1) : '—'}</span>
            </td>
        </tr>
    `).join('');
}

/** --- Modal & Charts --- **/

window.showMetrics = async function (pid) {
    const res = await apiCall(`/dashboard/players/${pid}/trends`);
    document.getElementById('playerModalTitle').textContent = res.player_name;

    // Stats Detail
    const stats = document.getElementById('playerStatsDetail');
    const totalMinutes = res.trend.reduce((s, t) => s + t.minutes, 0);
    const totalPoints = res.trend.reduce((s, t) => s + t.points, 0);
    stats.innerHTML = `
        <div class="row g-2">
            <div class="col-6"><span class="metric-label">Total Pts</span><div class="metric-value h5">${totalPoints}</div></div>
            <div class="col-6"><span class="metric-label">Minutes</span><div class="metric-value h5 text-info">${totalMinutes.toLocaleString()}</div></div>
            <div class="col-6"><span class="metric-label">Mean Form</span><div class="metric-value h5">${res.overall_form || '-'}</div></div>
            <div class="col-6"><span class="metric-label">Gameweeks</span><div class="metric-value h5">${res.trend.length}</div></div>
        </div>
    `;

    const modal = new bootstrap.Modal(document.getElementById('playerModal'));
    modal.show();

    // Modal Chart
    const ctx = document.getElementById('playerTrendChart').getContext('2d');
    if (state.charts.player) state.charts.player.destroy();
    state.charts.player = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: res.trend.map(t => `GW${t.gameweek}`),
            datasets: [
                { label: 'Points', data: res.trend.map(t => t.points), backgroundColor: '#7c3aed', borderRadius: 4 },
                { label: 'Minutes', data: res.trend.map(t => t.minutes), type: 'line', borderColor: '#06b6d4', tension: 0.3 }
            ]
        },
        options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } }
    });
};

function renderTrendChart(t) {
    const ctx = document.getElementById('trendChart').getContext('2d');
    if (state.charts.trend) state.charts.trend.destroy();
    state.charts.trend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: t.data.map(d => `GW${d.gameweek}`),
            datasets: [{ label: 'Pts', data: t.data.map(d => d.total_points), borderColor: '#7c3aed', fill: true, backgroundColor: 'rgba(124,58,237,0.1)', tension: 0.4 }]
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });
}

function renderPosDist(d) {
    const ctx = document.getElementById('positionChart').getContext('2d');
    if (state.charts.pos) state.charts.pos.destroy();

    // Use actual data if available, fallback to defaults just in case
    const labels = d.by_position.length ? d.by_position.map(p => p.position) : ['GKP', 'DEF', 'MID', 'FWD'];
    const values = d.by_position.length ? d.by_position.map(p => p.player_count) : [0, 0, 0, 0];

    state.charts.pos = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: ['#7c3aed', '#3b82f6', '#10b981', '#f59e0b'],
                borderWidth: 0
            }]
        },
        options: { responsive: true, maintainAspectRatio: false, cutout: '80%' }
    });
}

function renderVolatilityChart(t) {
    const ctx = document.getElementById('goalsAssistsChart').getContext('2d');
    if (state.charts.ga) state.charts.ga.destroy();
    state.charts.ga = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: t.data.map(d => `GW${d.gameweek}`),
            datasets: [
                { label: 'Goals', data: t.data.map(d => d.total_goals), backgroundColor: '#10b981' },
                { label: 'Assists', data: t.data.map(d => d.total_assists), backgroundColor: '#3b82f6' }
            ]
        },
        options: { responsive: true, maintainAspectRatio: false }
    });
}

function updateKPIs(s) {
    document.getElementById('totalPlayers').textContent = s.total_players.toLocaleString();
    document.getElementById('totalTeams').textContent = s.total_teams;
    document.getElementById('totalGoals').textContent = s.total_goals.toLocaleString();
    document.getElementById('avgPoints').textContent = s.avg_points_per_player.toFixed(1);
}

/** --- Initialization --- **/

async function init() {
    // Basic Auth Check & User Info
    if (!state.token) {
        window.location.href = '/';
        return;
    }

    try {
        const user = await apiCall('/auth/me');
        if (user && user.username) {
            document.getElementById('usernameDisplay').textContent = user.username;
        }
    } catch (e) {
        console.warn("User info fetch failed:", e);
    }

    const filters = await apiCall('/dashboard/filters');

    // Populate Season
    const sSel = document.getElementById('seasonFilter');
    sSel.innerHTML = filters.seasons.map(s => `<option value="${s}">${s}</option>`).join('');
    sSel.value = state.season;

    // Populate Teams
    const tSel = document.getElementById('teamFilter');
    tSel.innerHTML = '<option value="">League-wide</option>' + filters.teams.map(t => `<option value="${t.id}">${t.name}</option>`).join('');

    // Events
    sSel.addEventListener('change', (e) => { state.season = e.target.value; refreshData(); });
    tSel.addEventListener('change', (e) => { state.team_id = e.target.value; refreshData(); });
    document.querySelectorAll('.nav-item').forEach(i => i.addEventListener('click', () => setView(i.dataset.view)));

    // Search Trigger
    document.getElementById('executeSearch').addEventListener('click', () => {
        state.search.name = document.getElementById('playerSearch').value;
        state.search.pos = document.getElementById('posFilter').value;
        state.search.sort = document.getElementById('sortFilter').value;
        refreshData();
    });

    // Routing
    const hash = window.location.hash.substring(1) || 'overview';
    setView(hash);
}

document.addEventListener('DOMContentLoaded', init);
document.getElementById('logoutBtn').addEventListener('click', () => { localStorage.removeItem('fpl_token'); window.location.href = '/'; });
