/**
 * FPL Dashboard JavaScript
 * Handles API calls, Chart.js initialization, and UI updates
 */

// Configuration
const API_BASE = '';
let charts = {};

// Auth check
const token = localStorage.getItem('fpl_token');
if (!token) {
    window.location.href = '/';
}

// API Helper
async function apiCall(endpoint, options = {}) {
    const defaultOptions = {
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
        },
    };
    
    const response = await fetch(`${API_BASE}${endpoint}`, { ...defaultOptions, ...options });
    
    if (response.status === 401) {
        localStorage.removeItem('fpl_token');
        window.location.href = '/';
        return null;
    }
    
    return response.json();
}

// Chart.js configuration
Chart.defaults.color = '#a0a0b0';
Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.05)';
Chart.defaults.font.family = "'Inter', sans-serif";

const chartColors = {
    purple: '#7c3aed',
    blue: '#3b82f6',
    green: '#10b981',
    orange: '#f59e0b',
    pink: '#ec4899',
    cyan: '#06b6d4',
};

// Initialize Dashboard
async function initDashboard() {
    try {
        // Fetch user info
        const user = await apiCall('/auth/me');
        if (user) {
            document.getElementById('usernameDisplay').textContent = user.username;
        }
        
        // Fetch filters
        const filters = await apiCall('/dashboard/filters');
        populateFilters(filters);
        
        // Load dashboard data
        await loadDashboardData();
        
        // Show content
        document.getElementById('loadingState').classList.add('d-none');
        document.getElementById('dashboardContent').classList.remove('d-none');
        
    } catch (error) {
        console.error('Dashboard initialization failed:', error);
    }
}

// Populate filter dropdowns
function populateFilters(filters) {
    const teamSelect = document.getElementById('teamFilter');
    
    if (filters.teams) {
        filters.teams.forEach(team => {
            const option = document.createElement('option');
            option.value = team.id;
            option.textContent = team.name;
            teamSelect.appendChild(option);
        });
    }
    
    teamSelect.addEventListener('change', () => loadDashboardData());
}

// Load all dashboard data
async function loadDashboardData() {
    const teamId = document.getElementById('teamFilter').value;
    const queryParam = teamId ? `?team_id=${teamId}` : '';
    
    try {
        const [summary, trends, distributions, topPlayers] = await Promise.all([
            apiCall(`/dashboard/summary${queryParam}`),
            apiCall(`/dashboard/trends${queryParam}`),
            apiCall(`/dashboard/distributions${queryParam}`),
            apiCall('/dashboard/top-players?limit=10'),
        ]);
        
        updateKPIs(summary);
        updateTrendChart(trends);
        updatePositionChart(distributions);
        updateTeamGoalsChart(distributions);
        updateTopPlayersTable(topPlayers);
        updateGoalsAssistsChart(trends);
        
    } catch (error) {
        console.error('Failed to load dashboard data:', error);
    }
}

// Update KPI cards
function updateKPIs(summary) {
    document.getElementById('totalPlayers').textContent = summary.total_players?.toLocaleString() || 0;
    document.getElementById('totalTeams').textContent = summary.total_teams || 0;
    document.getElementById('totalGoals').textContent = summary.total_goals?.toLocaleString() || 0;
    document.getElementById('avgPoints').textContent = summary.avg_points_per_player?.toFixed(1) || 0;
}

// Trend Chart (Points by Gameweek)
function updateTrendChart(trends) {
    const ctx = document.getElementById('trendChart').getContext('2d');
    
    if (charts.trend) {
        charts.trend.destroy();
    }
    
    const labels = trends.data.map(d => `GW ${d.gameweek}`);
    const points = trends.data.map(d => d.total_points);
    
    charts.trend = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Total Points',
                data: points,
                borderColor: chartColors.purple,
                backgroundColor: 'rgba(124, 58, 237, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: chartColors.purple,
                pointBorderColor: '#fff',
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false,
                },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)',
                    },
                },
                x: {
                    grid: {
                        display: false,
                    },
                },
            },
        },
    });
}

// Position Distribution (Pie Chart)
function updatePositionChart(distributions) {
    const ctx = document.getElementById('positionChart').getContext('2d');
    
    if (charts.position) {
        charts.position.destroy();
    }
    
    const labels = distributions.by_position.map(d => d.position);
    const data = distributions.by_position.map(d => d.player_count);
    
    charts.position = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: [
                    chartColors.purple,
                    chartColors.blue,
                    chartColors.green,
                    chartColors.orange,
                ],
                borderWidth: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        usePointStyle: true,
                        pointStyle: 'circle',
                    },
                },
            },
            cutout: '60%',
        },
    });
}

// Team Goals Chart (Bar Chart)
function updateTeamGoalsChart(distributions) {
    const ctx = document.getElementById('teamGoalsChart').getContext('2d');
    
    if (charts.teamGoals) {
        charts.teamGoals.destroy();
    }
    
    // Top 10 teams by goals
    const topTeams = distributions.by_team
        .sort((a, b) => b.total_goals - a.total_goals)
        .slice(0, 10);
    
    const labels = topTeams.map(d => d.team_name);
    const data = topTeams.map(d => d.total_goals);
    
    charts.teamGoals = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Goals',
                data,
                backgroundColor: createGradient(ctx, chartColors.blue, chartColors.cyan),
                borderRadius: 8,
                borderSkipped: false,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: {
                    display: false,
                },
            },
            scales: {
                x: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)',
                    },
                },
                y: {
                    grid: {
                        display: false,
                    },
                },
            },
        },
    });
}

// Goals & Assists Trend Chart
function updateGoalsAssistsChart(trends) {
    const ctx = document.getElementById('goalsAssistsChart').getContext('2d');
    
    if (charts.goalsAssists) {
        charts.goalsAssists.destroy();
    }
    
    const labels = trends.data.map(d => `GW ${d.gameweek}`);
    const goals = trends.data.map(d => d.total_goals);
    const assists = trends.data.map(d => d.total_assists);
    
    charts.goalsAssists = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Goals',
                    data: goals,
                    backgroundColor: chartColors.green,
                    borderRadius: 4,
                },
                {
                    label: 'Assists',
                    data: assists,
                    backgroundColor: chartColors.blue,
                    borderRadius: 4,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        pointStyle: 'circle',
                    },
                },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)',
                    },
                },
                x: {
                    grid: {
                        display: false,
                    },
                },
            },
        },
    });
}

// Update Top Players Table
function updateTopPlayersTable(players) {
    const tbody = document.querySelector('#topPlayersTable tbody');
    tbody.innerHTML = '';
    
    players.forEach((player, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><span class="badge bg-primary">${index + 1}</span></td>
            <td>${player.player_name}</td>
            <td>${player.team_name}</td>
            <td><strong>${player.total_points}</strong></td>
            <td>${player.total_goals}</td>
        `;
        tbody.appendChild(row);
    });
}

// Helper: Create gradient for charts
function createGradient(ctx, color1, color2) {
    const gradient = ctx.createLinearGradient(0, 0, 200, 0);
    gradient.addColorStop(0, color1);
    gradient.addColorStop(1, color2);
    return gradient;
}

// Logout handler
document.getElementById('logoutBtn').addEventListener('click', () => {
    localStorage.removeItem('fpl_token');
    window.location.href = '/';
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', initDashboard);
