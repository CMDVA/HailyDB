/**
 * NWS Alert Service Dashboard JavaScript
 * Focused on data integrity verification and system monitoring
 */

// Global variables
let dashboardData = {};

// Initialize dashboard
function initializeDashboard() {
    try {
        // Load dashboard data from script tag
        const dataElement = document.getElementById('dashboard-data');
        if (dataElement) {
            const dataText = dataElement.textContent.trim();
            if (dataText) {
                dashboardData = JSON.parse(dataText);
            }
        }
        
        // Ensure dashboardData has default values
        dashboardData = dashboardData || {};
        dashboardData.scheduler_running = dashboardData.scheduler_running || false;
        
        // Update status indicator
        updateStatusIndicator();
        
        // Calculate next poll time
        updateNextPollTime();
        
        // Load today's data for cron verification
        loadTodaysAlerts();
        loadTodaysSPCEvents();
        
        // Set up automatic refresh every 30 seconds
        setInterval(() => {
            loadTodaysAlerts();
            loadTodaysSPCEvents();
            updateStatusIndicator();
            updateNextPollTime();
            updateLastUpdateTime();
        }, 30000);
        
        console.log('Dashboard initialized successfully');
    } catch (error) {
        console.error('Error initializing dashboard:', error);
        dashboardData = {
            scheduler_running: false
        };
    }
}

// Update status indicators
function updateStatusIndicator() {
    const statusElement = document.getElementById('scheduler-status');
    if (statusElement && dashboardData.scheduler_running !== undefined) {
        if (dashboardData.scheduler_running) {
            statusElement.className = 'text-success';
            statusElement.innerHTML = '<i class="fas fa-play-circle me-1"></i>Running';
        } else {
            statusElement.className = 'text-danger';
            statusElement.innerHTML = '<i class="fas fa-stop-circle me-1"></i>Stopped';
        }
    }
}

// Calculate and display next poll time
function updateNextPollTime() {
    const nextPollElement = document.getElementById('next-poll-time');
    if (nextPollElement) {
        const now = new Date();
        const nextPoll = new Date(now.getTime() + (5 * 60 * 1000)); // 5 minutes from now
        nextPollElement.textContent = nextPoll.toLocaleTimeString();
    }
}

// Load today's alerts for cron verification
async function loadTodaysAlerts() {
    try {
        const today = new Date().toISOString().split('T')[0];
        const response = await fetch(`/alerts?format=json&per_page=50`);
        const data = await response.json();
        
        const tableContainer = document.getElementById('todays-alerts-table');
        if (!tableContainer) return;
        
        // Filter to today's alerts
        const todaysAlerts = data.alerts ? data.alerts.filter(alert => {
            const alertDate = new Date(alert.ingested_at || alert.effective).toISOString().split('T')[0];
            return alertDate === today;
        }) : [];
        
        if (todaysAlerts.length > 0) {
            // Group by event type for compact breakdown
            const alertsByType = todaysAlerts.reduce((acc, alert) => {
                const eventType = alert.event || 'Unknown';
                acc[eventType] = (acc[eventType] || 0) + 1;
                return acc;
            }, {});
            
            let html = `<div class="mb-2"><strong>${todaysAlerts.length} alerts ingested today</strong></div>`;
            html += '<div class="row text-center mb-2">';
            
            // Show top 3 alert types
            const sortedTypes = Object.entries(alertsByType)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 3);
            
            sortedTypes.forEach(([type, count]) => {
                const shortType = type.replace(' Warning', '').replace(' Watch', '').replace(' Advisory', '');
                html += `<div class="col-4"><div class="h6 text-primary">${count}</div><small>${shortType}</small></div>`;
            });
            
            html += '</div>';
            
            // Show recent alerts list (compact)
            if (todaysAlerts.length > 0) {
                html += '<div class="small">';
                todaysAlerts.slice(0, 5).forEach(alert => {
                    const time = new Date(alert.effective).toLocaleTimeString();
                    html += `<div class="d-flex justify-content-between border-bottom py-1">
                        <span>${alert.event}</span>
                        <span class="text-muted">${time}</span>
                    </div>`;
                });
                html += '</div>';
            }
            
            tableContainer.innerHTML = html;
        } else {
            tableContainer.innerHTML = '<div class="text-center py-3"><div class="h5 text-warning">0</div><small class="text-muted">No alerts ingested today</small></div>';
        }
    } catch (error) {
        console.error('Error loading today\'s alerts:', error);
        const tableContainer = document.getElementById('todays-alerts-table');
        if (tableContainer) {
            tableContainer.innerHTML = '<p class="text-danger">Error loading today\'s alerts.</p>';
        }
    }
}

// Load today's SPC events for cron verification
async function loadTodaysSPCEvents() {
    try {
        const today = new Date().toISOString().split('T')[0];
        const response = await fetch(`/spc/reports?format=json`);
        const data = await response.json();
        
        const container = document.getElementById('todays-spc-events');
        if (!container) return;
        
        // Filter to today's SPC events
        const todaysEvents = data.reports ? data.reports.filter(report => {
            return report.report_date === today;
        }) : [];
        
        if (todaysEvents.length > 0) {
            // Group by type
            const eventsByType = todaysEvents.reduce((acc, event) => {
                acc[event.report_type] = (acc[event.report_type] || 0) + 1;
                return acc;
            }, {});
            
            let html = `<div class="mb-2"><strong>${todaysEvents.length} SPC events today</strong></div>`;
            html += '<div class="row text-center mb-2">';
            
            // Always show all three types with 0 if none
            const tornado = eventsByType.tornado || 0;
            const wind = eventsByType.wind || 0;
            const hail = eventsByType.hail || 0;
            
            html += `<div class="col-4"><div class="h6 text-danger">${tornado}</div><small>Tornado</small></div>`;
            html += `<div class="col-4"><div class="h6 text-warning">${wind}</div><small>Wind</small></div>`;
            html += `<div class="col-4"><div class="h6 text-info">${hail}</div><small>Hail</small></div>`;
            
            html += '</div>';
            
            // Show recent events by location (compact)
            if (todaysEvents.length > 0) {
                html += '<div class="small">';
                todaysEvents.slice(0, 5).forEach(event => {
                    const location = event.location || `${event.county}, ${event.state}`;
                    const typeIcon = event.report_type === 'tornado' ? '🌪️' : 
                                   event.report_type === 'wind' ? '💨' : '🧊';
                    html += `<div class="d-flex justify-content-between border-bottom py-1">
                        <span>${typeIcon} ${location}</span>
                        <span class="text-muted">${event.time_utc || ''}</span>
                    </div>`;
                });
                html += '</div>';
            }
            
            container.innerHTML = html;
        } else {
            container.innerHTML = '<div class="text-center py-3"><div class="h5 text-warning">0</div><small class="text-muted">No SPC events today</small></div>';
        }
    } catch (error) {
        console.error('Error loading today\'s SPC events:', error);
        const container = document.getElementById('todays-spc-events');
        if (container) {
            container.innerHTML = '<p class="text-danger">Error loading today\'s SPC events.</p>';
        }
    }
}

// Get badge color for severity
function getSeverityColor(severity) {
    switch (severity?.toLowerCase()) {
        case 'extreme': return 'danger';
        case 'severe': return 'warning';
        case 'moderate': return 'info';
        case 'minor': return 'secondary';
        default: return 'light';
    }
}

// Update dashboard status
async function updateDashboardStatus() {
    try {
        const response = await fetch('/internal/status');
        const status = await response.json();
        
        // Update timestamp
        updateLastUpdateTime();
        
        // Reload recent alerts
        loadRecentAlerts();
        
    } catch (error) {
        console.error('Error updating dashboard status:', error);
    }
}

// Trigger manual ingestion
async function triggerIngestion() {
    try {
        const response = await fetch('/internal/ingest', { method: 'POST' });
        const result = await response.json();
        
        if (response.ok) {
            showNotification('Manual ingestion triggered successfully', 'success');
            setTimeout(() => {
                updateDashboardStatus();
            }, 2000);
        } else {
            showNotification('Error triggering ingestion: ' + (result.message || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('Error triggering ingestion:', error);
        showNotification('Error triggering ingestion', 'error');
    }
}

// Enrich batch
async function enrichBatch() {
    try {
        const response = await fetch('/internal/enrich-batch', { method: 'POST' });
        const result = await response.json();
        
        if (response.ok) {
            showNotification(`Enrichment batch completed: ${result.enriched || 0} alerts enriched`, 'success');
            setTimeout(() => {
                updateDashboardStatus();
            }, 2000);
        } else {
            showNotification('Error enriching batch: ' + (result.message || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('Error enriching batch:', error);
        showNotification('Error enriching batch', 'error');
    }
}

// View metrics (placeholder for future implementation)
function viewMetrics() {
    showNotification('Metrics view not yet implemented', 'info');
}

// Toggle scheduler
async function toggleScheduler() {
    try {
        const action = dashboardData.scheduler_running ? 'stop' : 'start';
        const response = await fetch('/internal/cron', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: action })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            dashboardData.scheduler_running = !dashboardData.scheduler_running;
            updateStatusIndicator();
            showNotification(`Scheduler ${action}ed successfully`, 'success');
        } else {
            showNotification('Error toggling scheduler: ' + (result.message || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('Error toggling scheduler:', error);
        showNotification('Error toggling scheduler', 'error');
    }
}

// Refresh dashboard
function refreshDashboard() {
    location.reload();
}

// Show notification
function showNotification(message, type = 'info') {
    const alertClass = type === 'error' ? 'alert-danger' : `alert-${type}`;
    const alert = document.createElement('div');
    alert.className = `alert ${alertClass} alert-dismissible fade show position-fixed`;
    alert.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.body.appendChild(alert);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.parentNode.removeChild(alert);
        }
    }, 5000);
}

// Update last update time
function updateLastUpdateTime() {
    const timeElement = document.getElementById('last-update-time');
    if (timeElement) {
        timeElement.textContent = new Date().toLocaleTimeString();
    }
}

// Load integrity verification data
async function loadIntegrityVerification() {
    const container = document.getElementById('integrity-verification-container');
    if (!container) return;
    
    // Show loading state
    container.innerHTML = `
        <div class="text-center py-3">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="text-muted small mt-2">Verifying data integrity...</p>
        </div>
    `;
    
    try {
        const response = await fetch('/internal/spc-verify?format=json&days=7');
        const data = await response.json();
        
        if (response.ok && data.results) {
            displayIntegrityResults(data.results, data.summary);
        } else {
            container.innerHTML = `
                <div class="alert alert-warning">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Unable to verify data integrity. Please try again.
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading integrity verification:', error);
        container.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-times me-2"></i>
                Error loading verification data.
            </div>
        `;
    }
}

// Display integrity verification results
function displayIntegrityResults(results, summary) {
    const container = document.getElementById('integrity-verification-container');
    
    let html = `
        <div class="row mb-3">
            <div class="col-md-3">
                <small class="text-muted">Total Checked</small>
                <div class="h6 mb-0">${summary.total_dates} days</div>
            </div>
            <div class="col-md-3">
                <small class="text-success">Matches</small>
                <div class="h6 mb-0 text-success">${summary.matches}</div>
            </div>
            <div class="col-md-3">
                <small class="text-danger">Mismatches</small>
                <div class="h6 mb-0 text-danger">${summary.mismatches}</div>
            </div>
            <div class="col-md-3">
                <small class="text-muted">Match Rate</small>
                <div class="h6 mb-0">${summary.match_percentage.toFixed(1)}%</div>
            </div>
        </div>
        
        <div class="table-responsive">
            <table class="table table-sm">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>HailyDB</th>
                        <th>SPC Live</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    results.forEach(result => {
        const statusBadge = result.match_status === 'MATCH' 
            ? '<span class="badge bg-success">MATCH</span>'
            : result.match_status === 'MISMATCH'
            ? '<span class="badge bg-danger">MISMATCH</span>'
            : '<span class="badge bg-warning">N/A</span>';
            
        const spcCount = result.spc_live_count !== null ? result.spc_live_count : 'N/A';
        
        html += `
            <tr>
                <td>${result.date}</td>
                <td><span class="badge bg-primary">${result.hailydb_count}</span></td>
                <td><span class="badge bg-secondary">${spcCount}</span></td>
                <td>${statusBadge}</td>
            </tr>
        `;
    });
    
    html += `
                </tbody>
            </table>
        </div>
        
        <div class="text-center mt-3">
            <a href="/internal/spc-verify" class="btn btn-sm btn-outline-primary">
                <i class="fas fa-external-link-alt me-1"></i>View Full Report
            </a>
        </div>
    `;
    
    container.innerHTML = html;
}

console.log('Dashboard JavaScript loaded successfully');