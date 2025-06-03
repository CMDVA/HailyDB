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

// Load recent alerts into table
async function loadRecentAlerts() {
    try {
        const response = await fetch('/alerts?format=json&per_page=10');
        const data = await response.json();
        
        const tableContainer = document.getElementById('recent-alerts-table');
        if (!tableContainer) return;
        
        if (data.alerts && data.alerts.length > 0) {
            let html = '<div class="table-responsive"><table class="table table-striped">';
            html += '<thead><tr><th>Event</th><th>Area</th><th>Severity</th><th>Time</th></tr></thead>';
            html += '<tbody>';
            
            data.alerts.forEach(alert => {
                const time = new Date(alert.effective).toLocaleString();
                html += `<tr>
                    <td><a href="/alerts/${alert.id}">${alert.event}</a></td>
                    <td>${alert.area_desc ? alert.area_desc.substring(0, 50) + '...' : 'N/A'}</td>
                    <td><span class="badge bg-${getSeverityColor(alert.severity)}">${alert.severity || 'N/A'}</span></td>
                    <td>${time}</td>
                </tr>`;
            });
            
            html += '</tbody></table></div>';
            tableContainer.innerHTML = html;
        } else {
            tableContainer.innerHTML = '<p class="text-muted">No recent alerts found.</p>';
        }
    } catch (error) {
        console.error('Error loading recent alerts:', error);
        const tableContainer = document.getElementById('recent-alerts-table');
        if (tableContainer) {
            tableContainer.innerHTML = '<p class="text-danger">Error loading alerts.</p>';
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