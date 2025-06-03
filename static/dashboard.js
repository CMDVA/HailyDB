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
        const response = await fetch('/alerts?limit=10');
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
                    <td><a href="/alert/${alert.id}">${alert.event}</a></td>
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

console.log('Dashboard JavaScript loaded successfully');