/**
 * NWS Alert Service Dashboard JavaScript
 * Handles real-time updates, charts, and admin controls
 */

// Global variables
let severityChart = null;
let eventChart = null;
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
        dashboardData.severity_stats = dashboardData.severity_stats || {};
        dashboardData.event_stats = dashboardData.event_stats || {};
        dashboardData.scheduler_running = dashboardData.scheduler_running || false;
        
        // Initialize charts
        initializeCharts();
        
        // Update status indicator
        updateStatusIndicator();
        
        // Calculate next poll time
        updateNextPollTime();
        
        console.log('Dashboard initialized successfully');
    } catch (error) {
        console.error('Error initializing dashboard:', error);
        // Set default values on error
        dashboardData = {
            severity_stats: {},
            event_stats: {},
            scheduler_running: false
        };
    }
}

// Initialize Chart.js charts
function initializeCharts() {
    // Severity Distribution Chart
    const severityCtx = document.getElementById('severityChart');
    if (severityCtx && dashboardData.severity_stats) {
        const severityData = dashboardData.severity_stats;
        
        severityChart = new Chart(severityCtx, {
            type: 'doughnut',
            data: {
                labels: Object.keys(severityData),
                datasets: [{
                    data: Object.values(severityData),
                    backgroundColor: [
                        '#FF6384', // Extreme
                        '#FF9F40', // Severe  
                        '#FFCD56', // Moderate
                        '#4BC0C0', // Minor
                        '#9966FF'  // Other
                    ],
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 20,
                            usePointStyle: true
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((context.raw / total) * 100).toFixed(1);
                                return `${context.label}: ${context.raw} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    // Event Types Chart
    const eventCtx = document.getElementById('eventChart');
    if (eventCtx && dashboardData.event_stats) {
        const eventData = dashboardData.event_stats;
        
        eventChart = new Chart(eventCtx, {
            type: 'bar',
            data: {
                labels: Object.keys(eventData).slice(0, 8), // Top 8 events
                datasets: [{
                    label: 'Alert Count',
                    data: Object.values(eventData).slice(0, 8),
                    backgroundColor: '#1E88E5',
                    borderColor: '#1565C0',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            title: function(context) {
                                return context[0].label;
                            },
                            label: function(context) {
                                return `Count: ${context.raw}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0
                        }
                    },
                    x: {
                        ticks: {
                            maxRotation: 45,
                            minRotation: 0
                        }
                    }
                }
            }
        });
    }
}

// Update status indicator
function updateStatusIndicator() {
    const indicator = document.getElementById('status-indicator');
    const schedulerRunning = dashboardData.scheduler_running;
    
    if (indicator) {
        if (schedulerRunning) {
            indicator.textContent = 'Online';
            indicator.className = 'badge bg-success';
        } else {
            indicator.textContent = 'Offline';
            indicator.className = 'badge bg-warning';
        }
    }
}

// Update next poll time
function updateNextPollTime() {
    const nextPollElement = document.getElementById('next-poll');
    if (!nextPollElement) return;
    
    // Calculate next poll (every 5 minutes from last ingestion)
    const lastIngestionElement = document.getElementById('last-ingestion');
    if (lastIngestionElement && lastIngestionElement.textContent !== 'Never') {
        const lastIngestion = new Date(lastIngestionElement.textContent.replace(' UTC', 'Z'));
        const nextPoll = new Date(lastIngestion.getTime() + (5 * 60 * 1000));
        const now = new Date();
        
        if (nextPoll > now) {
            const diffMs = nextPoll - now;
            const diffMins = Math.floor(diffMs / 60000);
            const diffSecs = Math.floor((diffMs % 60000) / 1000);
            nextPollElement.textContent = `${diffMins}m ${diffSecs}s`;
            
            // Update every second
            setTimeout(updateNextPollTime, 1000);
        } else {
            nextPollElement.textContent = 'Now';
            setTimeout(updateNextPollTime, 5000);
        }
    } else {
        nextPollElement.textContent = 'Unknown';
    }
}

// Load recent alerts table
async function loadRecentAlerts() {
    try {
        const response = await fetch('/alerts?per_page=10&format=json');
        const data = await response.json();
        
        const container = document.getElementById('recent-alerts-table');
        if (!container) return;
        
        if (data.alerts && data.alerts.length > 0) {
            let tableHTML = `
                <div class="table-responsive">
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Event</th>
                                <th>Severity</th>
                                <th>Area</th>
                                <th>Effective</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            data.alerts.forEach(alert => {
                const effectiveDate = alert.effective ? 
                    new Date(alert.effective).toLocaleDateString('en-US', {
                        month: 'short',
                        day: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit'
                    }) : 'N/A';
                
                const statusBadge = alert.ai_summary ? 
                    '<span class="badge bg-success">Enriched</span>' :
                    '<span class="badge bg-secondary">Raw</span>';
                
                tableHTML += `
                    <tr>
                        <td>
                            <a href="/alerts/${alert.id}" class="text-decoration-none">
                                ${alert.event || 'Unknown Event'}
                            </a>
                        </td>
                        <td>
                            <span class="severity-badge severity-${(alert.severity || 'unknown').toLowerCase()}">
                                ${alert.severity || 'N/A'}
                            </span>
                        </td>
                        <td class="text-truncate" style="max-width: 200px;">
                            ${alert.area_desc || 'Area not specified'}
                        </td>
                        <td>${effectiveDate}</td>
                        <td>${statusBadge}</td>
                    </tr>
                `;
            });
            
            tableHTML += `
                        </tbody>
                    </table>
                </div>
            `;
            
            container.innerHTML = tableHTML;
        } else {
            container.innerHTML = `
                <div class="text-center py-4 text-muted">
                    <i class="fas fa-inbox fa-2x mb-2"></i>
                    <p>No recent alerts found</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading recent alerts:', error);
        const container = document.getElementById('recent-alerts-table');
        if (container) {
            container.innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Error loading recent alerts. Please try again.
                </div>
            `;
        }
    }
}

// Update dashboard status
async function updateDashboardStatus() {
    try {
        const response = await fetch('/internal/status');
        const status = await response.json();
        
        // Update last update time
        const lastUpdateElement = document.getElementById('last-update');
        if (lastUpdateElement) {
            lastUpdateElement.textContent = new Date().toLocaleString();
        }
        
        // Update status indicator
        const indicator = document.getElementById('status-indicator');
        if (indicator) {
            if (status.scheduler_running) {
                indicator.textContent = 'Online';
                indicator.className = 'badge bg-success';
            } else {
                indicator.textContent = 'Offline';
                indicator.className = 'badge bg-warning';
            }
        }
        
        // Update last ingestion time
        const lastIngestionElement = document.getElementById('last-ingestion');
        if (lastIngestionElement && status.last_ingestion) {
            const date = new Date(status.last_ingestion);
            lastIngestionElement.textContent = date.toLocaleString() + ' UTC';
        }
        
    } catch (error) {
        console.error('Error updating dashboard status:', error);
    }
}

// Trigger manual ingestion
async function triggerIngestion() {
    const button = event.target;
    const originalText = button.innerHTML;
    
    try {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Ingesting...';
        
        const response = await fetch('/internal/cron', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ action: 'trigger' })
        });
        
        const result = await response.json();
        
        if (result.status === 'triggered') {
            showNotification(`Ingestion completed! ${result.ingested_count} new alerts processed.`, 'success');
            setTimeout(() => {
                refreshDashboard();
            }, 2000);
        } else {
            showNotification(`Ingestion failed: ${result.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Error triggering ingestion:', error);
        showNotification('Error triggering ingestion. Please try again.', 'error');
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
}

// Enrich batch of alerts
async function enrichBatch() {
    const button = event.target;
    const originalText = button.innerHTML;
    
    try {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Enriching...';
        
        const response = await fetch('/api/alerts/enrich-batch', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ limit: 50 })
        });
        
        const result = await response.json();
        
        if (result.status === 'success') {
            showNotification(`Batch enrichment completed! ${result.enriched} alerts enriched, ${result.failed} failed.`, 'success');
            setTimeout(() => {
                refreshDashboard();
            }, 2000);
        } else {
            showNotification(`Batch enrichment failed: ${result.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Error enriching batch:', error);
        showNotification('Error enriching alerts. Please try again.', 'error');
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
}

// View metrics
function viewMetrics() {
    window.open('/internal/metrics', '_blank');
}

// Toggle scheduler
async function toggleScheduler() {
    const button = event.target;
    const originalText = button.innerHTML;
    
    try {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Updating...';
        
        const action = dashboardData.scheduler_running ? 'stop' : 'start';
        
        const response = await fetch('/internal/cron', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ action: action })
        });
        
        const result = await response.json();
        
        if (result.status === action + 'ted' || result.status === action + 'ed') {
            showNotification(`Scheduler ${result.status}!`, 'success');
            dashboardData.scheduler_running = !dashboardData.scheduler_running;
            updateStatusIndicator();
        } else {
            showNotification('Error updating scheduler status.', 'error');
        }
        
    } catch (error) {
        console.error('Error toggling scheduler:', error);
        showNotification('Error updating scheduler. Please try again.', 'error');
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
}

// Refresh dashboard
function refreshDashboard() {
    window.location.reload();
}

// Show notification
function showNotification(message, type = 'info') {
    const alertClass = type === 'error' ? 'danger' : type;
    const iconClass = type === 'error' ? 'exclamation-circle' : 
                     type === 'success' ? 'check-circle' : 'info-circle';
    
    const notification = document.createElement('div');
    notification.className = `alert alert-${alertClass} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 1050; min-width: 300px;';
    notification.innerHTML = `
        <i class="fas fa-${iconClass} me-2"></i>
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.body.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 5000);
}

// Update last update time
function updateLastUpdateTime() {
    const lastUpdateElement = document.getElementById('last-update');
    if (lastUpdateElement) {
        lastUpdateElement.textContent = new Date().toLocaleString();
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    updateLastUpdateTime();
    
    // Set up periodic updates
    setInterval(updateDashboardStatus, 30000); // Every 30 seconds
    setInterval(updateLastUpdateTime, 60000);  // Every minute
    
    console.log('Dashboard JavaScript loaded successfully');
});
