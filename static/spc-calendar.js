/**
 * SPC Reports Calendar Interface
 * Manages 60-day verification calendar with real-time updates
 */

class SPCCalendar {
    constructor() {
        this.currentData = [];
        this.isLoading = false;
        this.monthNames = [
            'January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December'
        ];
        this.dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.loadCalendarData();
    }
    
    bindEvents() {
        const refreshBtn = document.getElementById('calendar-refresh');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.loadCalendarData());
        }
        
        // Navigation buttons (for future implementation)
        const prevBtn = document.getElementById('calendar-prev');
        const nextBtn = document.getElementById('calendar-next');
        if (prevBtn) prevBtn.addEventListener('click', () => this.navigatePrevious());
        if (nextBtn) nextBtn.addEventListener('click', () => this.navigateNext());
    }
    
    async loadCalendarData() {
        if (this.isLoading) return;
        
        this.isLoading = true;
        this.showLoading();
        
        try {
            const response = await fetch('/api/spc/calendar-verification');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            if (data.status === 'success') {
                this.currentData = data.results;
                this.renderCalendar();
                this.updateDateRange(data.date_range);
            } else {
                throw new Error(data.error || 'Failed to load calendar data');
            }
        } catch (error) {
            console.error('Error loading calendar data:', error);
            this.showError('Failed to load calendar data. Please try again.');
        } finally {
            this.isLoading = false;
        }
    }
    
    showLoading() {
        const loading = document.getElementById('calendar-loading');
        const container = document.getElementById('calendar-container');
        if (loading) loading.style.display = 'block';
        if (container) container.style.display = 'none';
    }
    
    hideLoading() {
        const loading = document.getElementById('calendar-loading');
        const container = document.getElementById('calendar-container');
        if (loading) loading.style.display = 'none';
        if (container) container.style.display = 'block';
    }
    
    showError(message) {
        const container = document.getElementById('calendar-container');
        if (container) {
            container.innerHTML = `
                <div class="text-center py-4">
                    <div class="text-danger">
                        <i class="fas fa-exclamation-triangle"></i>
                        ${message}
                    </div>
                    <button class="btn btn-sm btn-outline-primary mt-2" onclick="spcCalendar.loadCalendarData()">
                        Try Again
                    </button>
                </div>
            `;
            container.style.display = 'block';
        }
        this.hideLoading();
    }
    
    renderCalendar() {
        if (!this.currentData || this.currentData.length === 0) {
            this.showError('No calendar data available');
            return;
        }
        
        // Group data by month
        const groupedData = this.groupDataByMonth(this.currentData);
        const months = Object.keys(groupedData).sort();
        
        // Render up to 2 months (current implementation)
        const month1Container = document.getElementById('calendar-month-1');
        const month2Container = document.getElementById('calendar-month-2');
        
        if (months.length >= 1 && month1Container) {
            month1Container.innerHTML = this.renderMonth(months[0], groupedData[months[0]]);
        }
        
        if (months.length >= 2 && month2Container) {
            month2Container.innerHTML = this.renderMonth(months[1], groupedData[months[1]]);
        } else if (month2Container) {
            month2Container.innerHTML = '';
        }
        
        this.hideLoading();
    }
    
    groupDataByMonth(data) {
        const grouped = {};
        
        data.forEach(item => {
            const date = new Date(item.date);
            const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
            
            if (!grouped[monthKey]) {
                grouped[monthKey] = [];
            }
            grouped[monthKey].push(item);
        });
        
        return grouped;
    }
    
    renderMonth(monthKey, monthData) {
        const [year, month] = monthKey.split('-').map(Number);
        const monthDate = new Date(year, month - 1, 1);
        const monthName = this.monthNames[monthDate.getMonth()];
        
        // Create data lookup by day
        const dataByDay = {};
        monthData.forEach(item => {
            const date = new Date(item.date);
            const day = date.getDate();
            dataByDay[day] = item;
        });
        
        // Generate calendar grid
        const firstDay = new Date(year, month - 1, 1);
        const lastDay = new Date(year, month, 0);
        const startDate = new Date(firstDay);
        startDate.setDate(startDate.getDate() - firstDay.getDay()); // Start from Sunday
        
        let html = `
            <div class="calendar-header">
                ${monthName} ${year}
            </div>
            <div class="calendar-grid">
        `;
        
        // Day headers
        this.dayNames.forEach(day => {
            html += `<div class="calendar-day-header">${day}</div>`;
        });
        
        // Calendar days
        for (let week = 0; week < 6; week++) {
            for (let day = 0; day < 7; day++) {
                const currentDate = new Date(startDate);
                currentDate.setDate(startDate.getDate() + (week * 7) + day);
                
                const dayNum = currentDate.getDate();
                const isCurrentMonth = currentDate.getMonth() === month - 1;
                const isToday = this.isToday(currentDate);
                const dayData = isCurrentMonth ? dataByDay[dayNum] : null;
                
                let dayClass = 'calendar-day';
                if (!isCurrentMonth) dayClass += ' other-month';
                if (isToday) dayClass += ' today';
                
                html += `<div class="${dayClass}">`;
                html += `<div class="calendar-day-number">${dayNum}</div>`;
                
                if (dayData) {
                    html += this.renderDayBadge(dayData);
                    if (dayData.hailydb_count > 0) {
                        html += `<div class="calendar-day-count">${dayData.hailydb_count}</div>`;
                    }
                }
                
                html += '</div>';
            }
            
            // Stop if we've gone past the current month
            if (week > 0 && currentDate.getMonth() !== month - 1) break;
        }
        
        html += '</div>';
        return html;
    }
    
    renderDayBadge(dayData) {
        const { match_status, hailydb_count, spc_live_count, date } = dayData;
        
        let badgeClass = 'calendar-day-badge';
        let badgeText = hailydb_count.toString();
        let clickHandler = '';
        
        switch (match_status) {
            case 'MATCH':
                badgeClass += ' match';
                break;
            case 'MISMATCH':
                badgeClass += ' mismatch';
                clickHandler = `onclick="spcCalendar.reimportDate('${date}')"`;
                break;
            case 'PROCESSING':
                badgeClass += ' processing';
                break;
            default:
                badgeClass += ' unavailable';
                badgeText = '0';
        }
        
        return `<div class="${badgeClass}" ${clickHandler} title="${match_status}: ${hailydb_count} HailyDB, ${spc_live_count || 'N/A'} SPC Live">${badgeText}</div>`;
    }
    
    async reimportDate(date) {
        if (this.isLoading) return;
        
        // Convert date format from YYYY-MM-DD to YYMMDD
        const dateObj = new Date(date);
        const dateStr = dateObj.getFullYear().toString().slice(2) + 
                       String(dateObj.getMonth() + 1).padStart(2, '0') + 
                       String(dateObj.getDate()).padStart(2, '0');
        
        // Update badge to processing state
        this.updateBadgeStatus(date, 'processing');
        
        try {
            const response = await fetch(`/internal/spc-reupload/${dateStr}`, {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            
            if (result.success) {
                // Wait a moment then refresh the specific date
                setTimeout(() => this.refreshDateStatus(date), 2000);
            } else {
                throw new Error(result.error || 'Reimport failed');
            }
        } catch (error) {
            console.error('Error reimporting date:', error);
            // Revert badge state
            this.updateBadgeStatus(date, 'mismatch');
            alert('Failed to reimport data for this date. Please try again.');
        }
    }
    
    updateBadgeStatus(date, status) {
        // Find and update the badge for this specific date
        const badges = document.querySelectorAll('.calendar-day-badge');
        badges.forEach(badge => {
            if (badge.getAttribute('onclick') && badge.getAttribute('onclick').includes(date)) {
                badge.className = `calendar-day-badge ${status}`;
                if (status === 'processing') {
                    badge.removeAttribute('onclick');
                }
            }
        });
    }
    
    async refreshDateStatus(date) {
        // Refresh just this specific date's data
        try {
            const response = await fetch('/api/spc/calendar-verification');
            if (!response.ok) return;
            
            const data = await response.json();
            if (data.status === 'success') {
                const dateData = data.results.find(item => item.date === date);
                if (dateData) {
                    // Update the current data and re-render
                    const index = this.currentData.findIndex(item => item.date === date);
                    if (index >= 0) {
                        this.currentData[index] = dateData;
                        this.renderCalendar();
                    }
                }
            }
        } catch (error) {
            console.error('Error refreshing date status:', error);
        }
    }
    
    updateDateRange(dateRange) {
        const rangeElement = document.getElementById('calendar-range');
        if (rangeElement && dateRange) {
            const startDate = new Date(dateRange.start);
            const endDate = new Date(dateRange.end);
            rangeElement.textContent = `${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}`;
        }
    }
    
    isToday(date) {
        const today = new Date();
        return date.getFullYear() === today.getFullYear() &&
               date.getMonth() === today.getMonth() &&
               date.getDate() === today.getDate();
    }
    
    navigatePrevious() {
        // Future implementation for shifting date range backward
        console.log('Navigate to previous 60 days');
    }
    
    navigateNext() {
        // Future implementation for shifting date range forward
        console.log('Navigate to next 60 days');
    }
}

// Initialize calendar when page loads
let spcCalendar;
document.addEventListener('DOMContentLoaded', function() {
    if (document.getElementById('calendar-container')) {
        spcCalendar = new SPCCalendar();
    }
});