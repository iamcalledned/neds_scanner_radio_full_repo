// ======================================================
//  Scanner "Stats" Page JS
// ======================================================

document.addEventListener('DOMContentLoaded', () => {

    // --- Configuration ------------------------------------
    // Define towns and departments (could also fetch this from an API)
    const towns = [
        { slug: "hopedale", name: "Hopedale", pd: "pd", fd: "fd" },
        { slug: "milford", name: "Milford", pd: "mpd", fd: "mfd" },
        { slug: "bellingham", name: "Bellingham", pd: "bpd", fd: "bfd" },
        { slug: "mendon", name: "Mendon", pd: "mndpd", fd: "mndfd" },
        { slug: "upton", name: "Upton", pd: "uptpd", fd: "uptfd" },
        { slug: "blackstone", name: "Blackstone", pd: "blkpd", fd: "blkfd" },
        { slug: "franklin", name: "Franklin", pd: "frkpd", fd: "frkfd" },
        // Add other towns if needed
    ];

    const departments = [
        { id: 'pd', name: 'Hopedale PD', color: '#60a5fa', townSlug: 'hopedale' }, // blue-400
        { id: 'fd', name: 'Hopedale FD', color: '#f87171', townSlug: 'hopedale' }, // red-400
        { id: 'mpd', name: 'Milford PD', color: '#60a5fa', townSlug: 'milford' },
        { id: 'mfd', name: 'Milford FD', color: '#f87171', townSlug: 'milford' },
        { id: 'bpd', name: 'Bellingham PD', color: '#60a5fa', townSlug: 'bellingham' },
        { id: 'bfd', name: 'Bellingham FD', color: '#f87171', townSlug: 'bellingham' },
        { id: 'mndpd', name: 'Mendon PD', color: '#60a5fa', townSlug: 'mendon' },
        { id: 'mndfd', name: 'Mendon FD', color: '#f87171', townSlug: 'mendon' },
        { id: 'blkpd', name: 'Blackstone PD', color: '#60a5fa', townSlug: 'blackstone' },
        { id: 'blkfd', name: 'Blackstone FD', color: '#f87171', townSlug: 'blackstone' },
        { id: 'uptpd', name: 'Upton PD', color: '#60a5fa', townSlug: 'upton' },
        { id: 'uptfd', name: 'Upton FD', color: '#f87171', townSlug: 'upton' },
        { id: 'frkpd', name: 'Franklin PD', color: '#60a5fa', townSlug: 'franklin' },
        { id: 'frkfd', name: 'Franklin FD', color: '#f87171', townSlug: 'franklin' },
        // Add matching colors for Chart.js
    ];

    // Chart.js instances
    let callsPerHourChartInstance = null;
    let callsPerDeptChartInstance = null;
    let callsPerDayChartInstance = null;

    // --- WebSocket Logic (Same as before) ------------------
    let socket = null;

    function initSocketIO() {
        const serverURL = window.location.origin.includes('iamcalledned.ai')
            ? "https://iamcalledned.ai"
            : "http://localhost:5005"; 
        
        socket = io(serverURL, { transports: ['websocket', 'polling'] });

        socket.on('connect', () => console.log('Socket.IO: Connected!'));
        socket.on('disconnect', () => console.log('Socket.IO: Disconnected.'));
        socket.on('connect_error', (err) => console.error('Socket.IO Connection Error:', err.message));

        socket.on('transmitting_update', (msg) => {
            if (msg && typeof msg === 'object') {
                 console.log(`Socket.IO: Received status update:`, msg);
                 Object.entries(msg).forEach(([deptId, status]) => {
                     updateLiveIndicator(deptId, status);
                 });
            }
        });
    }

    function buildLiveStatusSection() {
        const grid = document.getElementById('live-status-grid');
        if (!grid) return;
        grid.innerHTML = ''; // Clear loading
        departments.forEach(dept => {
            const div = document.createElement('div');
            div.className = 'flex items-center';
            div.innerHTML = `<span class="live-indicator" data-dept="${dept.id}"></span><span>${dept.name}</span>`;
            grid.appendChild(div);
        });
        // Request initial status after building
        // Assuming transmitting_worker sends full status on connect or periodically
    }

    function updateLiveIndicator(deptId, status) {
        const indicator = document.querySelector(`.live-indicator[data-dept="${deptId}"]`);
        if (indicator) {
            indicator.classList.toggle('live', status === 'Y');
        }
    }

    // --- Filter Logic -------------------------------------
    const townFilter = document.getElementById('town-filter');
    const deptFilter = document.getElementById('dept-filter');
    const resetFiltersButton = document.getElementById('reset-filters');

    /** Populates the town dropdown */
    function populateTownFilter() {
        if (!townFilter) return;
        towns.forEach(town => {
            const option = document.createElement('option');
            option.value = town.slug;
            option.textContent = town.name;
            townFilter.appendChild(option);
        });
    }

    /** Populates the department dropdown based on selected town */
    function populateDeptFilter(selectedTownSlug = '') {
        if (!deptFilter) return;
        const currentDeptValue = deptFilter.value; // Remember selection
        deptFilter.innerHTML = '<option value="">All Departments</option>'; // Reset

        const filteredDepts = selectedTownSlug
            ? departments.filter(dept => dept.townSlug === selectedTownSlug)
            : departments; // Show all if no town selected

        filteredDepts.forEach(dept => {
            const option = document.createElement('option');
            option.value = dept.id;
            option.textContent = dept.name;
            deptFilter.appendChild(option);
        });
        
        // Try to restore previous selection if still valid
        if (filteredDepts.some(d => d.id === currentDeptValue)) {
            deptFilter.value = currentDeptValue;
        }
    }

    /** Handles filter changes and reloads data */
    function handleFilterChange() {
        const selectedTown = townFilter.value;
        populateDeptFilter(selectedTown); // Update dependent dropdown
        loadStatsData(); // Reload data with new filters
    }

    /** Resets filters to default and reloads data */
    function resetFilters() {
        townFilter.value = '';
        populateDeptFilter(); // Reset department dropdown to show all
        deptFilter.value = '';
        loadStatsData();
    }


    // --- Data Fetching & Chart Rendering ------------------

    async function loadStatsData() {
        // Show loading messages
        const msgEls = document.querySelectorAll('.chart-container p');
        msgEls.forEach(el => {
            el.textContent = 'Loading chart data...';
            el.style.display = 'flex'; // Make sure it's visible
        });
        document.getElementById('stat-total-today').textContent = '--';
        document.getElementById('stat-total-all-time').textContent = '--';
        document.getElementById('stat-disk-usage').textContent = '--';

        // Destroy old charts before fetching new data
        if (callsPerHourChartInstance) callsPerHourChartInstance.destroy();
        if (callsPerDeptChartInstance) callsPerDeptChartInstance.destroy();
        if (callsPerDayChartInstance) callsPerDayChartInstance.destroy();
        
        // Get filter values
        const selectedTownSlug = townFilter.value;
        const selectedDeptId = deptFilter.value;
        
        // Construct API URL with query parameters
        let apiUrl = '/scanner/api/stats_data';
        const params = new URLSearchParams();
        if (selectedTownSlug) params.append('town', selectedTownSlug);
        if (selectedDeptId) params.append('department', selectedDeptId);
        
        const queryString = params.toString();
        if (queryString) {
            apiUrl += `?${queryString}`;
        }
        
        console.log("Fetching stats from:", apiUrl); // Log the URL being fetched

        try {
            const response = await fetch(apiUrl); 
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            const statsData = await response.json();
            
            // --- Determine Filtered Title Suffix ---
            let titleSuffix = '';
            if (selectedDeptId) {
                const dept = departments.find(d => d.id === selectedDeptId);
                titleSuffix = ` (${dept?.name || selectedDeptId})`;
            } else if (selectedTownSlug) {
                 const town = towns.find(t => t.slug === selectedTownSlug);
                 titleSuffix = ` (${town?.name || selectedTownSlug})`;
            }
            // ------------------------------------

            updateKeyStats(statsData.key_stats); 
            renderCallsPerHourChart(statsData.calls_per_hour_today, titleSuffix);
            renderCallsPerDeptChart(statsData.calls_per_dept_today, titleSuffix);
            renderCallsPerDayChart(statsData.calls_per_day_last_7, titleSuffix);

        } catch (error) {
            console.error("Failed to load stats data:", error);
            msgEls.forEach(el => el.textContent = `Error loading chart: ${error.message}`);
             document.getElementById('stat-total-today').textContent = 'Error';
             document.getElementById('stat-total-all-time').textContent = 'Error';
             document.getElementById('stat-disk-usage').textContent = 'Error';
        }
    }
    
    function updateKeyStats(keyStats) {
        if (!keyStats) return;
        // Key stats might need adjustment based on filters, 
        // assuming API provides filtered totals if applicable
        document.getElementById('stat-total-today').textContent = keyStats.total_calls_today ?? '--';
        document.getElementById('stat-total-all-time').textContent = keyStats.total_calls_all_time ?? '--';
        document.getElementById('stat-disk-usage').textContent = keyStats.total_disk_usage_readable ?? '--';
    }

    /** === MODIFIED: Updated Dark Theme Colors === */
    function getCommonChartOptions() {
        Chart.defaults.color = '#94a3b8'; // Default text color (slate-400)
        Chart.defaults.borderColor = '#334155'; // Default grid line color (slate-700)

        return {
            responsive: true,
            maintainAspectRatio: false, 
            plugins: {
                legend: {
                    labels: { color: '#cbd5e1' } // slate-300
                },
                tooltip: {
                    backgroundColor: 'rgba(30, 41, 59, 0.9)', // slate-800 background
                    titleColor: '#f1f5f9', // slate-100
                    bodyColor: '#cbd5e1', // slate-300
                    borderColor: '#475569', // slate-600 border
                    borderWidth: 1,
                    padding: 10,
                    boxPadding: 3
                }
            },
            scales: { // Applied by default, specific charts can override
                x: {
                    ticks: { color: '#94a3b8' }, // slate-400
                    grid: { color: '#334155' } // slate-700
                },
                y: {
                    ticks: { color: '#94a3b8' }, // slate-400
                    grid: { color: '#334155' } // slate-700
                }
            }
        };
    }
    // --- End Dark Theme Update ---

    /** Renders the 'Calls Per Hour' bar chart. */
    function renderCallsPerHourChart(data, titleSuffix = '') {
        const ctx = document.getElementById('callsPerHourChart');
        const msgEl = document.getElementById('callsPerHourChartMsg');
        const titleEl = document.getElementById('callsPerHourTitle');
        if (!ctx || !msgEl || !titleEl) return;

        titleEl.textContent = `Calls Per Hour (Today)${titleSuffix}`; // Update title
        
         if (!data || !data.labels || data.labels.length === 0) {
             msgEl.textContent = 'No call data for today yet.';
             msgEl.style.display = 'flex';
             return;
         }
         msgEl.style.display = 'none';

        callsPerHourChartInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels, 
                datasets: [{
                    label: 'Calls Recorded',
                    data: data.values, 
                    backgroundColor: 'rgba(56, 189, 248, 0.6)', // scannerBlue opacity
                    borderColor: 'rgba(56, 189, 248, 1)',
                    borderWidth: 1,
                    borderRadius: 4, // Add rounded corners
                    hoverBackgroundColor: 'rgba(56, 189, 248, 0.8)' // Darker on hover
                }]
            },
            options: {
                ...getCommonChartOptions(), // Apply dark theme base
                 scales: {
                     x: { 
                         ticks: { color: '#94a3b8' }, 
                         grid: { display: false } // No vertical grid lines for bars
                      }, 
                     y: { 
                         ticks: { color: '#94a3b8', stepSize: 1 }, 
                         grid: { color: '#334155' },
                         beginAtZero: true
                      } 
                 }
            }
        });
    }

    /** Renders the 'Calls Per Department' doughnut chart. */
    function renderCallsPerDeptChart(data, titleSuffix = '') {
        const ctx = document.getElementById('callsPerDeptChart');
        const msgEl = document.getElementById('callsPerDeptChartMsg');
        const titleEl = document.getElementById('callsPerDeptTitle');
        const container = ctx ? ctx.closest('.glass') : null; // Get container to hide/show
        
        if (!ctx || !msgEl || !titleEl || !container) return;

        titleEl.textContent = `Calls By Department (Today)${titleSuffix}`;

        // Hide this chart if a specific department is selected
        const selectedDeptId = deptFilter.value;
        if (selectedDeptId) {
            container.style.display = 'none'; // Hide the whole section
            if (callsPerDeptChartInstance) callsPerDeptChartInstance.destroy(); // Clean up old chart
            return; 
        } else {
             container.style.display = 'block'; // Ensure visible if resetting filters
        }

         if (!data || !data.labels || data.labels.length === 0) {
             msgEl.textContent = 'No call data for today yet.';
              msgEl.style.display = 'flex';
             return;
         }
         msgEl.style.display = 'none';

        const chartLabels = data.labels.map(id => departments.find(d => d.id === id)?.name || id);
        const chartColors = data.labels.map(id => departments.find(d => d.id === id)?.color || '#94a3b8'); 

        callsPerDeptChartInstance = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: chartLabels, 
                datasets: [{
                    label: 'Calls Today',
                    data: data.values, 
                    backgroundColor: chartColors,
                    borderColor: '#0b0f19', // Match background
                    borderWidth: 2,
                    hoverOffset: 8 // Make hover pop more
                }]
            },
            options: {
                 ...getCommonChartOptions(),
                 plugins: {
                     legend: { 
                         position: 'bottom', 
                         labels: { 
                             color: '#cbd5e1', 
                             padding: 15 // Add padding 
                         } 
                     } 
                 },
                 // Remove scales for doughnut
                 scales: { x: { display: false }, y: { display: false } } 
            }
        });
    }

    /** Renders the 'Calls Per Day' line chart. */
    function renderCallsPerDayChart(data, titleSuffix = '') {
        const ctx = document.getElementById('callsPerDayChart');
        const msgEl = document.getElementById('callsPerDayChartMsg');
        const titleEl = document.getElementById('callsPerDayTitle');
        if (!ctx || !msgEl || !titleEl) return;

         titleEl.textContent = `Total Calls (Last 7 Days)${titleSuffix}`;

         if (!data || !data.labels || data.labels.length === 0) {
             msgEl.textContent = 'Not enough historical data.';
             msgEl.style.display = 'flex';
             return;
         }
         msgEl.style.display = 'none';

        callsPerDayChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels, 
                datasets: [{
                    label: 'Total Calls',
                    data: data.values, 
                    fill: true,
                    backgroundColor: 'rgba(56, 189, 248, 0.2)', // scannerBlue area fill
                    borderColor: 'rgba(56, 189, 248, 1)', // scannerBlue line
                    tension: 0.2, // Smoother curve
                    pointBackgroundColor: 'rgba(56, 189, 248, 1)', // Make points visible
                    pointRadius: 3,
                    pointHoverRadius: 5
                }]
            },
            options: {
                 ...getCommonChartOptions(),
                 scales: {
                     x: { 
                        ticks: { color: '#94a3b8' }, 
                        grid: { display: false } // Hide vertical grid lines for line chart
                     }, 
                     y: { 
                        ticks: { color: '#94a3b8' }, 
                        grid: { color: '#334155' }, 
                        beginAtZero: true 
                     } 
                 }
            }
        });
    }

    // --- Init ---------------------------------------------
    buildLiveStatusSection();
    populateTownFilter();
    populateDeptFilter(); // Initial population with all departments
    loadStatsData(); // Initial data load (all towns/depts)
    initSocketIO();

    // Add event listeners for filters
    if (townFilter) townFilter.addEventListener('change', handleFilterChange);
    if (deptFilter) deptFilter.addEventListener('change', handleFilterChange);
    if (resetFiltersButton) resetFiltersButton.addEventListener('click', resetFilters);
    
    // Auth check and Install button logic are handled by the shared scanner_app_new.js

});

