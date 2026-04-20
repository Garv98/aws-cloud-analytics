// ========================================
// Nimbus Insights - World-Class Analytics
// ========================================

const config = window.APP_CONFIG || {};
const apiBaseUrl = (config.apiBaseUrl || "").replace(/\/$/, "");
const apiHostRoot = apiBaseUrl.replace(/\/(create-job|prod|jobs)$/i, "");

function getChatEndpoint(jobId) {
  // Some console-created APIs expose chat at /chat/{jobId} while jobs live under /create-job/jobs/{jobId}
  if (/\/(create-job|jobs)$/i.test(apiBaseUrl)) {
    return `${apiHostRoot}/chat/${jobId}`;
  }
  return `${apiBaseUrl}/chat/${jobId}`;
}

// Supported file extensions
const SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls'];

// Chart instances storage for cleanup
let chartInstances = {};

// DOM Elements
const uploadForm = document.getElementById("upload-form");
const fileInput = document.getElementById("csv-file");
const dropZone = document.getElementById("drop-zone");
const browseBtn = document.getElementById("browse-btn");
const removeFileBtn = document.getElementById("remove-file");
const runBtn = document.getElementById("run-btn");

const filePreview = document.getElementById("file-preview");
const fileName = document.getElementById("file-name");

const uploadFeedback = document.getElementById("upload-feedback");
const progressContainer = document.getElementById("progress-container");
const progressFill = document.getElementById("progress-fill");
const progressText = document.getElementById("progress-text");

const statusSection = document.getElementById("status-section");
const statusPill = document.getElementById("status-pill");
const statusText = document.getElementById("status-text");
const jobIdText = document.getElementById("job-id");

const resultsSection = document.getElementById("results-section");
const chatSidebar = document.getElementById("chat-sidebar");

// ========================================
// Drag & Drop Functionality
// ========================================

dropZone.addEventListener("click", () => fileInput.click());


browseBtn.addEventListener("click", (e) => {
  e.preventDefault();
  e.stopPropagation();
  fileInput.click();
});

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("drag-over");
});

dropZone.addEventListener("dragleave", () => {
  dropZone.classList.remove("drag-over");
});

dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("drag-over");
  
  const files = e.dataTransfer.files;
  if (files.length > 0) {
    fileInput.files = files;
    handleFileSelect(files[0]);
  }
});

fileInput.addEventListener("change", (e) => {
  const file = e.target.files[0];
  if (file) {
    handleFileSelect(file);
  }
});

removeFileBtn.addEventListener("click", (e) => {
  e.stopPropagation();
  fileInput.value = "";
  filePreview.classList.add("hidden");
  runBtn.disabled = true;
  clearFeedback();
});

function handleFileSelect(file) {
  const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
  if (!SUPPORTED_EXTENSIONS.includes(ext)) {
    setFeedback(`Unsupported format. Please use: ${SUPPORTED_EXTENSIONS.join(', ')}`, "error");
    fileInput.value = "";
    return;
  }
  
  fileName.textContent = file.name;
  filePreview.classList.remove("hidden");
  runBtn.disabled = false;
  
  const typeLabel = ext === '.csv' ? 'CSV' : ext === '.xlsx' ? 'Excel (XLSX)' : 'Excel (XLS)';
  setFeedback(`${typeLabel} file selected: ${formatFileSize(file.size)}`, "info");
}

function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

// ========================================
// UI Helper Functions
// ========================================

function setFeedback(message, type = "info") {
  uploadFeedback.textContent = message;
  uploadFeedback.className = `feedback-message ${type}`;
  uploadFeedback.style.display = message ? "block" : "none";
}

function clearFeedback() {
  uploadFeedback.style.display = "none";
}

function setProgress(percent, message = "") {
  progressContainer.classList.remove("hidden");
  progressFill.style.width = `${percent}%`;
  progressText.textContent = message;
}

function hideProgress() {
  progressContainer.classList.add("hidden");
}

function setStatus(status, detail = "") {
  const value = (status || "WAITING").toUpperCase();
  statusPill.innerHTML = `<span class="status-dot"></span>${value}`;
  statusPill.className = `status-badge status-${status.toLowerCase()}`;
  statusText.textContent = detail || getStatusMessage(status);
  statusSection.classList.remove("hidden");
}

function getStatusMessage(status) {
  const messages = {
    CREATED: "Job created successfully",
    UPLOADED: "File uploaded to S3",
    PROCESSING: "Lambda analyzing your data...",
    COMPLETED: "Analysis complete!",
    FAILED: "Analysis failed",
    WAITING: "No active job"
  };
  return messages[status] || "";
}

function formatNumber(num, decimals = 2) {
  if (num === null || num === undefined) return "-";
  if (typeof num !== 'number') return num;
  if (Math.abs(num) >= 1e9) return (num / 1e9).toFixed(1) + 'B';
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + 'M';
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + 'K';
  return num.toFixed(decimals);
}

// ========================================
// Analytics Rendering Functions
// ========================================

function renderAnalytics(analytics) {
  // Clear previous charts
  Object.values(chartInstances).forEach(chart => chart.destroy());
  chartInstances = {};
  
  resultsSection.innerHTML = '';
  
  // Normalize field names (backend uses camelCase, frontend expects snake_case in some places)
  const normalizedAnalytics = {
    ...analytics,
    data_quality: analytics.dataQuality || analytics.data_quality || {},
    file_type: analytics.fileType || analytics.file_type || 'csv',
    row_count: analytics.overview?.totalRows || analytics.row_count || 0,
    column_count: analytics.overview?.totalColumns || analytics.column_count || 0,
    numeric_columns: analytics.overview?.numericColumns || analytics.numeric_columns || 0,
    categorical_columns: analytics.overview?.categoricalColumns || analytics.categorical_columns || 0,
    datetime_columns: analytics.overview?.datetimeColumns || analytics.datetime_columns || 0,
  };
  
  // Render each section
  renderDataQuality(normalizedAnalytics);
  renderInsights(analytics.insights || []);
  renderOverview(normalizedAnalytics);
  renderCharts(analytics);
  renderColumns(analytics.columns || {});
  renderCorrelations(analytics.correlations || []);
  
  // Render advanced analytics
  const advancedSection = renderAdvancedAnalytics(analytics);
  if (advancedSection) {
    resultsSection.appendChild(advancedSection);
  }
  
  resultsSection.classList.remove("hidden");
  
  setChatEnabled(true);
  addBotMessage("Your analysis is complete! Feel free to ask me any questions about your data.");
}

function renderDataQuality(analytics) {
  const quality = analytics.data_quality || {};
  const score = quality.qualityScore || quality.score || 0;
  const scoreClass = score >= 80 ? 'success' : score >= 60 ? 'warning' : 'error';
  
  const section = document.createElement('div');
  section.className = 'quality-section card';
  section.innerHTML = `
    <h2><span class="icon quality-icon"></span> Data Quality Score</h2>
    <div class="quality-content">
      <div class="quality-score-container">
        <div class="quality-score">${Math.round(score)}%</div>
        <div class="quality-label">${getQualityLabel(score)}</div>
      </div>
      <div class="quality-details">
        <div class="quality-item">
          <span class="quality-item-label">Completeness</span>
          <span class="quality-item-value">${(quality.completeness || 0).toFixed(1)}%</span>
        </div>
        <div class="quality-item">
          <span class="quality-item-label">Missing Values</span>
          <span class="quality-item-value">${quality.missingCells || quality.missing_count || 0}</span>
        </div>
        <div class="quality-item">
          <span class="quality-item-label">Total Cells</span>
          <span class="quality-item-value">${quality.totalCells || 0}</span>
        </div>
        <div class="quality-item">
          <span class="quality-item-label">File Type</span>
          <span class="quality-item-value">${(analytics.file_type || 'csv').toUpperCase()}</span>
        </div>
      </div>
    </div>
  `;
  resultsSection.appendChild(section);
}

function getQualityLabel(score) {
  if (score >= 90) return 'Excellent';
  if (score >= 80) return 'Good';
  if (score >= 60) return 'Fair';
  if (score >= 40) return 'Poor';
  return 'Critical';
}

function renderInsights(insights) {
  if (!insights.length) return;
  
  const section = document.createElement('div');
  section.className = 'insights-section';
  section.innerHTML = `
    <div class="card insights-card">
      <h2><span class="icon insights-icon"></span> Key Insights</h2>
      <div class="insights-list">
        ${insights.map(insight => `
          <div class="insight-item">
            <span class="insight-icon insight-icon-${getInsightIcon(insight)}"></span>
            <span class="insight-text">${insight}</span>
          </div>
        `).join('')}
      </div>
    </div>
  `;
  resultsSection.appendChild(section);
}

function getInsightIcon(insight) {
  const text = insight.toLowerCase();
  if (text.includes('correlation')) return 'link';
  if (text.includes('outlier')) return 'alert';
  if (text.includes('missing')) return 'question';
  if (text.includes('skew')) return 'trend';
  if (text.includes('unique')) return 'target';
  if (text.includes('range')) return 'measure';
  return 'pin';
}

function renderOverview(analytics) {
  const section = document.createElement('div');
  section.className = 'overview-section';
  section.innerHTML = `
    <div class="card">
      <h2><span class="icon overview-icon"></span> Dataset Overview</h2>
      <div class="overview-grid">
        <div class="overview-stat">
          <div class="overview-stat-value">${formatNumber(analytics.row_count, 0)}</div>
          <div class="overview-stat-label">Total Rows</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-value">${analytics.column_count || 0}</div>
          <div class="overview-stat-label">Columns</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-value">${analytics.numeric_columns || 0}</div>
          <div class="overview-stat-label">Numeric</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-value">${analytics.categorical_columns || 0}</div>
          <div class="overview-stat-label">Categorical</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-value">${analytics.datetime_columns || 0}</div>
          <div class="overview-stat-label">Datetime</div>
        </div>
        <div class="overview-stat">
          <div class="overview-stat-value">${(analytics.memory_usage || 'N/A')}</div>
          <div class="overview-stat-label">Memory</div>
        </div>
      </div>
    </div>
  `;
  resultsSection.appendChild(section);
}

function renderCharts(analytics) {
  const columns = analytics.columns || {};
  const numericCols = Object.entries(columns).filter(([_, info]) => info.type === 'numeric');
  const categoricalCols = Object.entries(columns).filter(([_, info]) => info.type === 'categorical');
  
  if (numericCols.length === 0 && categoricalCols.length === 0) return;
  
  const section = document.createElement('div');
  section.className = 'charts-section';
  section.innerHTML = `
    <div class="section-header">
      <h2><span class="icon charts-icon"></span> Visualizations</h2>
    </div>
    <div class="charts-grid" id="charts-grid"></div>
  `;
  resultsSection.appendChild(section);
  
  const chartsGrid = document.getElementById('charts-grid');
  
  // Create distribution charts for numeric columns (limit to 4)
  numericCols.slice(0, 4).forEach(([colName, info], index) => {
    const chartCard = document.createElement('div');
    chartCard.className = 'chart-card';
    chartCard.style.animationDelay = `${index * 0.1}s`;
    chartCard.innerHTML = `
      <div class="chart-card-header">
        <span class="chart-card-title">${colName}</span>
        <span class="chart-card-type">Distribution</span>
      </div>
      <div class="chart-wrapper">
        <canvas id="chart-${index}"></canvas>
      </div>
    `;
    chartsGrid.appendChild(chartCard);
    
    // Create histogram
    setTimeout(() => createHistogram(`chart-${index}`, colName, info), 100);
  });
  
  // Create bar chart for first categorical column with top values
  categoricalCols.slice(0, 2).forEach(([colName, info], catIndex) => {
    const topValues = info.stats?.topValues || info.top_values || [];
    if (topValues.length === 0) return;
    
    const index = numericCols.length + catIndex;
    const chartCard = document.createElement('div');
    chartCard.className = 'chart-card';
    chartCard.style.animationDelay = `${index * 0.1}s`;
    chartCard.innerHTML = `
      <div class="chart-card-header">
        <span class="chart-card-title">${colName}</span>
        <span class="chart-card-type">Categories</span>
      </div>
      <div class="chart-wrapper">
        <canvas id="chart-cat-${catIndex}"></canvas>
      </div>
    `;
    chartsGrid.appendChild(chartCard);
    
    setTimeout(() => createBarChart(`chart-cat-${catIndex}`, colName, topValues), 100);
  });
}

function createHistogram(canvasId, colName, info) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  
  // Generate histogram data from statistics
  const stats = info.stats || {};
  const min = stats.min || 0;
  const max = stats.max || 100;
  const mean = stats.mean || (min + max) / 2;
  const std = stats.std || (max - min) / 6;
  
  // Generate approximate distribution (normal-ish) for visualization
  const bins = 12;
  const binWidth = (max - min) / bins;
  const labels = [];
  const data = [];
  
  for (let i = 0; i < bins; i++) {
    const binStart = min + i * binWidth;
    const binEnd = binStart + binWidth;
    labels.push(`${formatNumber(binStart, 1)}`);
    
    // Approximate frequency using normal distribution
    const binMid = (binStart + binEnd) / 2;
    const z = (binMid - mean) / (std || 1);
    const freq = Math.exp(-0.5 * z * z) * 100;
    data.push(Math.max(freq, 5));
  }
  
  // Create gradient for bars
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, 'rgba(99, 102, 241, 0.9)');
  gradient.addColorStop(1, 'rgba(139, 92, 246, 0.6)');
  
  chartInstances[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: colName,
        data: data,
        backgroundColor: gradient,
        borderColor: 'rgba(99, 102, 241, 1)',
        borderWidth: 2,
        borderRadius: 6,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 1000,
        easing: 'easeOutQuart'
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(17, 24, 39, 0.9)',
          titleColor: '#fff',
          bodyColor: '#fff',
          padding: 12,
          cornerRadius: 8,
          displayColors: false,
          callbacks: {
            title: (items) => `Range: ${items[0].label}`,
            label: (item) => `Frequency: ${item.raw.toFixed(1)}`
          }
        }
      },
      scales: {
        x: {
          title: { 
            display: true, 
            text: 'Value Range',
            color: '#6b7280',
            font: { weight: 600, size: 12 }
          },
          grid: { display: false },
          ticks: { 
            color: '#9ca3af',
            font: { size: 11 }
          }
        },
        y: {
          title: { 
            display: true, 
            text: 'Frequency',
            color: '#6b7280',
            font: { weight: 600, size: 12 }
          },
          beginAtZero: true,
          grid: { 
            color: 'rgba(0,0,0,0.05)',
            drawBorder: false
          },
          ticks: { 
            color: '#9ca3af',
            font: { size: 11 }
          }
        }
      }
    }
  });
}

function createBarChart(canvasId, colName, topValues) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  const labels = topValues.map(v => v.value || v[0] || 'Unknown');
  const data = topValues.map(v => v.count || v[1] || 0);
  
  const colors = [
    'rgba(99, 102, 241, 0.85)',
    'rgba(139, 92, 246, 0.85)',
    'rgba(236, 72, 153, 0.85)',
    'rgba(34, 197, 94, 0.85)',
    'rgba(245, 158, 11, 0.85)',
    'rgba(20, 184, 166, 0.85)',
    'rgba(168, 85, 247, 0.85)',
    'rgba(239, 68, 68, 0.85)',
  ];
  
  // Create gradient background
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, 'rgba(99, 102, 241, 0.1)');
  gradient.addColorStop(1, 'rgba(139, 92, 246, 0.05)');
  
  chartInstances[canvasId] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: data,
        backgroundColor: colors.slice(0, labels.length),
        borderColor: '#fff',
        borderWidth: 3,
        hoverOffset: 8,
        hoverBorderColor: '#fff'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '65%',
      animation: {
        animateRotate: true,
        animateScale: true,
        duration: 1000,
        easing: 'easeOutQuart'
      },
      plugins: {
        legend: {
          position: 'right',
          labels: {
            boxWidth: 16,
            padding: 16,
            font: { size: 12, weight: 500 },
            color: '#374151',
            usePointStyle: true,
            pointStyle: 'circle'
          }
        },
        tooltip: {
          backgroundColor: 'rgba(17, 24, 39, 0.9)',
          titleColor: '#fff',
          bodyColor: '#fff',
          padding: 16,
          cornerRadius: 12,
          displayColors: true,
          callbacks: {
            label: (item) => {
              const total = data.reduce((a, b) => a + b, 0);
              const percentage = ((item.raw / total) * 100).toFixed(1);
              return `${item.label}: ${item.raw} (${percentage}%)`;
            }
          }
        }
      }
    }
  });
}

function renderColumns(columns) {
  const columnEntries = Object.entries(columns);
  if (columnEntries.length === 0) return;
  
  const section = document.createElement('div');
  section.className = 'columns-section';
  section.innerHTML = `
    <div class="section-header">
      <h2><span class="icon columns-icon"></span> Column Analysis</h2>
    </div>
    <div class="columns-grid" id="columns-grid"></div>
  `;
  resultsSection.appendChild(section);
  
  const columnsGrid = document.getElementById('columns-grid');
  
  columnEntries.forEach(([colName, info], index) => {
    const card = document.createElement('div');
    card.className = 'column-card';
    card.style.animationDelay = `${index * 0.05}s`;
    
    let statsHtml = '';
    const stats = info.stats || {};
    
    if (info.type === 'numeric') {
      statsHtml = `
        <div class="column-stats">
          <div class="column-stat">
            <span class="column-stat-label">Mean</span>
            <span class="column-stat-value">${formatNumber(stats.mean)}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Median</span>
            <span class="column-stat-value">${formatNumber(stats.median)}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Std Dev</span>
            <span class="column-stat-value">${formatNumber(stats.std)}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Min</span>
            <span class="column-stat-value">${formatNumber(stats.min)}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Max</span>
            <span class="column-stat-value">${formatNumber(stats.max)}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Outliers</span>
            <span class="column-stat-value">${stats.outlierCount || stats.outliers || 0}</span>
          </div>
        </div>
      `;
    } else if (info.type === 'categorical') {
      const topValues = stats.topValues || info.top_values || [];
      statsHtml = `
        <div class="column-stats">
          <div class="column-stat">
            <span class="column-stat-label">Unique</span>
            <span class="column-stat-value">${stats.uniqueCount || stats.unique || info.unique || 0}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Mode</span>
            <span class="column-stat-value">${stats.mode || '-'}</span>
          </div>
        </div>
        ${renderTopValues(topValues)}
      `;
    } else {
      statsHtml = `
        <div class="column-stats">
          <div class="column-stat">
            <span class="column-stat-label">Unique</span>
            <span class="column-stat-value">${info.unique || 0}</span>
          </div>
          <div class="column-stat">
            <span class="column-stat-label">Missing</span>
            <span class="column-stat-value">${info.missing || 0}</span>
          </div>
        </div>
      `;
    }
    
    card.innerHTML = `
      <div class="column-header">
        <span class="column-name">${colName}</span>
        <span class="column-type ${info.type}">${info.type}</span>
      </div>
      ${statsHtml}
    `;
    
    columnsGrid.appendChild(card);
  });
}

function renderTopValues(topValues) {
  if (!topValues || topValues.length === 0) return '';
  
  const total = topValues.reduce((sum, v) => sum + (v.count || v[1] || 0), 0);
  
  return `
    <div class="top-values-list">
      <div class="top-values-title">Top Values</div>
      ${topValues.slice(0, 5).map(v => {
        const value = v.value || v[0] || 'Unknown';
        const count = v.count || v[1] || 0;
        const percent = total > 0 ? (count / total * 100).toFixed(1) : 0;
        return `
          <div class="top-value-item">
            <span class="top-value-label" title="${value}">${value}</span>
            <div class="top-value-bar">
              <div class="top-value-fill" style="width: ${percent}%"></div>
            </div>
            <span class="top-value-percent">${percent}%</span>
          </div>
        `;
      }).join('')}
    </div>
  `;
}

function renderCorrelations(correlations) {
  if (!correlations || correlations.length === 0) return;
  
  // Filter significant correlations
  const significant = correlations.filter(c => Math.abs(c.correlation || c[2] || 0) >= 0.3);
  if (significant.length === 0) return;
  
  const section = document.createElement('div');
  section.className = 'correlations-section';
  section.innerHTML = `
    <div class="card">
      <h2><span class="icon correlations-icon"></span> Correlations</h2>
      <p class="muted">Showing correlations with |r| >= 0.3</p>
      <div class="correlations-list">
        ${significant.slice(0, 10).map(corr => {
          const col1 = corr.column1 || corr[0] || '';
          const col2 = corr.column2 || corr[1] || '';
          const value = corr.correlation || corr[2] || 0;
          const absValue = Math.abs(value);
          const colorClass = value > 0 ? 'positive' : 'negative';
          
          return `
            <div class="correlation-item">
              <div class="correlation-columns">
                <span class="correlation-column">${col1}</span>
                <span class="correlation-arrow">â†”</span>
                <span class="correlation-column">${col2}</span>
              </div>
              <div class="correlation-bar">
                <div class="correlation-bar-fill ${colorClass}" style="width: ${absValue * 100}%"></div>
              </div>
              <span class="correlation-value ${colorClass}">${value.toFixed(3)}</span>
            </div>
          `;
        }).join('')}
      </div>
    </div>
  `;
  resultsSection.appendChild(section);
}

// ========================================
// API Functions
// ========================================

function getFileType(fileName) {
  const ext = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
  if (ext === '.xlsx') return 'xlsx';
  if (ext === '.xls') return 'xls';
  return 'csv';
}

async function createJob(fileType = 'csv') {
  const response = await fetch(`${apiBaseUrl}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fileType }),
  });

  if (!response.ok) {
    throw new Error(`Failed to create job (HTTP ${response.status})`);
  }
  return response.json();
}

async function uploadFile(url, file) {
  const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
  let contentType = 'text/csv';
  
  if (ext === '.xlsx') {
    contentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  } else if (ext === '.xls') {
    contentType = 'application/vnd.ms-excel';
  }
  
  const response = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": contentType },
    body: file,
  });
  
  if (!response.ok) {
    throw new Error(`Upload failed (HTTP ${response.status})`);
  }
}

async function getJob(jobId) {
  const response = await fetch(`${apiBaseUrl}/jobs/${jobId}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch job status (HTTP ${response.status})`);
  }
  return response.json();
}

async function getResult(jobId) {
  const response = await fetch(`${apiBaseUrl}/jobs/${jobId}/result`);
  if (!response.ok) {
    throw new Error(`Failed to fetch results (HTTP ${response.status})`);
  }
  return response.json();
}

async function pollUntilDone(jobId) {
  const maxAttempts = 90;
  const pollInterval = 2000;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const job = await getJob(jobId);
    const status = (job.status || "").toUpperCase();

    setStatus(status, job.errorMessage || "");
    
    const progress = Math.min(30 + (attempt / maxAttempts) * 60, 90);
    setProgress(progress, `Analyzing data... (${attempt + 1}/${maxAttempts})`);

    if (status === "COMPLETED") {
      setProgress(100, "Complete!");
      return;
    }
    
    if (status === "FAILED") {
      throw new Error(job.errorMessage || "Analysis failed");
    }
    
    await new Promise((resolve) => setTimeout(resolve, pollInterval));
  }

  throw new Error("Timeout: Analysis took too long.");
}

// ========================================
// Advanced Analytics Rendering
// ========================================

function renderAdvancedAnalytics(analytics) {
  const section = document.createElement('div');
  section.className = 'advanced-analytics-section';
  section.innerHTML = `
    <div class="advanced-analytics-header">
      <div class="advanced-analytics-title">
        <div class="advanced-analytics-kicker">Deep dive</div>
        <h2><span class="icon advanced-icon"></span> Advanced Analytics</h2>
        <p class="advanced-analytics-subtitle">Statistical tests, pattern detection, and clustering across numeric columns.</p>
      </div>
      <div class="advanced-analytics-meta" id="advanced-analytics-meta"></div>
    </div>
    <div class="analytics-grid"></div>
  `;

  const analyticsGrid = section.querySelector('.analytics-grid');
  let cardCount = 0;

  const columns = analytics.columns || {};
  
  const statisticalTests = document.createElement('div');
  statisticalTests.className = 'analytics-card analytics-card--stats';
  statisticalTests.innerHTML = `
    <h3><span class="icon stats-icon"></span> Statistical Tests</h3>
    <div class="statistical-tests-content"></div>
  `;
  
  let hasStatTests = false;
  const testsContent = statisticalTests.querySelector('.statistical-tests-content');
  
  for (const [colName, colInfo] of Object.entries(columns)) {
    if (colInfo.type === 'numeric' && colInfo.stats) {
      const stats = colInfo.stats;
      const normalityTest = stats.normalityTest;
      
      if (normalityTest && normalityTest.p_value !== null) {
        hasStatTests = true;
        const isNormal = normalityTest.is_normal;
        testsContent.innerHTML += `
          <div class="stat-test-item">
            <div class="stat-test-header">
              <span class="stat-test-column">${colName}</span>
              <span class="stat-test-badge ${isNormal ? 'normal' : 'not-normal'}">
                ${isNormal ? 'Normal' : 'Not Normal'}
              </span>
            </div>
            <div class="stat-test-details">
              <span>p-value: ${normalityTest.p_value}</span>
              <span>Skewness: ${normalityTest.skewness}</span>
              <span>Kurtosis: ${normalityTest.kurtosis}</span>
            </div>
          </div>
        `;
      }
    }
  }
  
  if (hasStatTests) {
    analyticsGrid.appendChild(statisticalTests);
    cardCount += 1;
  }

  const patternDetection = document.createElement('div');
  patternDetection.className = 'analytics-card analytics-card--pattern';
  patternDetection.innerHTML = `
    <h3><span class="icon pattern-icon"></span> Pattern Detection</h3>
    <div class="pattern-detection-content"></div>
  `;
  
  let hasPatterns = false;
  const patternsContent = patternDetection.querySelector('.pattern-detection-content');
  
  for (const [colName, colInfo] of Object.entries(columns)) {
    if (colInfo.type === 'numeric' && colInfo.stats) {
      const patterns = colInfo.stats.patterns || [];
      
      if (patterns.length > 0) {
        hasPatterns = true;
        patternsContent.innerHTML += `
          <div class="pattern-item">
            <span class="pattern-column">${colName}</span>
            <div class="pattern-list">
              ${patterns.map(p => `
                <span class="pattern-badge ${p.type}">${p.type.replace('_', ' ')}</span>
              `).join('')}
            </div>
          </div>
        `;
      }
    }
  }
  
  if (hasPatterns) {
    analyticsGrid.appendChild(patternDetection);
    cardCount += 1;
  }

  const clusteringAnalysis = document.createElement('div');
  clusteringAnalysis.className = 'analytics-card analytics-card--cluster';
  clusteringAnalysis.innerHTML = `
    <h3><span class="icon cluster-icon"></span> Clustering Analysis</h3>
    <div class="clustering-content"></div>
  `;
  
  let hasClustering = false;
  const clusteringContent = clusteringAnalysis.querySelector('.clustering-content');
  
  for (const [colName, colInfo] of Object.entries(columns)) {
    if (colInfo.type === 'numeric' && colInfo.stats) {
      const clustering = colInfo.stats.clustering;
      
      if (clustering) {
        hasClustering = true;
        clusteringContent.innerHTML += `
          <div class="clustering-item">
            <span class="clustering-column">${colName}</span>
            <span class="clustering-info">${clustering.k} clusters detected</span>
            <div class="cluster-details">
              ${clustering.clusters.map(c => `
                <div class="cluster-detail">
                  <span>Cluster ${c.cluster_id}</span>
                  <span>Size: ${c.size}</span>
                  <span>Centroid: ${c.centroid}</span>
                </div>
              `).join('')}
            </div>
          </div>
        `;
      }
    }
  }
  
  if (hasClustering) {
    analyticsGrid.appendChild(clusteringAnalysis);
    cardCount += 1;
  }

  if (cardCount === 0) {
    return null;
  }

  const meta = section.querySelector('#advanced-analytics-meta');
  meta.innerHTML = `<span class="advanced-pill">${cardCount} modules</span>`;

  return section;
}

// ========================================
// Chatbot Interface
// ========================================

let chatMessages = [];
let currentJobId = null;
let conversationHistory = [];
let isTyping = false;

function renderChatInterface() {
  if (!chatSidebar) return;

  chatSidebar.innerHTML = `
    <button class="chat-launcher" id="chat-launcher" aria-label="Open AI assistant" title="Open AI assistant">
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
      </svg>
    </button>
    <div class="chat-panel" id="chat-panel" aria-hidden="true">
      <div class="chat-header">
        <div class="chat-header-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
          </svg>
          AI Assistant
        </div>
        <div class="chat-header-actions">
          <button class="chat-header-btn" id="chat-clear" title="Clear chat">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M19 7l-.867 12.142A2 2 0 0 1 16.138 21H7.862a2 2 0 0 1-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v3M4 7h16"></path>
            </svg>
          </button>
          <button class="chat-header-btn" id="chat-export" title="Export chat">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M4 16v1a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
            </svg>
          </button>
          <button class="chat-header-btn" id="chat-close" title="Close chat">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>
      </div>
      <div class="chat-messages" id="chat-messages"></div>
      <div class="typing-indicator hidden" id="typing-indicator">
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
      </div>
      <form class="chat-input-form" id="chat-form">
        <input type="text" id="chat-input" class="chat-input" placeholder="Ask about your data..." autocomplete="off" disabled />
        <button type="submit" id="chat-send" class="chat-send-btn" disabled>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="22" y1="2" x2="11" y2="13"></line>
            <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
          </svg>
        </button>
      </form>
    </div>
  `;

  const chatForm = document.getElementById('chat-form');
  const chatLauncher = document.getElementById('chat-launcher');
  const chatClose = document.getElementById('chat-close');
  const chatClear = document.getElementById('chat-clear');
  const chatExport = document.getElementById('chat-export');

  chatForm.addEventListener('submit', handleChatSubmit);
  chatLauncher.addEventListener('click', toggleChat);
  chatClose.addEventListener('click', closeChat);
  chatClear.addEventListener('click', clearChat);
  chatExport.addEventListener('click', exportChatHistory);

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeChat();
    }
  });

  setChatEnabled(false);
  addBotMessage("Hello! Upload a file to begin your analysis. Once it's done, you can ask me anything about your data.", false);
}

function setChatEnabled(enabled) {
  const chatInput = document.getElementById('chat-input');
  const chatSend = document.getElementById('chat-send');
  if (!chatInput || !chatSend) return;
  chatInput.disabled = !enabled;
  chatSend.disabled = !enabled;
}

function openChat() {
  if (!chatSidebar) return;
  chatSidebar.classList.add('open');
  chatSidebar.classList.remove('has-unread');
  const panel = document.getElementById('chat-panel');
  if (panel) panel.setAttribute('aria-hidden', 'false');
}

function closeChat() {
  if (!chatSidebar) return;
  chatSidebar.classList.remove('open');
  const panel = document.getElementById('chat-panel');
  if (panel) panel.setAttribute('aria-hidden', 'true');
}

function toggleChat() {
  if (!chatSidebar) return;
  if (chatSidebar.classList.contains('open')) {
    closeChat();
    return;
  }
  openChat();
}

function expandChat() {
  openChat();
}

function handleChatSubmit(e) {
  e.preventDefault();
  const chatInput = document.getElementById('chat-input');
  const chatSend = document.getElementById('chat-send');
  if (!chatInput || !chatSend) return;

  const query = chatInput.value.trim();
  if (!query || !currentJobId || isTyping) return;

  addUserMessage(query);
  chatInput.value = '';
  chatSend.disabled = true;
  sendChatQuery(query);
}

function addBotMessage(text, shouldNotify = true) {
  addChatMessage('bot', text);
  if (shouldNotify && chatSidebar && !chatSidebar.classList.contains('open')) {
    chatSidebar.classList.add('has-unread');
  }
}

function addUserMessage(text) {
  addChatMessage('user', text);
}

function addChatMessage(sender, text) {
  const messagesContainer = document.getElementById('chat-messages');
  if (!messagesContainer) return;

  const messageDiv = document.createElement('div');
  messageDiv.className = `chat-message ${sender}`;

  const avatar = document.createElement('div');
  avatar.className = 'chat-avatar';
  avatar.innerHTML = sender === 'bot'
    ? `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
      </svg>
    `
    : `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
        <circle cx="12" cy="7" r="4"></circle>
      </svg>
    `;

  const bubble = document.createElement('div');
  bubble.className = 'chat-bubble';
  bubble.innerHTML = formatMessage(text);

  messageDiv.appendChild(avatar);
  messageDiv.appendChild(bubble);
  messagesContainer.appendChild(messageDiv);
  messagesContainer.scrollTop = messagesContainer.scrollHeight;

  chatMessages.push({ sender, message: text, time: new Date().toISOString() });
  conversationHistory.push({ role: sender === 'user' ? 'user' : 'assistant', content: text });
}

function clearChat() {
  const messagesContainer = document.getElementById('chat-messages');
  if (!messagesContainer) return;
  messagesContainer.innerHTML = '';
  chatMessages = [];
  conversationHistory = [];
  addBotMessage('Chat cleared. Ask me a new question.');
}

function formatMessage(message) {
  // Basic markdown formatting
  return message
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    .replace(/`(.*?)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br>');
}

function showTypingIndicator() {
  const typingIndicator = document.getElementById('typing-indicator');
  if (typingIndicator) {
    typingIndicator.classList.remove('hidden');
    isTyping = true;
    const chatSend = document.getElementById('chat-send');
    if (chatSend) chatSend.disabled = true;
  }
}

function hideTypingIndicator() {
  const typingIndicator = document.getElementById('typing-indicator');
  if (typingIndicator) {
    typingIndicator.classList.add('hidden');
    isTyping = false;
    setChatEnabled(Boolean(currentJobId));
  }
}

function showFollowUpSuggestions(suggestions) {
  const container = document.getElementById('follow-up-suggestions');
  if (!container || !suggestions || suggestions.length === 0) return;
  
  container.innerHTML = suggestions.map(s => 
    `<button class="quick-action follow-up" data-query="${s}">${s}</button>`
  ).join('');
  
  container.classList.remove('hidden');
  
  container.querySelectorAll('.follow-up').forEach(btn => {
    btn.addEventListener('click', () => {
      const query = btn.dataset.query;
      const chatInput = document.getElementById('chat-input');
      const chatSend = document.getElementById('chat-send');
      chatInput.value = query;
      chatSend.click();
      container.classList.add('hidden');
    });
  });
}

function exportChatHistory() {
  if (chatMessages.length === 0) {
    alert('No messages to export');
    return;
  }
  
  const exportData = {
    jobId: currentJobId,
    timestamp: new Date().toISOString(),
    messages: chatMessages
  };
  
  const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `chat-history-${currentJobId}-${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

async function sendChatQuery(query) {
  if (!currentJobId || !apiBaseUrl) return;
  
  showTypingIndicator();
  
  try {
    const chatEndpoint = getChatEndpoint(currentJobId);
    const response = await fetch(chatEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        query,
        history: conversationHistory.slice(-10) // Send last 10 messages for context
      })
    });
    
    const data = await response.json();
    
    hideTypingIndicator();
    
    const chatSend = document.getElementById('chat-send');
    if (chatSend) chatSend.disabled = false;
    
    if (response.ok) {
      addBotMessage(data.response || 'I was not able to generate a response.');
      
      // Show follow-up suggestions if provided
      if (data.follow_up_suggestions) {
        showFollowUpSuggestions(data.follow_up_suggestions);
      }
      
      // Suggest chart visualization if applicable
      if (data.result && data.result.chart_data) {
        addBotMessage('Visualization available. I can create a chart for this analysis. Would you like to see it?');
      }
      
      // Update quick actions based on context
      if (data.context_suggestions) {
        updateQuickActions(data.context_suggestions);
      }
    } else {
      addBotMessage(`Error: ${data.error || 'Failed to process your query'}`);
    }
  } catch (error) {
    hideTypingIndicator();
    const chatSend = document.getElementById('chat-send');
    if (chatSend) chatSend.disabled = false;
    addBotMessage(`Error: ${error.message}. Chat endpoint: ${getChatEndpoint(currentJobId)}.`);
  }
}

function updateQuickActions(suggestions) {
  const container = document.getElementById('quick-actions');
  if (!container || !suggestions) return;
  const chatSend = document.getElementById('chat-send');
  
  container.innerHTML = suggestions.map(s => 
    `<button class="quick-action" data-query="${s}">${s}</button>`
  ).join('');
  
  container.querySelectorAll('.quick-action').forEach(btn => {
    btn.addEventListener('click', () => {
      const query = btn.dataset.query;
      const chatInput = document.getElementById('chat-input');
      chatInput.value = query;
      if (chatSend) chatSend.click();
    });
  });
}

document.addEventListener('DOMContentLoaded', renderChatInterface);

// ========================================
// Form Submission Handler
// ========================================

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (!apiBaseUrl) {
    setFeedback("API URL not configured. Edit frontend/config.js first.", "error");
    return;
  }

  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    setFeedback("Please select a file first", "error");
    return;
  }

  const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
  if (!SUPPORTED_EXTENSIONS.includes(ext)) {
    setFeedback(`Unsupported format. Please use: ${SUPPORTED_EXTENSIONS.join(', ')}`, "error");
    return;
  }

  runBtn.disabled = true;
  hideProgress();
  statusSection.classList.remove("hidden");
  resultsSection.classList.add("hidden");

  try {
    const fileType = getFileType(file.name);
    setFeedback("Creating analysis job...", "info");
    setProgress(10, "Creating job...");
    setStatus("CREATED", "Initializing...");
    
    const job = await createJob(fileType);
    const jobId = job.jobId;
    currentJobId = jobId;
    jobIdText.textContent = `Job ID: ${jobId}`;

    setFeedback("Uploading file to S3...", "info");
    setProgress(20, "Uploading...");
    setStatus("CREATED", "Uploading file...");
    
    await uploadFile(job.upload.url, file);

    setFeedback("Processing in cloud...", "info");
    setProgress(30, "Analyzing...");
    setStatus("PROCESSING", "Running analytics...");
    
    await pollUntilDone(jobId);

    setFeedback("Fetching results...", "info");
    setProgress(95, "Loading results...");
    
    const resultResponse = await getResult(jobId);
    const result = resultResponse.result || {};

    setProgress(100, "Complete!");
    setTimeout(hideProgress, 1500);
    
    if (result.analytics) {
      renderAnalytics(result.analytics);
    } else if (result.columns || result.insights) {
      renderAnalytics(result);
    } else {
      renderLegacyResults(result);
    }

    setFeedback("Analysis complete! See results below.", "success");
    setStatus("COMPLETED", "Analytics ready");

    setTimeout(() => {
      resultsSection.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 500);

  } catch (error) {
    console.error("Analysis error:", error);
    hideProgress();
    setStatus("FAILED", error.message);
    setFeedback(error.message, "error");
  } finally {
    runBtn.disabled = false;
  }
});

function renderLegacyResults(result) {
  resultsSection.innerHTML = `
    <div class="card">
      <h2>Analysis Results</h2>
      <p>${result.summary || 'Analysis completed.'}</p>
      ${result.stats ? `
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-value">${result.stats.rowCount || '-'}</div>
            <div class="stat-label">Rows</div>
          </div>
        </div>
      ` : ''}
    </div>
  `;
  resultsSection.classList.remove("hidden");
}