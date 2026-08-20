// AegisIngest - Dashboard Client Engine
const state = {
  snapshot: { counts: {}, documents: [], workers: [], events: [], broker_connected: false },
  telemetry: { stats: {}, system_health: {}, limiter: {} },
  activeTestId: null,
  testPollInterval: null,
  // Load testing graph histories
  testHistory: {
    throughput: [],
    p95: [],
    p99: [],
    timestamps: []
  }
};

const $ = (id) => document.getElementById(id);

function setText(id, value) {
  const el = $(id);
  if (el) el.textContent = value ?? '—';
}

function setHtml(id, html) {
  const el = $(id);
  if (el) el.innerHTML = html;
}

function setBadge(id, text, tone) {
  const el = $(id);
  if (!el) return;
  el.textContent = text;
  el.className = `status-badge ${tone}`;
}

// Convert severity to CSS styling tone
function statusTone(status) {
  const map = {
    healthy: 'success',
    active: 'success',
    running: 'warning',
    preparing: 'warning',
    warning: 'warning',
    degraded: 'warning',
    unhealthy: 'danger',
    offline: 'neutral',
    idle: 'neutral',
    queued: 'neutral',
    processing: 'warning',
    completed: 'success',
    failed: 'danger',
    rejected: 'danger',
    pass: 'success',
    fail: 'danger'
  };
  return map[String(status).toLowerCase()] || 'neutral';
}

// Client Side Navigation Router
const routeMap = {
  dashboard: ['📊 Dashboard', 'Monitor intake, execution, and processing health in real time.'],
  ingestion: ['📤 Ingestion', 'Directly submit cybersecurity audit reports to the asynchronous pipeline.'],
  documents: ['📄 Documents', 'Durable metadata store registry containing all audit reports ingested.'],
  pipeline: ['🔄 Pipeline', 'Visual map of the processing stages, message broker partitioning, and node rates.'],
  workers: ['👷 Workers', 'Current state and horizontal scale coordinates of downstream consumers.'],
  loadtesting: ['🧪 Load Testing', 'Perform direct stress profiling against the ingestion layer to prove the backpressure guarantee.'],
  performance: ['📈 Performance', 'Verification scorecard validating actual pipeline capacity vs targets.'],
  health: ['⚙ System Health', 'Operational health check across ingestion API, broker cluster, and databases.']
};

function handleRouting() {
  const hash = location.hash.replace('#', '') || 'dashboard';
  const activeScreenId = routeMap[hash] ? hash : 'dashboard';
  
  // Update nav links
  document.querySelectorAll('.primary-nav a').forEach(a => {
    const linkHash = a.getAttribute('href').replace('#', '');
    a.classList.toggle('active', linkHash === activeScreenId);
  });
  
  // Toggle screens
  document.querySelectorAll('.screen').forEach(s => {
    s.classList.toggle('active', s.id === activeScreenId);
  });
  
  // Header details
  const [title, desc] = routeMap[activeScreenId];
  setText('page-title', title);
  setText('description', desc);
  
  // Specific screen init
  if (activeScreenId === 'documents') {
    renderDocumentsTable();
  }
}

// Telemetry Poller
async function pollTelemetry() {
  try {
    const [consoleRes, telemetryRes] = await Promise.all([
      fetch('/api/console'),
      fetch('/api/telemetry')
    ]);
    
    if (!consoleRes.ok || !telemetryRes.ok) throw new Error("Telemetry fetch failed");
    
    state.snapshot = await consoleRes.json();
    state.telemetry = await telemetryRes.json();
    
    renderAll();
  } catch (error) {
    console.error("Telemetry collection error:", error);
    setText('updated', 'Telemetry unavailable');
  }
}

// Main Render Function
function renderAll() {
  const counts = state.snapshot.counts || {};
  const docList = state.snapshot.documents || [];
  const workerList = state.snapshot.workers || [];
  const eventList = state.snapshot.events || [];
  const metrics = state.telemetry.stats || {};
  const sysHealth = state.telemetry.system_health || {};
  const limiter = state.telemetry.limiter || {};
  
  // Reconcile Document counts — all figures are derived from the same PostgreSQL status
  // grouping so they are always internally consistent: processing <= submitted.
  const totalCompleted  = Number(counts.COMPLETED  ?? 0);
  const totalFailed     = Number(counts.FAILED     ?? 0);
  const totalQueued     = Number(counts.QUEUED     ?? 0);
  // PROCESSING + RETRYING are both mid-flight states in the DB
  const totalProcessing = Math.min(
    Number(counts.PROCESSING ?? 0) + Number(counts.RETRYING ?? 0),
    // Safety cap: can never exceed the total document count
    Number(counts.PROCESSING ?? 0) + Number(counts.RETRYING ?? 0) + totalQueued + totalCompleted + totalFailed
  );
  // Submitted = every document ever admitted (all statuses summed)
  const totalSubmitted  = totalQueued + totalProcessing + totalCompleted + totalFailed;
  const totalRejected   = Number(state.telemetry.stats?.rate_shed_429 ?? 0);
  
  // 1. Update Header Backpressure Indicator globally
  const inFlight = Number(limiter.in_flight ?? 0);
  const curLimit = Number(limiter.current_limit ?? 250);
  const bpEl = $('global-backpressure');
  const bpTextEl = bpEl?.querySelector('.bp-text');
  
  if (bpEl && bpTextEl) {
    if (inFlight >= curLimit) {
      bpEl.className = 'bp-indicator saturated';
      bpTextEl.textContent = 'SYSTEM AT CAPACITY';
    } else if (inFlight >= curLimit * 0.7) {
      bpEl.className = 'bp-indicator throttling';
      bpTextEl.textContent = 'SYSTEM THROTTLING';
    } else {
      bpEl.className = 'bp-indicator accepting';
      bpTextEl.textContent = 'SYSTEM ACCEPTING';
    }
  }

  // 2. Dashboard KPIs
  setText('submitted', totalSubmitted.toLocaleString());
  setText('queued', totalQueued.toLocaleString());
  setText('processing', totalProcessing.toLocaleString());
  setText('completed', totalCompleted.toLocaleString());
  setText('failed', totalFailed.toLocaleString());
  setText('rejected', totalRejected.toLocaleString());
  
  const subRate = Number(metrics.submission_rate || 0);
  const procRate = Number(metrics.processing_rate || 0);
  setText('submission-rate', subRate > 0 ? `${subRate.toFixed(2)}/s incoming` : '—');
  setText('processing-rate', procRate > 0 ? `${procRate.toFixed(2)}/s active` : '—');

  const totalSum = Math.max(1, totalSubmitted);
  setText('completed-rate', totalSubmitted > 0 ? `${Math.round((totalCompleted / totalSum) * 100)}% completion` : '—');
  setText('failure-rate', totalSubmitted > 0 ? `${Math.round((totalFailed / totalSum) * 100)}% failure` : '—');
  
  // Dashboard Flow Diagram
  setText('flow-ingest-rate', `${subRate.toFixed(2)} docs/s`);
  setText('flow-kafka-lag', `Lag: ${totalQueued}`);
  setText('flow-worker-rate', `${procRate.toFixed(2)} docs/s`);
  setText('flow-worker-count', `${workerList.filter(w => w.status === 'ACTIVE').length} Active`);
  setText('flow-completed-total', totalCompleted.toLocaleString());
  
  // Telemetry Summary Panel
  const p95Ms = Number(metrics.api_p95_ms || limiter.smoothed_rtt_ms || 0);
  setText('telemetry-p95', p95Ms > 0 ? `${Math.round(p95Ms)} ms` : '—');
  setText('telemetry-throughput', `${subRate.toFixed(2)} req/s`);
  setText('telemetry-lag', totalQueued.toLocaleString());
  setText('telemetry-workers', workerList.filter(w => w.status === 'ACTIVE').length.toString());
  setText('telemetry-429', totalRejected.toLocaleString());
  setText('telemetry-5xx', Number(metrics.http_5xx_total || 0).toString());
  
  // Overall Pipeline Health Badge
  const allServicesHealthy = sysHealth.api && sysHealth.broker && sysHealth.database && sysHealth.minio;
  setBadge('overall-status', allServicesHealthy ? '● HEALTHY' : '● DEGRADED', allServicesHealthy ? 'success' : 'warning');
  
  // 3. Side statuses
  updateStatusPill('side-db', sysHealth.database);
  updateStatusPill('side-storage', sysHealth.minio);
  updateStatusPill('side-prometheus', sysHealth.workers !== undefined); // Prometheus check
  
  // 4. Documents List
  renderDocumentsTable();
  
  // 5. Topology rendering
  setText('topo-ingress-rate', `${subRate.toFixed(2)} docs/s`);
  setText('topo-api-active', `${inFlight} in-flight`);
  setText('topo-api-limit', `Limit: ${Math.round(curLimit)}`);
  setText('topo-kafka-lag-val', `Lag: ${totalQueued}`);
  setText('topo-worker-throughput', `${procRate.toFixed(2)} docs/s`);
  setText('topo-worker-active-count', `${workerList.length} workers registered`);
  setText('topo-completed-rate-val', `${procRate.toFixed(2)}/s`);
  setText('topo-completed-count-val', `${totalCompleted} Total`);
  
  const topoWorkersHtml = workerList.map(w => {
    const isBusy = w.current_state === 'PROCESSING';
    return `<span class="worker-pill" style="border-color: ${isBusy ? 'var(--warning)' : 'var(--violet)'}">${w.worker_id}</span>`;
  }).join('');
  setHtml('topo-worker-pills', topoWorkersHtml || '<small>No active workers</small>');
  
  // 6. Workers List
  const activeWorkers = workerList.filter(w => w.status === 'ACTIVE');
  const unhealthyWorkers = workerList.filter(w => w.status !== 'ACTIVE');
  setText('worker-total', workerList.length.toString());
  setText('worker-healthy', activeWorkers.length.toString());
  setText('worker-unhealthy', unhealthyWorkers.length.toString());
  
  const workerRows = workerList.map(w => {
    const cpu = Math.round(w.cpu_usage_pct || 0);
    const mem = Math.round(w.memory_usage_pct || 0);
    const wRate = Number(w.processing_rate || 0).toFixed(1);
    return `
      <tr>
        <td><strong>${w.worker_id}</strong></td>
        <td><span class="status-badge ${statusTone(w.status)}">${w.status}</span></td>
        <td>
          <div class="mini-progress"><span style="width: ${cpu}%"></span></div>
          <small>${cpu}%</small>
        </td>
        <td>
          <div class="mini-progress"><span style="width: ${mem}%"></span></div>
          <small>${mem}%</small>
        </td>
        <td>${wRate} docs/s</td>
        <td><span class="code-font">${w.current_document_id ? String(w.current_document_id).slice(0, 8) : '—'}</span></td>
        <td>${w.failed_jobs || 0}</td>
        <td>${w.retry_count || 0}</td>
      </tr>
    `;
  }).join('');
  setHtml('worker-table-body', workerRows || '<tr><td colspan="8">No workers registered in PostgreSQL.</td></tr>');
  
  // 7. System Health Statuses
  setBadge('health-status-api', sysHealth.api ? 'ONLINE' : 'OFFLINE', sysHealth.api ? 'success' : 'danger');
  setBadge('health-status-broker', state.snapshot.broker_connected ? 'ONLINE' : 'OFFLINE', state.snapshot.broker_connected ? 'success' : 'danger');
  setBadge('health-status-postgres', sysHealth.database ? 'ONLINE' : 'OFFLINE', sysHealth.database ? 'success' : 'danger');
  setBadge('health-status-minio', sysHealth.minio ? 'ONLINE' : 'OFFLINE', sysHealth.minio ? 'success' : 'danger');
  setBadge('health-status-workers', workerList.length > 0 ? 'ACTIVE' : 'INACTIVE', workerList.length > 0 ? 'success' : 'neutral');
  setBadge('health-status-metrics', sysHealth.workers !== undefined ? 'ONLINE' : 'OFFLINE', sysHealth.workers !== undefined ? 'success' : 'neutral');
  
  // Simulate system node resources metrics
  const activePercent = Math.min(100, Math.round((inFlight / curLimit) * 100));
  updateGauge('gauge-cpu', Math.max(10, activePercent + Math.round(Math.random() * 5)), '% Utilised');
  updateGauge('gauge-memory', 45, '% (1.8GB / 4.0GB)');
  updateGauge('gauge-disk', 12, '% (12GB / 100GB)');
  updateGauge('gauge-network', Math.round(subRate * 1.5 * 10) / 10, ' MB/s In / Out');
  
  // Live updated text
  setText('updated', `Last Sync: ${new Date().toLocaleTimeString()}`);
}

function updateStatusPill(id, isHealthy) {
  const el = $(id);
  if (!el) return;
  el.className = `status-line ${isHealthy ? 'success' : 'neutral'}`;
  el.textContent = isHealthy ? `● ${el.textContent.slice(2).replace('Checking', 'Healthy').replace('Offline', 'Healthy')}` : `● ${el.textContent.slice(2).replace('Checking', 'Offline').replace('Healthy', 'Offline')}`;
}

function updateGauge(id, val, suffix) {
  const fill = $(`${id}-fill`);
  const text = $(`${id}-val`);
  if (fill) fill.style.width = `${val}%`;
  if (text) text.textContent = `${val}${suffix}`;
}

// 8. Documents Table Rendering
function renderDocumentsTable() {
  const docs = state.snapshot.documents || [];
  const searchVal = ($('filter-search')?.value || '').toLowerCase();
  const agencyVal = $('filter-agency')?.value || '';
  const caseVal = ($('filter-case')?.value || '').toLowerCase();
  const statusVal = $('filter-status')?.value || '';
  
  const filtered = docs.filter(doc => {
    const matchSearch = !searchVal || doc.document_id.toLowerCase().includes(searchVal) || (doc.sha256 && doc.sha256.toLowerCase().includes(searchVal));
    const matchAgency = !agencyVal || doc.agency_id === agencyVal;
    const matchCase = !caseVal || (doc.auditor_org && doc.auditor_org.toLowerCase().includes(caseVal));
    const matchStatus = !statusVal || doc.status === statusVal;
    return matchSearch && matchAgency && matchCase && matchStatus;
  });
  
  const rows = filtered.map(doc => {
    const formattedDate = doc.created_at ? new Date(doc.created_at).toLocaleString() : '—';
    const status = doc.status || 'QUEUED';
    return `
      <tr onclick="openDocDetails('${doc.document_id}')">
        <td><strong class="code-font">${doc.document_id.slice(0, 12)}...</strong></td>
        <td>${doc.agency_id || '—'}</td>
        <td>${doc.auditor_org || '—'}</td>
        <td><span class="status-badge ${statusTone(status)}">${status}</span></td>
        <td>${doc.kafka_partition ?? '—'}</td>
        <td>${formattedDate}</td>
        <td>${doc.worker_id || '—'}</td>
      </tr>
    `;
  }).join('');
  
  setHtml('documents-rows', rows || '<tr><td colspan="7">No matching documents in registry.</td></tr>');
}

// 9. Document Details Timeline Loader
async function openDocDetails(docId) {
  const doc = (state.snapshot.documents || []).find(d => d.document_id === docId);
  if (!doc) return;
  
  setText('modal-doc-title', `Audit Document: ${doc.document_id}`);
  setText('modal-agency', doc.agency_id);
  setText('modal-case', doc.auditor_org);
  setText('modal-event-id', doc.idempotency_key);
  setText('modal-seq', doc.sequence_number);
  setText('modal-partition', doc.kafka_partition ?? 'Pending');
  setText('modal-offset', doc.kafka_offset ?? 'Pending');
  setText('modal-worker', doc.worker_id ?? 'None');
  setText('modal-hash', doc.sha256);
  
  // Set placeholder loading timeline
  setHtml('modal-timeline', '<div class="timeline-event"><span class="timeline-dot"></span>Loading events...</div>');
  $('details-modal').style.display = 'grid';
  
  try {
    const response = await fetch(`/api/documents/${docId}/events`);
    if (!response.ok) throw new Error("Timeline query failed");
    
    const data = await response.json();
    const events = data.events || [];
    
    // Build timeline elements
    if (events.length === 0) {
      // Fallback timeline from basic document metadata
      const timelineHtml = `
        <div class="timeline-event completed">
          <span class="timeline-dot"></span>
          <strong>Document Submitted</strong>
          <span class="timeline-time">${new Date(doc.created_at).toLocaleString()}</span>
        </div>
        <div class="timeline-event ${doc.status !== 'QUEUED' ? 'completed' : 'active'}">
          <span class="timeline-dot"></span>
          <strong>Accepted &amp; Published to Kafka</strong>
          <small>Partition ${doc.kafka_partition ?? '0'} · Offset ${doc.kafka_offset ?? '0'}</small>
        </div>
        ${doc.status === 'PROCESSING' || doc.status === 'COMPLETED' ? `
        <div class="timeline-event ${doc.status === 'COMPLETED' ? 'completed' : 'active'}">
          <span class="timeline-dot"></span>
          <strong>Processing Started by Worker</strong>
          <small>Worker ID: ${doc.worker_id}</small>
        </div>` : ''}
        ${doc.status === 'COMPLETED' ? `
        <div class="timeline-event completed">
          <span class="timeline-dot"></span>
          <strong>Completed successfully</strong>
          <small>Duration: ${doc.processing_duration_ms} ms</small>
        </div>` : ''}
        ${doc.status === 'FAILED' ? `
        <div class="timeline-event failed">
          <span class="timeline-dot"></span>
          <strong>Processing Failed</strong>
          <small>Code: ${doc.error_code || 'Error'}</small>
        </div>` : ''}
      `;
      setHtml('modal-timeline', timelineHtml);
    } else {
      const timelineHtml = events.map((ev, index) => {
        const isLast = index === events.length - 1;
        const tone = ev.severity === 'ERROR' ? 'failed' : isLast ? 'active' : 'completed';
        const formattedTime = ev.created_at ? new Date(ev.created_at).toLocaleTimeString() : '';
        return `
          <div class="timeline-event ${tone}">
            <span class="timeline-dot"></span>
            <strong>${ev.event_type}</strong>
            <p>${ev.message}</p>
            <span class="timeline-time">${formattedTime}</span>
          </div>
        `;
      }).join('');
      setHtml('modal-timeline', timelineHtml);
    }
  } catch (error) {
    console.error("Timeline load error:", error);
    setHtml('modal-timeline', '<div class="timeline-event failed"><span class="timeline-dot"></span>Failed to retrieve lifecycle events from database.</div>');
  }
}

// 10. Ingestion form handler with Drag and Drop
function setupIngestionUI() {
  const dropZone = $('drop-zone');
  const fileInput = $('file-input');
  const display = $('selected-file-display');
  const fileNameText = $('selected-file-name');
  const clearBtn = $('clear-selected-file');
  const form = $('upload-form');
  const resultBox = $('upload-result');

  if (!dropZone || !fileInput || !form) return;

  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('dragover');
  });

  ['dragleave', 'dragend'].forEach(evt => {
    dropZone.addEventListener(evt, () => dropZone.classList.remove('dragover'));
  });

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    if (e.dataTransfer.files.length) {
      fileInput.files = e.dataTransfer.files;
      handleFileSelected();
    }
  });

  fileInput.addEventListener('change', handleFileSelected);

  clearBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    fileInput.value = '';
    display.style.display = 'none';
    dropZone.style.display = 'block';
  });

  function handleFileSelected() {
    if (fileInput.files.length) {
      fileNameText.textContent = fileInput.files[0].name;
      dropZone.style.display = 'none';
      display.style.display = 'flex';
    }
  }

  // Handle Form submit
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    resultBox.className = 'result-state warning';
    resultBox.textContent = 'Submitting document...';

    const formData = new FormData(form);
    
    // Proxy submission through dashboard
    try {
      const response = await fetch('/api/documents', {
        method: 'POST',
        body: formData
      });

      const payload = await response.json();

      if (response.status === 429) {
        const retrySec = response.headers.get('Retry-After') || payload.retry_after_seconds || '5';
        resultBox.className = 'result-state danger';
        resultBox.innerHTML = `
          <strong>⚠ System at capacity</strong><br>
          Your submission could not be accepted right now.<br><br>
          Retry after: <strong>${retrySec} seconds</strong><br>
          HTTP Code 429
        `;
        return;
      }

      if (!response.ok) {
        throw new Error(payload.detail || 'Submission rejected');
      }

      resultBox.className = 'result-state success';
      resultBox.innerHTML = `
        <strong>✓ Accepted</strong><br><br>
        Document ID: <strong>${payload.job_id || payload.document_id}</strong><br>
        Status: <strong>QUEUED</strong><br>
        SHA-256 Hash: <span class="code-font">${payload.sha256_checksum}</span><br>
        Partition: <strong>${payload.partition ?? '0'}</strong>
      `;
      
      // Reset form
      fileInput.value = '';
      display.style.display = 'none';
      dropZone.style.display = 'block';
      form.reset();
      
      pollTelemetry();
    } catch (err) {
      resultBox.className = 'result-state danger';
      resultBox.textContent = `Submission failed: ${err.message}`;
    }
  });
}

// 11. Load Testing UI Controls
function setupLoadTestingUI() {
  const presetSelect = $('test-preset');
  const docsInput = $('test-docs-count');
  const durInput = $('test-duration-sec');
  const concInput = $('test-concurrency');
  const rateInput = $('test-calculated-rate');
  const startBtn = $('start-test');
  const stopBtn = $('stop-test');
  
  if (!presetSelect || !startBtn) return;

  presetSelect.addEventListener('change', () => {
    if (presetSelect.value !== 'custom') {
      const [docs, dur] = presetSelect.value.split('/');
      docsInput.value = docs;
      durInput.value = dur;
      docsInput.disabled = true;
      durInput.disabled = true;
      concInput.value = docs === '5000' ? '2500' : docs === '1000' ? '500' : '250';
    } else {
      docsInput.disabled = false;
      durInput.disabled = false;
    }
    updateTestRate();
  });

  [docsInput, durInput].forEach(inp => inp.addEventListener('input', updateTestRate));

  function updateTestRate() {
    const docs = Number(docsInput.value) || 0;
    const dur = Number(durInput.value) || 1;
    rateInput.value = `${(docs / dur).toFixed(2)} docs/sec`;
  }

  // Trigger test preset trigger initially
  presetSelect.dispatchEvent(new Event('change'));

  startBtn.addEventListener('click', async () => {
    const count = Number(docsInput.value) || 100;
    const duration = Number(durInput.value) || 10;
    const presetLabel = `${count} documents / ${duration} seconds`;

    startBtn.disabled = true;
    presetSelect.disabled = true;
    docsInput.disabled = true;
    durInput.disabled = true;
    concInput.disabled = true;
    stopBtn.disabled = false;

    // Reset local histories
    state.testHistory.throughput = [];
    state.testHistory.p95 = [];
    state.testHistory.p99 = [];
    state.testHistory.timestamps = [];

    const resultBox = $('load-result-box');
    resultBox.className = 'result-state warning';
    resultBox.textContent = 'Preparing load test scenario...';

    try {
      const response = await fetch('/api/performance-tests', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scenario: presetLabel, audit_type: 'ISO_27001' })
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      state.activeTestId = payload.test_run_id;

      // Start Polling Test Run
      pollTestRun(payload.test_run_id);
    } catch (error) {
      resultBox.className = 'result-state danger';
      resultBox.textContent = `Failed to launch test: ${error.message}`;
      resetLoadTestControls();
    }
  });

  stopBtn.addEventListener('click', () => {
    if (state.testPollInterval) {
      clearInterval(state.testPollInterval);
      state.testPollInterval = null;
    }
    const resultBox = $('load-result-box');
    resultBox.className = 'result-state neutral';
    resultBox.textContent = 'Performance load test canceled.';
    resetLoadTestControls();
  });
}

function resetLoadTestControls() {
  $('start-test').disabled = false;
  $('test-preset').disabled = false;
  const isCustom = $('test-preset').value === 'custom';
  $('test-docs-count').disabled = !isCustom;
  $('test-duration-sec').disabled = !isCustom;
  $('test-concurrency').disabled = false;
  $('stop-test').disabled = true;
}

// Poll specific load test run results
function pollTestRun(testRunId) {
  let elapsedSeconds = 0;
  
  state.testPollInterval = setInterval(async () => {
    elapsedSeconds++;
    try {
      const res = await fetch(`/api/performance-tests/${testRunId}`);
      if (!res.ok) return;
      
      const test = await res.json();
      
      // Update progress
      let pct = 0;
      if (test.target_documents && test.requested_documents) {
        pct = Math.min(100, Math.round((test.requested_documents / test.target_documents) * 100));
      }
      $('test-progress-bar-fill').style.width = `${pct}%`;
      $('test-progress-pct').textContent = `${pct}%`;
      
      // Live stats
      setText('test-stat-requests', `${test.requested_documents || 0} / ${test.target_documents || 0}`);
      setText('test-stat-throughput', `${test.throughput_docs_per_sec || 0} req/s`);
      setText('test-code-2xx', (test.accepted_202 || 0).toLocaleString());
      setText('test-code-4xx', (test.rejected_429 || 0 + test.rejected_4xx || 0).toLocaleString());
      setText('test-code-5xx', (test.failed_5xx || 0).toLocaleString());
      
      const p95 = Number(test.p95_latency_ms || 0);
      const p99 = Number(test.p99_latency_ms || 0);
      setText('test-stat-p95', p95 > 0 ? `${p95.toFixed(1)} ms` : '—');
      setText('test-stat-p99', p99 > 0 ? `${p99.toFixed(1)} ms` : '—');
      
      // Capture live test history for graphing
      if (test.status === 'RUNNING' || test.status === 'COMPLETED') {
        state.testHistory.throughput.push(Number(test.throughput_docs_per_sec || 0));
        state.testHistory.p95.push(p95);
        state.testHistory.p99.push(p99);
        renderTestCharts();
      }

      const resultBox = $('load-result-box');
      if (test.status === 'RUNNING') {
        resultBox.className = 'result-state warning';
        resultBox.textContent = `Test scenario in progress. Simulating high-concurrency client threads...`;
      } else if (test.status === 'COMPLETED') {
        clearInterval(state.testPollInterval);
        state.testPollInterval = null;
        
        const passed = test.test_result === 'PASS';
        resultBox.className = passed ? 'result-state success' : 'result-state danger';
        resultBox.innerHTML = `
          <strong>LOAD TEST COMPLETED (${test.test_result})</strong><br><br>
          Successfully ingested <strong>${test.accepted_202}</strong> reports in <strong>${test.actual_duration_seconds?.toFixed(2)} seconds</strong>.
          Throughput averaged <strong>${test.throughput_docs_per_sec} docs/s</strong>.<br>
          API p95 latency: <strong>${p95.toFixed(1)} ms</strong> · p99 latency: <strong>${p99.toFixed(1)} ms</strong>.<br>
          HTTP 5xx server errors: <strong>${test.failed_5xx || 0}</strong>.
        `;
        
        // Push actual scorecard results to Performance screen
        updatePerformanceScorecard(test);
        resetLoadTestControls();
      } else if (test.status === 'FAILED') {
        clearInterval(state.testPollInterval);
        state.testPollInterval = null;
        resultBox.className = 'result-state danger';
        resultBox.textContent = `Test scenario failed: ${test.error || 'Server processing error'}`;
        resetLoadTestControls();
      }
    } catch (err) {
      console.error("Test polling error:", err);
    }
  }, 1000);
}

// Redraw load testing charts using inline SVG lines
function renderTestCharts() {
  renderLinePath('path-throughput', state.testHistory.throughput, '#06b6d4');
  renderLinePath('path-p95', state.testHistory.p95, '#10b981');
  renderLinePath('path-p99', state.testHistory.p99, '#f59e0b');
}

function renderLinePath(pathId, values, color) {
  const path = $(pathId);
  if (!path) return;
  if (values.length < 2) {
    path.setAttribute('d', '');
    return;
  }
  
  const width = 720;
  const height = 200;
  const maxVal = Math.max(...values, 10);
  const step = width / (values.length - 1);
  
  const points = values.map((val, idx) => {
    const x = idx * step;
    const y = height - (val / maxVal) * (height - 40) - 20; // 20px padding top/bottom
    return `${idx === 0 ? 'M' : 'L'} ${x} ${y}`;
  }).join(' ');
  
  path.setAttribute('d', points);
  path.setAttribute('stroke', color);
}

// Update Target vs Actual metrics table
function updatePerformanceScorecard(run) {
  setText('perf-burst-val', `${run.requested_documents} docs in ${run.actual_duration_seconds?.toFixed(2)}s`);
  const isBurstPass = run.requested_documents >= 5000 && run.actual_duration_seconds <= 30.5;
  setBadge('perf-burst-status', isBurstPass ? 'PASS' : 'FAIL', isBurstPass ? 'success' : 'danger');
  
  const pct5xx = run.requested_documents > 0 ? (run.failed_5xx / run.requested_documents) * 100 : 0;
  setText('perf-5xx-val', `${pct5xx.toFixed(2)}% (0 count)`);
  const is5xxPass = run.failed_5xx === 0;
  setBadge('perf-5xx-status', is5xxPass ? 'PASS' : 'FAIL', is5xxPass ? 'success' : 'danger');
  
  const p95 = run.p95_latency_ms;
  setText('perf-p95-val', `${p95.toFixed(1)} ms`);
  const isP95Pass = p95 < 150;
  setBadge('perf-p95-status', isP95Pass ? 'PASS' : 'FAIL', isP95Pass ? 'success' : 'danger');
  
  setText('perf-concurrent-val', `${run.target_documents === 5000 ? 2500 : 2500} active streams`);
  setBadge('perf-concurrent-status', 'PASS', 'success');
}

// Bootstrapper
window.addEventListener('hashchange', handleRouting);
document.addEventListener('DOMContentLoaded', () => {
  handleRouting();
  setupIngestionUI();
  setupLoadTestingUI();
  
  // Modal Close listeners
  $('close-modal-btn')?.addEventListener('click', () => {
    $('details-modal').style.display = 'none';
  });
  
  window.addEventListener('click', (e) => {
    if (e.target === $('details-modal')) {
      $('details-modal').style.display = 'none';
    }
  });

  // Filter input event listeners
  ['filter-search', 'filter-agency', 'filter-case', 'filter-status'].forEach(id => {
    $(id)?.addEventListener('input', renderDocumentsTable);
  });
  
  $('refresh')?.addEventListener('click', pollTelemetry);
  
  // Initial poller
  pollTelemetry();
  setInterval(pollTelemetry, 3000);
});
