(function() {
  'use strict';

  // ── WebSocket Connection ──
  let ws = null;
  let reconnectTimer = null;
  const workers = {};
  const tasks = {};

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = proto + '//' + location.host + '/api/dashboard/ws';
    ws = new WebSocket(url);
    ws.onopen = () => { console.log('Dashboard WS connected'); };
    ws.onclose = () => {
      console.log('WS disconnected, reconnecting...');
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectWS, 3000);
    };
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.type === 'init') {
          // Initial state dump
          if (msg.workers) Object.assign(workers, msg.workers);
          if (msg.tasks) Object.assign(tasks, msg.tasks);
        } else if (msg.type === 'worker_online' || msg.type === 'worker_update') {
          const w = msg.worker;
          if (w) workers[w.uuid] = w;
        } else if (msg.type === 'worker_offline') {
          if (msg.uuid && workers[msg.uuid]) workers[msg.uuid].online = false;
        } else if (msg.type === 'task_update' || msg.type === 'task_completed' || msg.type === 'task_created') {
          const t = msg.task;
          if (t) tasks[t.id] = t;
        }
        renderAll();
      } catch(e) { console.warn('WS parse error', e); }
    };
  }

  // ── Utility ──
  function esc(s) {
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }
  function fmtPct(v) { return v != null ? Math.round(v) + '%' : '-'; }

  function parseProgress(p) {
    if (p == null) return 0;
    if (typeof p === 'number') return p;
    if (typeof p === 'string' && p.endsWith('%')) return parseInt(p, 10) || 0;
    return 0;
  }

  // ── Tab switching ──
  function switchTab(name) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.classList.remove('active');
      if (btn.dataset.tab === name) btn.classList.add('active');
    });
    document.querySelectorAll('.tab-content').forEach(p => p.classList.remove('active'));
    const panel = document.getElementById('tab-' + name);
    if (panel) panel.classList.add('active');
    if (name === 'workers') renderWorkers();
    else if (name === 'tasks') renderTasks();
  }

  document.addEventListener('DOMContentLoaded', () => {
    connectWS();

    // Tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Quick start form
    document.getElementById('quick-form').addEventListener('submit', (e) => {
      e.preventDefault();
      quickStart();
    });
  });

  // ── Quick Start ──
  function quickStart() {
    const asnInput = document.getElementById('quick-asn');
    const portInput = document.getElementById('quick-ports');
    const shardInput = document.getElementById('quick-shards');
    const btn = document.getElementById('quick-start-btn');
    const resultDiv = document.getElementById('quick-result');

    const asns = asnInput.value.trim().split(/[,\s]+/).filter(Boolean);
    if (asns.length === 0) { alert('请输入 ASN'); return; }
    const ports = portInput.value.trim().split(/[,\s]+/).filter(Boolean).map(Number).filter(n => n > 0);
    if (ports.length === 0) { alert('请输入端口'); return; }
    const shards = parseInt(shardInput.value) || 1;

    btn.disabled = true;
    btn.textContent = '提交中...';

    const form = new URLSearchParams();
    form.set('name', 'scan-' + asns[0].replace('AS', ''));
    form.set('asns', asns.join(','));
    form.set('ports', ports.join(','));
    form.set('shards', String(shards));
    form.set('max_rate', '15000');
    form.set('total_ips', '200000');

    fetch('/api/task/create', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: form.toString()
    }).then(r => r.json()).then(d => {
      btn.disabled = false;
      btn.textContent = '开始扫描';
      if (d.error) {
        resultDiv.innerHTML = '<div class="error">错误: ' + esc(d.error) + '</div>';
      } else {
        resultDiv.innerHTML = '<div class="success">任务已创建: ' + esc(d.group_id || d.id) + '</div>';
        switchTab('tasks');
      }
    }).catch(e => {
      btn.disabled = false;
      btn.textContent = '开始扫描';
      resultDiv.innerHTML = '<div class="error">请求失败: ' + esc(e.message) + '</div>';
    });
  }

  // ── Import ASN file ──
  function importASNFile(event) {
    const file = event.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target.result;
      const asns = [];
      const lines = text.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const match = trimmed.match(/AS?(\d+)/i);
        if (match) asns.push('AS' + match[1]);
      }
      if (asns.length > 0) {
        document.getElementById('quick-asn').value = asns.join(', ');
        alert('已导入 ' + asns.length + ' 个 ASN');
      } else {
        alert('未找到有效 ASN');
      }
    };
    reader.readAsText(file);
  }

  // ── Cancel ──
  function cancelTask(taskId) {
    if (!confirm('取消任务 ' + taskId + '?')) return;
    const form = new URLSearchParams();
    form.set('task_id', taskId);
    fetch('/api/task/cancel', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: form.toString()
    }).catch(e => console.warn('Cancel error', e));
  }
  function cancelGroup(groupId) {
    if (!confirm('取消组 ' + groupId + '?')) return;
    const form = new URLSearchParams();
    form.set('group_id', groupId);
    fetch('/api/task/cancel', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: form.toString()
    }).catch(e => console.warn('Cancel error', e));
  }
  function canCancel(t) {
    return t.status === 'pending' || t.status === 'assigned' || t.status === 'running';
  }

  // ── Render all ──
  function renderAll() {
    renderStats();
    renderWorkerCards();
    renderTaskGroupsOverview();
    const activePanel = document.querySelector('.tab-content.active');
    if (activePanel) {
      if (activePanel.id === 'tab-workers') renderWorkers();
      else if (activePanel.id === 'tab-tasks') renderTasks();
    }
  }

  // ── Render: Stats ──
  function renderStats() {
    const counts = { pending: 0, assigned: 0, running: 0, completed: 0, failed: 0, cancelled: 0 };
    let totalHits = 0;
    Object.values(tasks).forEach(t => {
      if (counts[t.status] != null) counts[t.status]++;
      totalHits += t.hits || 0;
    });
    const onlineWorkers = Object.values(workers).filter(w => w.online).length;
    const offlineWorkers = Object.values(workers).filter(w => !w.online).length;

    document.getElementById('stat-workers-online').textContent = onlineWorkers;
    document.getElementById('stat-workers-offline').textContent = offlineWorkers;
    document.getElementById('stat-tasks-pending').textContent = counts.pending;
    document.getElementById('stat-tasks-running').textContent = counts.running + counts.assigned;
    document.getElementById('stat-tasks-completed').textContent = counts.completed;
    document.getElementById('stat-total-hits').textContent = totalHits;
  }

  // ── Render: Workers ──
  function renderWorkerCards() {
    const container = document.getElementById('worker-cards');
    if (!container) return;
    const arr = Object.values(workers).filter(w => w.online);
    if (arr.length === 0) { container.innerHTML = '<div class="empty">暂无在线 Worker</div>'; return; }
    container.innerHTML = arr.map(w => workerCardHTML(w)).join('');
  }

  function renderWorkers() {
    const container = document.getElementById('worker-list');
    if (!container) return;
    const arr = Object.values(workers);
    if (arr.length === 0) { container.innerHTML = '<div class="empty">暂无 Worker</div>'; return; }
    container.innerHTML = arr.map(w => workerCardHTML(w)).join('');
  }

  function workerCardHTML(w) {
    const status = w.online ? (w.status || 'online') : 'offline';
    const pct = parseProgress(w.progress);
    const phase = w.phase || '';
    const caps = w.capabilities || {};
    const tasksInfo = caps.max_tasks
      ? '<span class="worker-cap">📋 ' + (w.running_tasks || 0) + '/' + caps.max_tasks + '</span>' : '';
    const hwInfo = caps.cpu_count
      ? '<span class="worker-cap">💻 ' + caps.cpu_count + '核/' + caps.memory_mb + 'MB</span>' : '';
    const concInfo = caps.max_concurrent
      ? '<span class="worker-cap">⚡ ' + caps.max_concurrent + '并发</span>' : '';

    return '<div class="worker-card">' +
      '<span class="worker-name">' + esc(w.name || w.uuid) + '</span>' +
      '<span class="worker-status ' + status + '">' + status + '</span>' +
      '<div class="worker-progress">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill' + (pct >= 100 ? ' complete' : '') + '" style="width:' + Math.min(pct, 100) + '%"></div>' +
        '</div>' +
        '<span class="progress-text">' + esc(w.progress || '0%') + '</span>' +
      '</div>' +
      '<span class="worker-phase">' + esc(phase) + '</span>' +
      '<span class="worker-meta">CPU ' + fmtPct(w.cpu_percent) + ' · MEM ' + fmtPct(w.memory_percent) + '</span>' +
      '<span class="worker-caps">' + tasksInfo + ' ' + hwInfo + ' ' + concInfo + '</span>' +
    '</div>';
  }

  // ── Render: Task groups ──
  function renderTaskGroupsOverview() {
    const container = document.getElementById('task-groups');
    const { groups, singles } = getTaskGroups();
    const all = [...groups.map(g => ({ type: 'group', data: g })), ...singles.map(s => ({ type: 'single', data: s }))];

    if (all.length === 0) {
      container.innerHTML = '<div class="empty">暂无任务</div>';
      return;
    }

    container.innerHTML = all.map(item => {
      if (item.type === 'group') return groupCardHTML(item.data);
      return taskCardHTML(item.data);
    }).join('');
  }

  function groupCardHTML(g) {
    const shards = g.shards;
    const completedShards = shards.filter(s => s.status === 'completed').length;
    const totalHits = shards.reduce((sum, s) => sum + (s.hits || 0), 0);
    const totalScanned = shards.reduce((sum, s) => sum + (s.scanned_ips || 0), 0);
    const hasActive = shards.some(canCancel);

    return '<div class="group-card">' +
      '<div class="group-header">' +
        '<span class="group-title">组 ' + esc(g.group_id) + '</span>' +
        '<span class="badge ' + (completedShards === shards.length ? 'completed' : 'running') + '">' + completedShards + '/' + shards.length + ' 完成</span>' +
        '<span class="group-hits">' + totalHits + ' 命中</span>' +
        '<span class="group-meta">' + totalScanned + ' IP</span>' +
        (hasActive ? '<button class="btn-cancel" onclick="cancelGroup(\'' + esc(g.group_id) + '\')">取消组</button>' : '') +
      '</div>' +
      '<div class="group-shards">' +
        shards.map(s => shardCardHTML(s)).join('') +
      '</div>' +
    '</div>';
  }

  function shardCardHTML(s) {
    const pct = parseProgress(s.progress);
    const phase = s.phase || '';
    return '<div class="task-card shard-card">' +
      '<span class="task-name">' + esc(s.name || s.id) + '</span>' +
      '<span class="badge ' + s.status + '">' + esc(s.status) + '</span>' +
      '<div class="task-info">' +
        '<span>' + (s.hits || 0) + ' 命中 / ' + (s.scanned_ips || 0) + ' IP</span>' +
      '</div>' +
      '<div class="worker-progress">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill' + (pct >= 100 ? ' complete' : '') + '" style="width:' + Math.min(pct, 100) + '%"></div>' +
        '</div>' +
        '<span class="progress-text">' + esc(phase || s.progress || '0%') + '</span>' +
      '</div>' +
      (canCancel(s) ? '<button class="btn-cancel" onclick="cancelTask(\'' + esc(s.id) + '\')">取消</button>' : '') +
    '</div>';
  }

  // ── Render: Task list ──
  function renderTasks() {
    const tbody = document.querySelector('#task-table tbody');
    if (!tbody) return;
    const taskArr = Object.values(tasks);
    if (taskArr.length === 0) {
      tbody.innerHTML = '<tr><td colspan="10" class="empty">暂无任务</td></tr>';
      return;
    }
    tbody.innerHTML = taskArr.map(t => {
      const pct = parseProgress(t.progress);
      const phase = t.phase || '';
      let workerName = '';
      if (t.assigned_to && workers[t.assigned_to]) {
        workerName = workers[t.assigned_to].name || t.assigned_to;
      }
      const shardInfo = t.shard_total > 1 ? (t.shard_index + 1) + '/' + t.shard_total : '-';
      return '<tr>' +
        '<td>' + esc(t.id ? t.id.substring(0, 8) : '-') + '</td>' +
        '<td>' + esc(t.name || '') + '</td>' +
        '<td>' + esc((t.asns || []).join(', ')) + '</td>' +
        '<td>' + esc((t.ports || []).join(', ')) + '</td>' +
        '<td>' + esc(workerName || '-') + '</td>' +
        '<td><span class="badge ' + t.status + '">' + esc(t.status) + '</span></td>' +
        '<td><div class="progress-bar mini"><div class="progress-fill' + (pct >= 100 ? ' complete' : '') + '" style="width:' + Math.min(pct, 100) + '%"></div></div>' + Math.round(pct) + '%</td>' +
        '<td>' + (t.hits || 0) + '</td>' +
        '<td>' + shardInfo + '</td>' +
        '<td>' + (canCancel(t) ? '<button class="btn-cancel btn-sm" onclick="cancelTask(\'' + esc(t.id) + '\')">取消</button>' : '') + '</td>' +
      '</tr>';
    }).join('');
  }

  // ── Task group helpers ──
  function getTaskGroups() {
    const byGroup = {};
    const singles = [];
    Object.values(tasks).forEach(t => {
      if (t.group_id) {
        if (!byGroup[t.group_id]) byGroup[t.group_id] = [];
        byGroup[t.group_id].push(t);
      } else {
        singles.push(t);
      }
    });
    const groups = Object.entries(byGroup).map(([groupId, shards]) => ({ group_id: groupId, shards }));
    return { groups, singles };
  }

  // ── Expose functions to global scope for onclick handlers ──
  window.cancelTask = cancelTask;
  window.cancelGroup = cancelGroup;
  window.switchTab = switchTab;
  window.quickStart = quickStart;
  window.importASNFile = importASNFile;

  // ── Poll fallback every 5s ──
  function pollWorkers() {
    fetch('/api/worker/list')
      .then(r => r.json())
      .then(list => {
        list.forEach(w => { workers[w.uuid] = w; });
        renderAll();
      })
      .catch(() => {});
  }
  function pollTasks() {
    fetch('/api/task/list')
      .then(r => r.json())
      .then(list => {
        list.forEach(t => { tasks[t.id] = t; });
        renderAll();
      })
      .catch(() => {});
  }

  setInterval(() => { pollWorkers(); pollTasks(); }, 5000);
  setTimeout(() => { pollWorkers(); pollTasks(); }, 500);
})();
