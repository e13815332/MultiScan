(function() {
  'use strict';

  // ── WebSocket Connection ──
  let ws = null;
  let reconnectTimer = null;
  const workers = {};
  const tasks = {};

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = proto + '//' + location.host + '/api/ws';
    ws = new WebSocket(url);
    ws.onopen = () => { console.log('WS connected'); };
    ws.onclose = () => {
      console.log('WS disconnected, reconnecting in 3s...');
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectWS, 3000);
    };
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        handleWSEvent(msg);
      } catch(e) { console.warn('WS parse error', e); }
    };
  }

  function handleWSEvent(msg) {
    if (msg.type === 'worker_online' || msg.type === 'worker_update') {
      const w = msg.worker;
      if (w) workers[w.uuid] = w;
      renderWorkers();
      renderOverview();
    } else if (msg.type === 'worker_offline') {
      if (msg.uuid && workers[msg.uuid]) workers[msg.uuid].online = false;
      renderWorkers();
      renderOverview();
    } else if (msg.type === 'task_update' || msg.type === 'task_completed') {
      const t = msg.task;
      if (t) tasks[t.id] = t;
      renderTasks();
      renderTaskGroupsOverview();
      renderOverview();
    }
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
    if (p.endsWith('%')) return parseInt(p, 10) || 0;
    return 0;
  }

  // ── Tab switching ──
  function switchTab(name) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.classList.remove('active');
      if (btn.dataset.tab === name) btn.classList.add('active');
    });
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    const panel = document.getElementById('tab-' + name);
    if (panel) panel.classList.add('active');
    if (name === 'workers') renderWorkers();
    else if (name === 'tasks') renderTasks();
    else if (name === 'overview') renderOverview();
  }

  document.addEventListener('DOMContentLoaded', () => {
    connectWS();

    // Tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Overview: scan button
    document.getElementById('btn-scan').addEventListener('click', () => {
      const asnInput = document.getElementById('asn-input');
      const portInput = document.getElementById('port-input');
      const shardInput = document.getElementById('quick-shards');
      const asns = asnInput.value.trim().split(/[,\s]+/).filter(Boolean);
      if (asns.length === 0) { alert('请输入 ASN'); return; }
      const ports = portInput.value.trim().split(/[,\s]+/).filter(Boolean).map(Number).filter(n => n > 0);
      if (ports.length === 0) { alert('请输入端口'); return; }
      const shards = parseInt(shardInput.value) || 1;
      // Use form-encoded POST matching /api/task/create
      const form = new URLSearchParams();
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
        if (d.error) alert('错误: ' + d.error);
        else { renderOverview(); switchTab('tasks'); }
      }).catch(e => alert('请求失败: ' + e));
    });

    // Overview: import file
    document.getElementById('file-input').addEventListener('change', (e) => {
      const file = e.target.files[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        const text = ev.target.result;
        const asns = [];
        const lines = text.split('\n');
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          // Support "AS13335", "13335", "AS12345/ASN"
          const match = trimmed.match(/AS?(\d+)/i);
          if (match) asns.push('AS' + match[1]);
        }
        if (asns.length > 0) {
          document.getElementById('asn-input').value = asns.join(', ');
          alert('已导入 ' + asns.length + ' 个 ASN');
        } else {
          alert('未找到有效 ASN');
        }
      };
      reader.readAsText(file);
    });
  });

  // ── API calls ──
  function cancelTask(taskId) {
    if (!confirm('取消任务 ' + taskId + '?')) return;
    fetch('/api/task/' + encodeURIComponent(taskId) + '/cancel', { method: 'POST' });
  }
  function cancelGroup(groupId) {
    if (!confirm('取消组 ' + groupId + '?')) return;
    fetch('/api/group/' + encodeURIComponent(groupId) + '/cancel', { method: 'POST' });
  }
  function canCancel(t) {
    return t.status === 'pending' || t.status === 'assigned' || t.status === 'running';
  }

  // ── Render: Overview ──
  function renderOverview() {
    const con = document.getElementById('overview-stats');
    const counts = { pending: 0, assigned: 0, running: 0, completed: 0, failed: 0, cancelled: 0 };
    let totalHits = 0;
    Object.values(tasks).forEach(t => {
      if (counts[t.status] != null) counts[t.status]++;
      totalHits += t.hits || 0;
    });
    const totalTasks = Object.keys(tasks).length;
    const onlineWorkers = Object.values(workers).filter(w => w.online).length;
    const offlineWorkers = Object.values(workers).filter(w => !w.online).length;

    con.innerHTML = `
      <div class="stat-card"><span class="stat-num">${onlineWorkers}</span><span class="stat-label">在线 Worker</span></div>
      <div class="stat-card"><span class="stat-num">${offlineWorkers}</span><span class="stat-label">离线 Worker</span></div>
      <div class="stat-card"><span class="stat-num">${counts.pending}</span><span class="stat-label">待处理</span></div>
      <div class="stat-card"><span class="stat-num">${counts.running + counts.assigned}</span><span class="stat-label">运行中</span></div>
      <div class="stat-card"><span class="stat-num">${counts.completed}</span><span class="stat-label">已完成</span></div>
      <div class="stat-card"><span class="stat-num">${totalHits}</span><span class="stat-label">总命中</span></div>
    `;
  }

  // ── Render: Workers ──
  function renderWorkerCards() {
    const container = document.getElementById('worker-cards-overview');
    if (!container) return;
    const arr = Object.values(workers);
    if (arr.length === 0) { container.innerHTML = '<div class="empty">暂无 Worker</div>'; return; }
    container.innerHTML = arr.map(w => workerCardHTML(w)).join('');
  }

  function renderWorkers() {
    const container = document.getElementById('workers-list');
    if (!container) return;
    const arr = Object.values(workers);
    if (arr.length === 0) { container.innerHTML = '<div class="empty">暂无 Worker</div>'; return; }
    container.innerHTML = arr.map(w => workerCardHTML(w)).join('');
  }

  function workerCardHTML(w) {
    const status = w.online ? (w.status || 'online') : 'offline';
    const pct = parseProgress(w.progress);
    const phase = w.phase || '';
    const taskLink = w.current_task
      ? '<span class="worker-task">任务 <a href="#" onclick="event.preventDefault();switchTab(\'tasks\')">' + esc(w.current_task) + '</a></span>'
      : '';

    // Capabilities info
    const caps = w.capabilities || {};
    const tasksInfo = caps.max_tasks
      ? '<span class="worker-cap">&#' + '128203; ' + (w.running_tasks || 0) + '/' + caps.max_tasks + '</span>'
      : '';
    const hwInfo = caps.cpu_count
      ? '<span class="worker-cap">&#' + '128187; ' + caps.cpu_count + '核/' + caps.memory_mb + 'MB</span>'
      : '';
    const concInfo = caps.max_concurrent
      ? '<span class="worker-cap">&#' + '9889; ' + caps.max_concurrent + '并发</span>'
      : '';

    return '<div class="worker-card">' +
      '<span class="worker-name">' + esc(w.name || w.uuid) + '</span>' +
      '<span class="worker-status ' + status + '">' + status + '</span>' +
      taskLink +
      '<div class="worker-progress">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill' + (pct >= 100 ? ' complete' : '') + '" style="width:' + Math.min(pct, 100) + '%"></div>' +
        '</div>' +
        '<span>' + esc(w.progress || '0%') + '</span>' +
      '</div>' +
      '<span class="worker-phase">' + phase + '</span>' +
      '<span class="worker-meta">CPU ' + fmtPct(w.cpu_percent) + ' &middot; MEM ' + fmtPct(w.memory_percent) + '</span>' +
      '<span class="worker-caps">' + tasksInfo + ' ' + hwInfo + ' ' + concInfo + '</span>' +
    '</div>';
  }

  // ── Render: Task groups (overview section) ──
  function renderTaskGroupsOverview() {
    const container = document.getElementById('task-groups');
    const { groups, singles } = getTaskGroups();
    const all = [...groups.map(g => ({ type: 'group', data: g })), ...singles.map(s => ({ type: 'single', data: s }))];

    if (all.length === 0) {
      container.innerHTML = '<div class="empty">暂无任务</div>';
      return;
    }

    container.innerHTML = all.map(item => {
      if (item.type === 'group') {
        return groupCardHTML(item.data);
      }
      return taskCardHTML(item.data);
    }).join('');
  }

  function groupCardHTML(g) {
    const shards = g.shards;
    const completedShards = shards.filter(s => s.status === 'completed').length;
    const totalHits = shards.reduce((sum, s) => sum + (s.hits || 0), 0);
    const totalScanned = shards.reduce((sum, s) => sum + (s.scanned_ips || 0), 0);
    const allDone = completedShards === shards.length;
    const status = allDone ? 'completed' : 'running';
    const hasActive = shards.some(canCancel);

    return '<div class="group-card">' +
      '<div class="group-header">' +
        '<span class="group-title">组 ' + esc(g.group_id) + '</span>' +
        '<span class="badge ' + status + '">' + completedShards + '/' + shards.length + ' 完成</span>' +
        '<span class="group-hits">' + totalHits + ' 命中</span>' +
        '<span class="group-meta">' + totalScanned + ' IP</span>' +
        (hasActive ? '<button class="btn-cancel" onclick="cancelGroup(\'' + esc(g.group_id) + '\')">取消组</button>' : '') +
      '</div>' +
      '<div class="group-shards">' +
        shards.map(s => shardCardHTML(s, g.group_id)).join('') +
      '</div>' +
    '</div>';
  }

  function shardCardHTML(s, groupId) {
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
        '<span>' + esc(phase || s.progress || '0%') + '</span>' +
      '</div>' +
      (canCancel(s) ? '<button class="btn-cancel" onclick="cancelTask(\'' + esc(s.id) + '\')">取消</button>' : '') +
    '</div>';
  }

  // ── Render: Task list (Tasks tab) ──
  function renderTasks() {
    const container = document.getElementById('tasks-list');
    if (!container) return;

    const taskArr = Object.values(tasks);
    if (taskArr.length === 0) {
      container.innerHTML = '<div class="empty">暂无任务</div>';
      return;
    }

    container.innerHTML = taskArr.map(t => taskCardHTML(t)).join('');
  }

  function taskCardHTML(t) {
    const pct = parseProgress(t.progress);
    const phase = t.phase || '';
    let workerName = '';
    if (t.assigned_to && workers[t.assigned_to]) {
      workerName = workers[t.assigned_to].name || t.assigned_to;
    }
    return '<div class="task-card">' +
      '<span class="task-name">' + esc(t.name || t.id) + '</span>' +
      '<span class="badge ' + t.status + '">' + esc(t.status) + '</span>' +
      (workerName ? '<span class="worker-ref">Worker: ' + esc(workerName) + '</span>' : '') +
      '<div class="task-info">' +
        '<span>' + (t.hits || 0) + ' 命中 / ' + (t.scanned_ips || 0) + ' IP</span>' +
        (t.total_ips ? '<span>共 ' + t.total_ips + ' IP</span>' : '') +
        (t.shard_total > 1 ? '<span>分片 ' + (t.shard_index + 1) + '/' + t.shard_total + '</span>' : '') +
      '</div>' +
      '<div class="worker-progress">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill' + (pct >= 100 ? ' complete' : '') + '" style="width:' + Math.min(pct, 100) + '%"></div>' +
        '</div>' +
        '<span>' + esc(phase || t.progress || '0%') + '</span>' +
      '</div>' +
      (canCancel(t) ? '<button class="btn-cancel" onclick="cancelTask(\'' + esc(t.id) + '\')">取消</button>' : '') +
    '</div>';
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
    const groups = Object.entries(byGroup).map(([groupId, shards]) => ({
      group_id: groupId,
      shards
    }));
    return { groups, singles };
  }

  // ── Auto-refresh via polling (fallback) ──
  function fetchState() {
    fetch('/api/state')
      .then(r => r.json())
      .then(data => {
        if (data.workers) {
          data.workers.forEach(w => { workers[w.uuid] = w; });
        }
        if (data.tasks) {
          data.tasks.forEach(t => { tasks[t.id] = t; });
        }
        renderOverview();
        renderWorkerCards();
        renderTaskGroupsOverview();
        // Only update active tab panels
        const activePanel = document.querySelector('.tab-panel.active');
        if (activePanel) {
          if (activePanel.id === 'tab-workers') renderWorkers();
          else if (activePanel.id === 'tab-tasks') renderTasks();
        }
      })
      .catch(e => console.warn('Poll error', e));
  }

  // Poll every 5s as fallback (WebSocket fills gaps)
  setInterval(fetchState, 5000);
  setTimeout(fetchState, 500);
})();
