# Multiscan 🔍

**Distributed Cloudflare proxy IP scanner** — 全 Go monorepo，Master-Worker 架构，支持亿级 CIDR 全量扫描。

```
                    ┌──────────────────┐
                    │   Web Panel      │
                    │  :8800 (HTTP)    │
                    └────────┬─────────┘
                             │ REST + WS
                    ┌────────▼─────────┐
                    │   Master         │
                    │ 任务调度 / 分片    │
                    │ worker_store     │
                    │ task_scheduler   │
                    └────────┬─────────┘
                             │ WebSocket
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──────┐ ┌────▼──────┐ ┌────▼──────┐
     │  Worker 1     │ │ Worker 2  │ │ Worker N  │
     │ masscan       │ │ masscan   │ │ masscan   │
     │ cf-scanner    │ │ cf-scanner│ │ cf-scanner│
     │ verify        │ │ verify    │ │ verify    │
     └───────────────┘ └───────────┘ └───────────┘
```

## 特性

- **全量 CIDR 扫描** — Master 调用 RIPE API 解析 ASN → CIDR，贪心均分 shard，Worker 直接喂 masscan（不取样，不生成中间 IP 文件）
- **Master-Worker 架构** — 单 Master 管理 N 个 Worker，WebSocket 通信 + 心跳保活
- **Web 面板** — 纯 HTML+JS 内嵌在 Master 二进制中，零外部依赖
- **硬件防守三层** — Master 按 `max_tasks` 调度不超额发、Runner 按 `max_concurrent` 硬 cap 并发、Worker 超限时直接 reject
- **快慢均衡** — `max_tasks=1` + 完成即拉取：快 Worker 连续干，慢 Worker 不拖累
- **systemd 保活** — 崩溃自动重启，开机自启，SSH 断线不影响
- **多 Worker 并发** — 每个 Worker 独立跑 masscan + cf-scanner + verify，结果汇总到 Master
- **REST API** — 创建/取消任务、查看 Worker 状态、查询扫描结果

## 快速开始

### 一键安装

```bash
# 从 GitHub Release 安装（推荐）
curl -sL https://github.com/e13815332/multiscan/releases/latest/download/install.sh | bash

# 从源码安装
git clone https://github.com/e13815332/multiscan.git
cd multiscan
make build-all
sudo make install-systemd
```

### 单机快速部署

```bash
# 1. 编译
make build-all

# 2. 一键安装（Master + Worker + systemd + CLI 命令）
sudo bash install.sh all

# 3. 访问面板
#    http://<你的IP>:8800

# 4. 查看状态
multiscan status
```

### 分布式部署

**机器 A — Master:**

```bash
sudo bash install.sh master
```

**机器 B/C/D — Worker:**

```bash
sudo bash install.sh worker ws://<master-ip>:8800/api/worker/ws <worker-name>
```

## 快捷命令

安装后可通过 `multiscan` 命令快速操作：

```bash
multiscan                    # 显示服务状态
multiscan start              # 启动 Master + Worker
multiscan stop               # 停止所有服务
multiscan restart            # 重启所有服务
multiscan status             # 详细状态
multiscan logs               # 查看日志（最近 30 行）
multiscan logs master        # 仅 Master 日志
multiscan logs worker        # 仅 Worker 日志
multiscan update             # 从 GitHub Release 更新
multiscan uninstall          # 一键卸载
multiscan version            # 版本信息
```

## 使用示例

### 1. 扫描单个 ASN

在 Web 面板输入：
- **ASN**: `AS13335`
- **端口**: `443,8443,2053,2083,2087,2096`
- **分片数**: `4`
- 点击「提交扫描」

> Master 自动调用 RIPE API 解析 ASN → CIDR 列表，贪心均分到 4 个 shard，依次分配给空闲 Worker。

### 2. 扫描多个 ASN

```
AS13335,AS209242,AS63949
```

多个 ASN 用逗号分隔，Master 合并所有 CIDR 后统一分片。

### 3. 导入 CIDR 文件

在面板粘贴 CIDR 列表（每行一个），选择端口后提交。

## 架构详情

### 工作流程

```
用户创建任务
     │
     ▼
Master 解析 ASN → CIDR（RIPE API）
     │
     ▼
Master DistributeCIDRs（贪心均分，按前缀长度平衡）
     │
     ▼
Master 创建 N 个 shard，每个含同构的 CIDR 列表
     │
     ▼
TaskScheduler 轮询 pendingQ → 找空闲 Worker
     │
     ▼
Worker pull → 领 1 个 shard
     │
     ▼
Worker.runner 执行三阶段扫描：
  ┌───────────────────────────────────┐
  │ 1. masscan (全端口 + CIDR 扫描)    │
  │ 2. cf-scanner (HTTP proxy 验证)   │
  │ 3. verify (二次确认, server=cloudflare+CF-RAY) │
  └───────────────────────────────────┘
     │
     ▼
结果 → 回传 Master → 面板实时更新
     │
     ▼
Worker 空闲 → 发 pull → 领下一个 shard
```

### 硬件防守

| 层级 | 位置 | 机制 |
|------|------|------|
| 1 | Master 调度器 | `running_tasks < max_tasks` 才分配 |
| 2 | Worker Runner | cf-scanner 并发硬 cap `≤200` |
| 3 | Worker 注册 | 上报 CPU/内存，`max_tasks=1` |

### 二进制大小对比

| 架构 | Master | Worker |
|------|--------|--------|
| linux/amd64 | ~5.5 MB | ~5.5 MB |
| linux/arm64 | ~5.3 MB | ~5.3 MB |

## 注意事项 ⚠️

1. **权限** — masscan 需要 root（或 `CAP_NET_RAW`）。install.sh 用 systemd 以 root 运行。
2. **网络** — masscan 会发送大量 SYN 包，确保目标网络/防火墙不拦截。建议速率 ≤ 15000 pps（`max_rate=15000`）。
3. **libpcap** — masscan 依赖 libpcap。Ubuntu: `apt install libpcap0.8`；CentOS: `yum install libpcap`。
4. **资源消耗** — 一台 16 核/16GB 机器可跑 3-4 个并行 Worker。masscan + cf-scanner 各约 50MB 内存。
5. **WebSocket 心跳** — Worker 每 30 秒发心跳，Master 60 秒无心跳判离线。systemd 自动重启离线 Worker。
6. **ASN 解析** — 使用 RIPEStat API（免费，无需 Key），国内网络可能需要代理。
7. **端口选择** — 建议扫描 Cloudflare 常用端口：443, 8443, 2053, 2083, 2087, 2096。

## API 参考

| 端点 | 方法 | 说明 |
|------|------|------|
| `/` | GET | Web 面板 |
| `/api/worker/ws` | WebSocket | Worker 通信 |
| `/api/dashboard/ws` | WebSocket | 面板实时更新 |
| `/api/worker/list` | GET | Worker 列表 |
| `/api/worker/get?uuid=` | GET | 单个 Worker |
| `/api/task/create` | POST | 创建扫描任务 |
| `/api/task/list` | GET | 任务列表 |
| `/api/task/get?id=` | GET | 单个任务 |
| `/api/task/counts` | GET | 任务统计 |
| `/api/task/cancel` | POST | 取消任务 |
| `/health` | GET | 健康检查 |

## 卸载

```bash
multiscan uninstall
# 或
sudo bash scripts/uninstall.sh
```

## 许可证

MIT
