# MultiScan v2

分布式 CF 反代 IP 扫描流水线 — masscan 端口扫描 → TLS 检测 → 090227 API 验证。

## 架构

```
Master (VPS)  ←→  PostgreSQL (队列/结果)
    ↓ NATS
Worker × N   (masscan → TLS scan → 090227 verify)
```

三阶段：

| 阶段 | 说明 | 并发 |
|------|------|------|
| Phase1 | masscan 端口扫描（流式写库，Web 实时进度） | 按节点速率 |
| Phase2a | TLS 握手检测 Cloudflare 证书 | 500 |
| Phase2b | 090227 API 验证 CF 代理可用性 | 32 |

## 快速开始

```bash
# Master（需要 PostgreSQL + NATS）
PG_PASSWORD=xxx ./multiscan-v2-master

# Worker
PG_PASSWORD=xxx ./multiscan-v2-worker \
  -id worker-1 \
  -name worker-1 \
  -pg-host 127.0.0.1 \
  -nats nats://127.0.0.1:4222
```

Web 面板: `http://master:8801`

## 项目结构

```
.
├── internal/          # 共享库（masscan, resolver, scanner, verifier）
├── v2/
│   ├── cmd/
│   │   ├── master/    # Master 节点
│   │   └── worker/    # Worker 节点
│   └── internal/
│       └── pg/        # PostgreSQL 操作
├── go.mod
└── v2/go.mod
```

## 配置

| 环境变量 | 说明 | 默认值 |
|----------|------|--------|
| `PG_PASSWORD` | PostgreSQL 密码 | `multiscan` |

Worker 硬件探测自动缓存于 `/var/lib/multiscan/hardware.json`，加 `-reprobe` 强制重测。
