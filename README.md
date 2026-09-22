# 实验室仪器数据接收 API

基于 **Python + FastAPI + SQLite** 的仪器测量数据接收服务。仪器以设备为单位发送带序号（seq）的批量测量包，服务负责：

- **幂等去重**：每个包携带 `idempotency_key`，重复键（仪器重发）不会二次入库，只记录到 `duplicates` 表；
- **乱序暂存**：序号缺口前方的包先落入暂存区（`staged`），不阻塞接收；
- **按序提交**：当缺口补齐后，暂存区中连续的包一次性"提交"（`committed`），提交水位线 `last_seq` 连续推进；
- **缺口/已提交/重复包查询**：提供设备维度的查询接口。

容器启动时会自动安装依赖（镜像构建阶段已预装，开发态挂载源码时会补装）并自动创建 SQLite 数据库与全部表结构，无需手动迁移。

## 目录结构

```
.
├── app/
│   ├── main.py            # FastAPI 入口，启动时初始化数据库
│   ├── config.py          # 环境变量配置
│   ├── database.py        # SQLite 连接、WAL、建表、事务
│   ├── service.py         # 核心逻辑：去重 / 暂存 / 按序提交 / 查询
│   ├── schemas.py         # 请求/响应模型
│   └── routers/api.py     # HTTP 路由
├── tests/test_api.py      # 端到端测试
├── docker/entrypoint.sh   # 容器启动脚本（装依赖 + 建库）
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── requirements-dev.txt
```

## 数据模型

| 表 | 说明 |
|---|---|
| `devices` | 设备。`start_seq` 为起始序号（默认 1），`last_seq` 为已连续提交的最大序号 |
| `packets` | 测量包。`committed_at IS NULL` 表示暂存中，非空表示已提交；`(device_id, seq)` 唯一、`idempotency_key` 全局唯一 |
| `duplicates` | 所有被拒绝的重复包，含 `reason` 与 `original_packet_id` |

序号提交规则：

1. `seq < start_seq` → `422`；
2. `idempotency_key` 已存在 → `duplicate`（`reason=idempotency_key`），不覆盖旧包；
3. `seq <= last_seq`（该序号已提交）→ `duplicate`（`reason=seq_already_committed`）；
4. 同序号已有暂存包但键不同 → `duplicate`（`reason=seq_already_staged`）；
5. 否则暂存新包，随后从 `last_seq + 1` 开始，把所有连续的暂存包一次性提交。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 服务：<http://localhost:8000>
- 交互式文档（Swagger）：<http://localhost:8000/docs>
- 健康检查：`GET http://localhost:8000/api/v1/health`
- SQLite 数据库文件位于 Docker 卷 `instrument-data`，容器内路径 `/data/measurements.db`

## 本地运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

export DB_PATH=./data/measurements.db   # 可选，默认 /data/measurements.db
uvicorn app.main:app --reload --port 8000
```

## API 一览（前缀 `/api/v1`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/devices/{device_id}/packets` | 发送单个测量包 |
| POST | `/devices/{device_id}/packets/batch` | 发送批量测量包 |
| GET | `/devices/{device_id}/gaps` | 查询缺口、暂存序号、下一个期望序号 |
| GET | `/devices/{device_id}/committed` | 查询已提交序号（分页 `limit/offset`） |
| GET | `/devices/{device_id}/duplicates` | 查询重复包（分页） |
| GET | `/devices/{device_id}/status` | 设备汇总状态 |
| GET | `/devices` | 设备列表 |
| GET | `/health` | 健康检查 |

未知设备在**首次发包时自动注册**；查询不存在的设备返回 `404`。

### 请求示例

**批量发送（含乱序）**

```bash
curl -s -X POST http://localhost:8000/api/v1/devices/centrifuge-7/packets/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "packets": [
      {"seq": 1, "idempotency_key": "c7-1", "measurements": [{"channel": "rpm", "value": 3000}]},
      {"seq": 3, "idempotency_key": "c7-3", "measurements": [{"channel": "rpm", "value": 3012}]},
      {"seq": 2, "idempotency_key": "c7-2", "measurements": [{"channel": "rpm", "value": 3008}]}
    ]
  }'
```

响应：seq 1 到达即提交；seq 2、3 补齐缺口后连续提交。

```json
{
  "device_id": "centrifuge-7",
  "received": 3,
  "committed_count": 3,
  "staged_count": 0,
  "duplicate_count": 0,
  "committed_now": [1, 2, 3],
  "staged_seqs": [],
  "duplicates": [],
  "results": [ ... ]
}
```

**仪器重发（相同 idempotency_key）**

```bash
curl -s -X POST http://localhost:8000/api/v1/devices/centrifuge-7/packets \
  -H 'Content-Type: application/json' \
  -d '{"seq": 1, "idempotency_key": "c7-1", "measurements": [{"channel": "rpm", "value": 3000}]}'
```

```json
{
  "duplicate_count": 1,
  "duplicates": [
    {"seq": 1, "reason": "idempotency_key", "original_packet_id": 1, "duplicate_id": 1}
  ],
  "committed_now": []
}
```

**查询缺口**（假设已提交到 3，seq 5 已暂存）

```bash
curl -s http://localhost:8000/api/v1/devices/centrifuge-7/gaps
```

```json
{
  "device_id": "centrifuge-7",
  "start_seq": 1,
  "last_committed_seq": 3,
  "next_expected_seq": 4,
  "staged_seqs": [5],
  "gap_seqs": [4],
  "gap_ranges": [[4, 4]]
}
```

`gap_ranges` 把离散缺口压缩成 `[起点, 终点]` 区间，缺口很多时更紧凑。

**查询已提交序号 / 重复包**

```bash
curl -s 'http://localhost:8000/api/v1/devices/centrifuge-7/committed?limit=100&offset=0'
curl -s 'http://localhost:8000/api/v1/devices/centrifuge-7/duplicates?limit=100&offset=0'
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DB_PATH` | `/data/measurements.db` | SQLite 文件路径 |
| `DEFAULT_START_SEQ` | `1` | 新设备的起始序号 |
| `MAX_BATCH_SIZE` | `1000` | 单批次最大包数 |

## 并发与持久化说明

- SQLite 启用 **WAL 模式**；进程内使用单连接 + 可重入锁，`BEGIN IMMEDIATE` 事务保证"读取水位线 → 暂存 → 连续提交"原子完成，并发请求安全。
- 数据库文件通过 Compose 的命名卷 `instrument-data` 持久化，容器重建数据不丢失。
- 备份时建议同时复制 `.db`、`.db-wal`、`.db-shm` 三个文件（或先执行 `wal_checkpoint`）。

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

测试覆盖：顺序提交、乱序暂存与补齐、幂等键去重、同序号重复、已提交序号回放、缺口区间计算、分页查询、404/422 边界。
