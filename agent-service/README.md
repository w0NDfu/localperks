# LocalPerks 智能客服与消费助手

FastAPI + OpenAI 兼容 SDK + Router/Supervisor/DAG/ReAct + 确定性优惠计算 + pgvector 混合检索。业务身份、库存和订单始终由同仓库 Java 后端持有。当前实现为自研编排，不能描述成基于 nanobot 二次开发。

## 启动

Python 3.12+，创建 `.venv` 后执行 `python -m pip install -e '.[test]'`。仅首次从 `.env.example` 创建 `.env`，不要覆盖已有密钥。入口 `python main.py`，监听 127.0.0.1:8090。

示例模型配置为 `gpt-6-luna`，OpenLux API 地址 `https://api.openlux.ai/v1`。`OPENAI_REASONING_EFFORT=none` 用于当前 Chat Completions 工具调用兼容；Embedding 使用 `text-embedding-3-small`、1536 维，Rerank 使用 `BAAI/bge-reranker-v2-m3`。凭据只放忽略文件或环境变量。[官方 Luna 工具调用要求](https://developers.openai.com/api/docs/models/gpt-6-luna)

Java/Redis/MySQL/RocketMQ/pgvector 启动和知识导入见[根目录 README](../README.md) 与 [依赖说明](../docs/DOCKER_LOCAL_DEV.md)。Agent 状态 `AGENT_STATE_DSN` 默认复用 `PGVECTOR_DSN`，使用独立 schema；不回退 SQLite。历史库只作为迁移源保留。

## 主链路

- 店铺、券、个人订单和券包查询；多条件推荐经追问、并行查券、Decimal 估算和证据约束解释。
- `POST /agent/tasks` 登录后快速受理，`request_id` 按身份唯一、同键异义返回409；任务和去重记录同事务提交。
- 登录用户的异步任务通过 PostgreSQL 共享队列按持久受理顺序执行，同会话跨实例 FIFO；默认每实例16路、集群256路、队列10000条，均可配置。
- 短期登录凭据以 Fernet 认证加密后保存到 Redis，默认900秒，不进入任务表。执行前重新向 Java 鉴权；实例退出后其他实例可以接管，凭据失效则等待用户重新认证。所有实例必须共享 AGENT_CREDENTIAL_KEY。
- 模型 HTTP 连接池复用，Redis Lua 统一控制相同账户命名空间和模型的 RPM、TPM 与在途请求。输出 Token 有上限；未知用量保留预留额度，不按零用量处理。
- WebSocket 首帧认证，以每实例一个 PostgreSQL LISTEN 连接通知任务变化，15秒补查兜底；持久化事件序号和 HTTP 查询用于断线恢复。
- 我的偏好支持查看、版本条件更新和清除；当前需求优先于会话条件，会话条件优先于长期默认。预算与分类排除由程序过滤，偏好影响排序，风格用于回答生成；口味无法代替菜品或过敏原核验。
- `core/hooks.py` 的 Harness 生命周期检查请求边界，工具前统一 Schema、身份和风险检查。写工具不能经普通执行器提交，使用 Prepare / 本人 Confirm / Reconcile。
- 公共知识 BM25 + Dense/pgvector + RRF + Rerank，证据充分性单独检查。Redis 会话、PostgreSQL 任务按用户隔离，不把个人聊天放进公共向量库。

原 `/agent/chat`、SSE 和任务恢复接口保留。游客只能走原公开查询；异步任务要求 Java 登录身份。前端根据 `async_tasks` 能力标记接入异步接口。

首次升级运行 `python -m api.bootstrap`，为已有 `.env.platform` 补入共享加密密钥，保留原数据库和模型凭据。集群配置、失败恢复边界和测试见[共享调度文档](../docs/AGENT_SHARED_CONCURRENCY.md)。同步兼容接口仍使用会话锁；跨实例 FIFO 的入口是 `/agent/tasks`。

## 验证与可选实验

仓库根目录的 `tests/persistence/run.py` 在临时 PostgreSQL/Redis 上验证全部回归，`tests/customer_agent/` 包含异步接口、幂等、会话调度、Hook 和模型兼容测试。公开 Multi-CPR 评测与在线知识库完全分开；工具与样本位于 `evaluations/`，本地原始报告不公开上传。

故障诊断默认不注册，需显式 `AGENT_OPS_ENABLED=true`；Java 和前端还需要各自实验开关及管理员凭证。历史 `ops/` 代码与报告保留，排除出主系统能力及简历客服指标。

结束后关闭本服务及项目 Java/Vite，执行 `docker compose --profile agent --profile dashboard stop`，保留容器与数据卷。
