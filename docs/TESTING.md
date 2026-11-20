# 测试与验证

测试分为 Java、前端、Agent 功能回归，以及单独的压力与模型评测。不要把不同测试的指标混在一起。

## Java

在仓库根目录执行 `mvn -DskipTests package` 构建。完整的 `scripts/maven.ps1 test` 会加载本地数据库配置；加载 Spring 上下文或实际 Redis 的测试需要对应服务。

订单受理、消息合批、权限和状态相关代码也有使用 Mockito 的独立测试，可以用 Maven 的 `-Dtest=...` 选择执行。

2026-11-20 的 Redis 秒杀入口回归可从仓库根目录执行：

```powershell
.\agent-service\.venv\Scripts\python.exe tests/load/run_redis_admission.py
```

脚本建立独立 MySQL、Redis、RocketMQ 并运行真实 Java JAR，覆盖 MySQL 暂停时仍可受理、恢复后异步落库、并发库存、活动预热与取消补偿；随后执行包含 11 项真实 MySQL/Redis 集成用例的 53 项定向 Java 测试。原始结果由脚本保存到本地 `artifacts/test-runs/`，公开[验证摘要](validation/redis-admission-20261120.json)及[设计说明](SECKILL_REDIS_ADMISSION.md)。测试完成自动清理隔离实例并停止项目服务，保留原有业务数据卷。本轮是正确性验证，不是 QPS 容量测试。

## 前端

在 `frontend` 执行 `npm ci`，再运行 `npm run lint`、`npm run test`、`npm run build`。浏览器测试使用 Playwright 和 API 替身，不向真实业务服务提交订单。Windows 可设置 `PLAYWRIGHT_CHANNEL=msedge` 使用已安装的 Edge。

## Agent

根目录的 `tests/persistence/run.py` 使用 `agent-service/.venv` 中的 Python 启动。它建立全新 PostgreSQL/Redis，分配独立端口、schema、资源标签与清理记录；不会使用正常业务卷。

覆盖内容包括：

- 相同请求编号只产生一份任务，同编号不同参数被拒绝。
- 同会话受理顺序、跨会话并行、跨进程领取、租约接管和旧执行者保护。
- 队列容量、每用户预算、过期重提、登录失效和本人恢复。
- 跨用户任务、事件、工具与偏好隔离。
- 模型 RPM、TPM、在途并发、未知用量保留与输出上限。
- 偏好版本、删除、条件优先级、分类筛选、金额计算和排序。
- 模型不能修改明确预算，回答不得编造事实或改变程序排序。

模型与 Java HTTP 在这组测试中使用测试替身，外部请求被拦截。隔离存储使用临时内存文件系统，验证进程与事务行为，不代表掉电持久性。

可设置 `AGENT_TEST_CLUSTER_LOAD=1` 额外运行2000独立用户任务。它使用两个应用实例和模型替身；另有两个独立Python进程的协调回归。报告保存在本地 `artifacts/test-runs/`，不进入公开源码。

## 历史验证与重命名

2026-11-20 更新在待发布 monorepo 目录重新验证：`mvn -o clean verify` 构建通过，默认测试73项通过、4项按条件跳过；Agent 完整隔离回归183项通过、2项跳过，没有真实模型调用。前述 Java 默认测试与53项定向集成回归存在重叠，不相加。[发布检查](validation/release-20261120.json)

2026-11-17 公开整理版本重新验证：Java 构建成功，7个订单及消息测试类共41项通过；前端 ESLint、27项单测、类型检查与构建通过，Edge 的6项助手页面测试通过；Agent 隔离 PostgreSQL/Redis 回归164项通过、2项跳过。跳过项分别是需要知识导入的 pgvector 检索与需要显式开启的2000用户负载，没有真实模型调用。隔离运行器使用短临时路径，避免 Windows 深目录下的嵌套清理测试超过路径长度限制。

README 中的162项完整回归、43项最后专项、27项前端单测及6项页面测试来自重命名前的功能分支记录。完整与专项存在重叠，不能相加。公开整理版本调整了目录、品牌显示、默认图片位置和启动说明；本次发布的检查结果会另列在 README 中。

阶段性2000用户负载全部完成，耗时约51.12秒、受理P95约11.12秒，模型替身并发峰值6。供应商真实模型只做过一次工具调用兼容检查，返回55 Token，不能据此推算千路真实推理能力。

## 压测与公开数据

`tests/load/` 保留 JMeter、k6 和数据准备代码。部分脚本是特定实验的记录，需要本地准备工具与依赖，不是启动项目的必经步骤。提交前应先读脚本说明，使用隔离数据。

公开电商检索评测通过下载脚本准备，原始语料、向量、下载缓存和供应商原始响应不上传。真实模型、Embedding、Rerank 评测会消耗自行配置账户的额度，应与普通零模型回归分开执行。
