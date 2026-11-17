# 本地依赖与排查

完整安装步骤见[根目录 README](../README.md#本地运行)。三个应用的源码现在都在同一仓库，Agent 位于 `agent-service/`。

1. 复制根目录 `.env.example` 为 `.env`，设置自己的数据库密码。
2. 安装 Agent 依赖，复制它的 `.env.example`，运行 `python -m api.bootstrap` 生成 PostgreSQL 配置与共享密钥。
3. 在根目录执行 `docker compose --profile agent up -d`，等待 `docker compose --profile agent ps` 中的健康检查就绪。
4. 分别启动 Java、Agent 和 Vue 开发服务。

## 常见排查命令

```powershell
docker compose --profile agent ps
docker compose logs --tail=100 mysql
docker compose logs --tail=100 redis
docker compose logs --tail=100 rocketmq-broker
docker compose logs --tail=100 agent-postgres
```

不要把包含登录令牌、数据库密码或用户消息的日志直接发到公开 Issue。

- **MySQL 拒绝登录**：检查根目录 `.env` 与首次创建数据卷时的账号、密码是否一致。修改配置不会自动修改已有数据库用户密码。
- **RocketMQ 连接失败**：确认 NameServer 与 Broker 已启动，主机端口未被占用，Java 配置与 Compose 暴露地址一致。
- **Agent 提示缺少共享密钥**：在 `agent-service` 目录运行 `python -m api.bootstrap`。多实例应使用同一份安全配置。
- **没有知识检索结果**：确认 pgvector 数据库就绪后，按 README 导入知识源；只配置向量模型不等于已经建立索引。
- **店铺图片空白**：原学习库的一些演示图片不在仓库中，前端提供占位显示。新上传图片默认保存到 `frontend/public/imgs`；生产静态服务需映射同一目录。

开发结束用 `docker compose --profile agent --profile dashboard stop` 保留数据。不要用删除数据卷代替排查。
