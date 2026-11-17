# Repository Guidelines

## 项目结构与模块划分

- `src/main/java/com/hmdp/`：Spring Boot 后端，包含控制器、服务、MyBatis-Plus Mapper、实体、DTO、缓存、限流及消息模块。
- `src/main/resources/`：应用配置、Mapper XML 和 Lua 脚本；`db/` 存放数据库结构与种子数据，`redis/` 存放 Redis 初始化脚本。
- `src/test/java/com/hmdp/`：后端单元测试与集成测试，包结构对应业务代码。
- `frontend/src/`：Vue 3/TypeScript 页面、组件、API 封装、Pinia 状态、样式及就近放置的单元测试；浏览器测试位于 `frontend/e2e/`。
- `docs/`：API、缓存、RocketMQ 和 Nginx 文档；Docker 配置位于 `docker/` 及各 Compose 文件。

## 构建、测试与本地开发

后端命令在仓库根目录执行，需安装 Maven。Java 目标版本为 8，当前 Lombok 配置支持使用 JDK 17 构建。

- `mvn spring-boot:run`：启动后端，默认端口为 8081。
- `mvn test`：运行后端测试，所需外部服务见测试规范。
- `mvn -DskipTests package`：跳过测试执行并打包 JAR。
- `docker compose up -d`：启动 MySQL、Redis 和 RocketMQ；先在根目录 `.env` 中配置数据库密码，详见 `docs/DOCKER_LOCAL_DEV.md`。
- `.\scripts\start-backend.ps1`：读取根目录 `.env` 的数据库设置并启动后端。
- `.\scripts\maven.ps1 test` 或 `install`：加载 `.env` 后执行 Maven 测试或构建；IDEA Maven Runner 需单独配置相同环境变量。

进入 `frontend/` 后执行：

- `npm ci`：按锁文件安装依赖。
- `npm run dev`：启动 Vite，默认端口为 5173，`/api` 请求代理至后端。
- `npm run build`：执行类型检查并构建生产资源。
- `npm run lint`、`npm run test`、`npm run e2e`：分别运行 ESLint、Vitest 和 Playwright。

## 代码风格与命名约定

Java 使用四空格缩进，类名采用 PascalCase，成员采用 camelCase，常量采用 UPPER_SNAKE_CASE。延续 `IShopService`/`ShopServiceImpl` 等接口与实现类命名。业务逻辑放在 Service，持久化操作放在 Mapper。Vue/TypeScript 格式与相邻代码保持一致；组件文件使用 PascalCase，工具文件使用 camelCase。前端使用 ESLint，后端未配置 Java 格式化工具。

## 测试规范

后端使用 JUnit 5 和 Mockito，测试类命名为 `*Test`，方法名应表达行为，如 `shouldRejectExpiredRequests`。可通过 `mvn -Dtest=ShopServiceImplTest test` 单独运行测试。Redis 集成测试依赖 Redis，加载 Spring 上下文的测试还需要配置 MySQL 和 RocketMQ。现有测试会写入缓存及 Redis 数据，应使用可清理的本地测试数据。前端测试文件命名为 `*.spec.ts`，Playwright 使用 API 模拟数据。当前未设置覆盖率门槛，修改行为时应补充回归测试。

## 提交与 Pull Request 规范

历史提交同时采用简短中文描述和 `feat:`、`docs:` 等前缀。建议使用明确、简洁的消息，如 `fix: 防止优惠券重复下单`。PR 应说明行为变化、关联相关 Issue、列出已执行的检查和服务依赖；界面变更附截图，接口或数据库结构变更同步更新文档。

## 安全与配置

通过环境变量或已忽略的本地配置提供 `HMDP_DB_URL`、`HMDP_DB_USERNAME` 和 `HMDP_DB_PASSWORD`，不要提交凭据。按 README 初始化 MySQL 和 Redis，并确保后端图片上传路径与前端 Nginx 配置一致。

## 开发结束后的服务清理

按用户要求，每次修改并验证完成后，自动关闭本项目的前端、Java 后端、相邻 `agent-service` 服务及项目 Docker 容器，除非用户明确要求保留运行。使用 `docker compose --profile agent --profile dashboard stop` 保留容器和数据卷；只终止已确认属于本项目的进程，并检查相关端口已释放。
