# LocalPerks 前端

Vue 3、TypeScript、Pinia 和 Element Plus 实现商家、笔记、优惠券、本人券包与智能助手页面。完整启动步骤见[根目录 README](../README.md)。

## 本地开发

在本目录执行 `npm ci`，然后 `npm run dev`。Vite 默认监听5173，将 `/api` 代理至 Java 的8081，将 `/agent-api`（含 WebSocket）代理至 Agent 的8090。登录验证码在本地 Java 调试日志查看，没有接入短信服务。

常用检查：`npm run lint`、`npm run test`、`npm run build`、`npm run e2e`。浏览器测试使用 API fixture；Windows 可设置 `PLAYWRIGHT_CHANNEL=msedge`。

## Nginx 容器

先从仓库根目录启动 Java 与 Agent。然后在本目录执行 `docker compose up --build -d`，访问 `http://localhost:8080`。

默认将 `./public/imgs` 挂载至容器，与从仓库根目录启动的 Java 默认图片上传位置对应；自定义时，`HM_DP_IMAGE_DIR` 与 Java 的 `HMDP_IMAGE_UPLOAD_DIR` 应指向同一目录。

`/health` 检查 Nginx，`/api` 和 `/agent-api` 代理宿主机服务，`/imgs` 返回挂载图片。详细配置见 [Nginx 指南](../docs/NGINX_FRONTEND_GUIDE.md)。停止时执行 `docker compose stop`，保留容器与数据。

支付为本地模拟；真实支付、退款、到店核销需要额外接入。内容编辑、上传等学习接口的权限边界应在对外部署前进一步收紧。
