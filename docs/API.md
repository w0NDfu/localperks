# 黑马点评（hm-dianping）前端接口文档

> 文档版本：v1.0  
> 代码基线：当前 `main` 工作区（Spring Boot 2.3，默认端口 `8081`）  
> 生成时间：2026-07-10  
> 说明：本文以当前 Controller、Service、拦截器的**实际实现**为准，而不是按产品设想补全接口。未实现或仅用于演示的能力已明确标注。

## 1. 接入约定

### 1.1 服务地址与请求格式

- 开发环境基地址：`http://localhost:8081`
- 除文件上传外，提交 JSON 时使用 `Content-Type: application/json`。
- 文件上传使用 `multipart/form-data`。
- 项目当前没有 CORS 配置。前端本地开发时应通过 Vite/Webpack 开发服务器代理转发至 `8081`，或由后端补充 CORS 配置；浏览器跨域直连会被拦截。
- 时间字段由 Jackson 序列化，前端按 ISO-8601 本地日期时间字符串处理，例如 `"2026-07-10T14:30:00"`。
- 金额、经纬度和 `id` 均直接来自后端实体。Java `Long` 在 JSON 中以数字输出；若前端需要兼容 JavaScript 大整数精度，请在请求库层将大整数转换为字符串处理。

### 1.2 统一响应体

所有**正常进入 Controller**的接口均使用如下包装：

```json
{
  "success": true,
  "errorMsg": null,
  "data": {},
  "total": null
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `success` | boolean | 业务是否成功 |
| `errorMsg` | string \| null | 失败原因；成功时通常为 `null` |
| `data` | any \| null | 业务数据 |
| `total` | number \| null | 保留字段；当前已实现接口均未设置分页总数 |

业务失败示例（多数业务失败的 HTTP 状态仍为 `200`，必须判断 `success`）：

```json
{
  "success": false,
  "errorMsg": "库存不足",
  "data": null,
  "total": null
}
```

### 1.3 认证与错误处理

1. 先调用“发送验证码”与“登录”。登录成功后，`data` 是纯 token 字符串。
2. 登录态接口在每次请求中携带：`authorization: <token>`。不要自动添加 `Bearer ` 前缀，后端读取的是完整请求头值。
3. token 存于 Redis，服务端每次收到有效请求会续期；配置时长为 36,000 分钟。
4. 访问需登录接口而未登录/令牌无效时，拦截器直接返回 HTTP `401`，通常没有统一 JSON 响应体。
5. 未捕获的 `RuntimeException` 会被统一转为 `{ "success": false, "errorMsg": "服务器异常" }`。参数绑定、404 等框架错误不保证符合该包装。

### 1.4 权限标识

| 标识 | 含义 |
| --- | --- |
| `公开` | 未登录可调用；如果携带有效 token，部分返回字段会附带当前用户状态 |
| `登录` | 必须携带 `authorization` |
| `当前实现公开` | 代码未做鉴权，通常属于管理或测试能力，生产前应增加权限控制 |

### 1.5 分页与图片约定

- `current` 从 `1` 开始。
- 店铺按分类列表每页 `5` 条；店铺名称搜索、热门笔记、用户笔记列表每页 `10` 条。
- 返回的是数组，不包含 `hasNext`、`pages`、`total`；前端以“返回数量少于页大小”判断是否结束。
- `Shop.images` 和 `Blog.images` 是逗号分隔的图片路径字符串，前端需自行 `split(',')` 并过滤空字符串。
- 上传接口返回的路径形式为 `/blogs/{d1}/{d2}/{uuid}.{ext}`。该项目只负责写入本地目录，未提供图片读取接口；页面展示时需要由已配置的 Nginx/静态资源服务器拼接图片域名。

## 2. 数据模型

以下字段为接口实际返回或提交时使用的字段。`createTime`、`updateTime` 多数为数据库自动维护字段，创建/更新请求通常无需提交。

### 2.1 UserDTO（公开用户摘要）

```json
{ "id": 1, "nickName": "user_abcd1234", "icon": "/imgs/icons/1.png" }
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 用户 ID |
| `nickName` | string | 昵称 |
| `icon` | string | 头像路径，可能为空字符串 |

### 2.2 Shop（店铺）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 店铺 ID |
| `name` | string | 店铺名称 |
| `typeId` | number | 店铺分类 ID |
| `images` | string | 多图逗号分隔路径 |
| `area` | string | 商圈 |
| `address` | string | 地址 |
| `x` / `y` | number | 经度 / 纬度 |
| `avgPrice` | number | 人均价格（整数） |
| `sold` | number | 销量 |
| `comments` | number | 评论数 |
| `score` | number | 评分，数据库以 10 倍整数保存；展示时通常除以 10 |
| `openHours` | string | 营业时间，如 `10:00-22:00` |
| `distance` | number | 仅在携带 `x`、`y` 的分类附近搜索中出现，单位为米 |
| `createTime` / `updateTime` | string | 创建 / 修改时间 |

### 2.3 Blog（探店笔记）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 笔记 ID |
| `shopId` | number | 关联店铺 ID |
| `userId` | number | 作者用户 ID；创建时后端覆盖为当前用户 |
| `title` | string | 标题 |
| `images` | string | 最多 9 张图片的逗号分隔路径 |
| `content` | string | 正文 |
| `liked` / `comments` | number | 点赞数 / 评论数 |
| `name` / `icon` | string | 作者昵称 / 头像，仅热榜、详情、关注流中由后端补充 |
| `isLike` | boolean | 当前登录用户是否点赞；未登录查询热榜时该字段可能缺失或为 `null` |
| `createTime` / `updateTime` | string | 创建 / 修改时间 |

### 2.4 Voucher（优惠券）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 优惠券 ID |
| `shopId` | number | 店铺 ID |
| `title` / `subTitle` | string | 标题 / 副标题 |
| `rules` | string | 使用规则 |
| `payValue` / `actualValue` | number | 支付金额 / 抵扣金额，整数金额单位以当前数据为准 |
| `type` / `status` | number | 券类型 / 券状态；当前代码未定义枚举，请按后端数据展示 |
| `stock` | number | 秒杀券库存；普通券通常无此字段或为 `null` |
| `beginTime` / `endTime` | string | 秒杀起止时间；普通券通常无此字段或为 `null` |
| `createTime` / `updateTime` | string | 创建 / 修改时间 |

### 2.5 ScrollResult（关注流游标分页）

```json
{
  "list": ["Blog 数组"],
  "minTime": 1720584000000,
  "offset": 1
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `list` | Blog[] | 本次返回的笔记，最多 2 条 |
| `minTime` | number | 本页最小时间戳，下次作为 `lastId` 传入 |
| `offset` | number | 与 `minTime` 相同的记录数，下次作为 `offset` 传入 |

### 2.6 秒杀订单状态

提交秒杀成功时 `data` 为：

```json
{ "orderId": "700000000000000001", "statusCode": 10, "statusText": "排队中", "requestState": "ACCEPTED" }
```

查询订单状态时 `data` 为：

```json
{
  "orderId": "700000000000000001",
  "statusCode": 12,
  "statusText": "待支付",
  "requestState": "SUCCESS",
  "payTime": null,
  "createTime": "2026-07-10T14:30:00"
}
```

| `statusCode` | `statusText` | 前端建议 |
| --- | --- | --- |
| `10` | 排队中 | 继续轮询 |
| `11` | 处理中 | 继续轮询 |
| `12` | 待支付 | 进入支付页或展示模拟支付按钮 |
| `13` | 支付成功 | 结束轮询，展示成功 |
| `14` | 已取消 | 结束轮询，提示超时取消 |
| `15` | 处理失败 | 结束轮询，提示下单失败 |
| `19` | 受理结果待核对 / 投递结果待核对 | 保留原订单号，继续查询；不要用新订单重试 |

`requestState` 表达订单受理/创建结果：新请求的 `ACCEPTED` 表示已收到 RocketMQ 的 `SEND_OK` 确认，HTTP 不等待最终订单落库；受理阶段不再写 MySQL Outbox。`SUCCESS` 表示查询到真实订单，付款/取消仍由 `statusCode` 表达；`FAILED` 表示已确认的业务失败；`UNKNOWN` 表示尚未确认 Broker 接收结果。HTTP 200 和外层 `success=true` 只表示请求得到了可识别的响应，不能把其中的 `UNKNOWN` 统计为可靠受理。发送超时、刷盘超时不证明未投递，后台按原订单号核对和重试，不能立即释放预留。全局未完成名额达到上限时返回 HTTP 429，不预扣新用户库存。

## 3. 接口总览

| 模块 | 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- | --- |
| 用户 | POST | `/user/code` | 公开 | 发送短信验证码（验证码仅打印到后端日志） |
| 用户 | POST | `/user/login` | 公开 | 手机号 + 验证码登录/自动注册 |
| 用户 | GET | `/user/me` | 登录 | 当前用户摘要 |
| 用户 | GET | `/user/{id}` | 登录 | 用户摘要 |
| 用户 | GET | `/user/info/{id}` | 登录 | 用户扩展资料 |
| 用户 | POST | `/user/sign` | 登录 | 当日签到 |
| 用户 | GET | `/user/sign/count` | 登录 | 连续签到天数 |
| 用户 | POST | `/user/logout` | 登录 | 未实现，固定失败 |
| 店铺 | GET | `/shop/{id}` | 公开 | 店铺详情 |
| 店铺 | GET | `/shop/of/type` | 公开 | 分类/附近店铺列表 |
| 店铺 | GET | `/shop/of/name` | 公开 | 名称搜索 |
| 店铺 | POST | `/shop` | 登录 | 新增店铺 |
| 店铺 | PUT | `/shop` | 登录 | 更新店铺 |
| 分类 | GET | `/shop-type/list` | 公开 | 店铺分类列表 |
| 笔记 | POST | `/blog` | 登录 | 发布笔记 |
| 笔记 | PUT | `/blog/like/{id}` | 登录 | 点赞/取消点赞切换 |
| 笔记 | GET | `/blog/hot` | 公开 | 热门笔记 |
| 笔记 | GET | `/blog/{id}` | 登录 | 笔记详情 |
| 笔记 | GET | `/blog/likes/{id}` | 登录 | 点赞前 5 位用户 |
| 笔记 | GET | `/blog/of/me` | 登录 | 我的笔记 |
| 笔记 | GET | `/blog/of/user` | 登录 | 指定用户笔记 |
| 笔记 | GET | `/blog/of/follow` | 登录 | 关注流游标分页 |
| 关注 | PUT | `/follow/{id}/{isFollow}` | 登录 | 关注或取关 |
| 关注 | GET | `/follow/or/not/{id}` | 登录 | 是否已关注 |
| 关注 | GET | `/follow/common/{id}` | 登录 | 共同关注 |
| 上传 | POST | `/upload/blog` | 公开 | 上传笔记图片 |
| 上传 | GET | `/upload/blog/delete` | 公开 | 删除笔记图片 |
| 优惠券 | POST | `/voucher` | 当前实现公开 | 新增普通券 |
| 优惠券 | POST | `/voucher/seckill` | 当前实现公开 | 新增秒杀券 |
| 优惠券 | GET | `/voucher/list/{shopId}` | 当前实现公开 | 店铺优惠券列表 |
| 秒杀订单 | POST | `/voucher-order/seckill/{id}` | 登录 | 提交秒杀请求 |
| 秒杀订单 | GET | `/voucher-order/status/{id}` | 登录 | 查询异步订单状态 |
| 秒杀订单 | POST | `/voucher-order/mock-pay/{id}/{success}` | 登录 | 演示用模拟支付 |

## 4. 用户与登录

### 4.1 发送验证码

`POST /user/code` · 公开

查询参数：

| 参数 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `phone` | 是 | string | 中国大陆手机号 |

示例：`POST /user/code?phone=13800138000`

成功时 `data` 为 `null`。验证码不通过接口返回，也不接入真实短信服务，而是输出在后端 debug 日志；有效期为 2 分钟。

### 4.2 登录/自动注册

`POST /user/login` · 公开

```json
{ "phone": "13800138000", "code": "123456" }
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `phone` | 是 | 手机号 |
| `code` | 是 | 验证码 |
| `password` | 否 | DTO 中虽有此字段，但当前服务端不使用密码登录 |

成功响应：`data` 为 token 字符串。该手机号不存在时会自动创建昵称为 `user_...` 的用户。

### 4.3 获取当前用户

`GET /user/me` · 登录

响应 `data`：[`UserDTO`](#21-userdto公开用户摘要)。

### 4.4 获取用户信息

`GET /user/{id}` · 登录

响应 `data`：[`UserDTO`](#21-userdto公开用户摘要)；用户不存在时返回成功且 `data: null`。

`GET /user/info/{id}` · 登录

响应 `data` 为扩展资料：

```json
{
  "userId": 1,
  "city": "上海",
  "introduce": "热爱探店",
  "fans": 10,
  "followee": 3,
  "gender": true,
  "birthday": "2000-01-01",
  "credits": 0,
  "level": false
}
```

资料不存在时同样返回成功且 `data: null`。`gender` 中 `true` 为男、`false` 为女；`level` 在实体中为布尔值，当前代码没有进一步的会员等级枚举。

### 4.5 签到

`POST /user/sign` · 登录

标记当前自然日已签到，成功时 `data: null`。重复调用是幂等的。

`GET /user/sign/count` · 登录

返回 `data: number`，表示截至今天的连续签到天数；如果今天未签到，返回 `0`。

### 4.6 登出（未实现）

`POST /user/logout` · 登录

当前固定返回 `success: false`，服务端不会销毁 token。前端暂时只能清除本地 token；如需真正退出，需要后端补充 Redis token 删除逻辑。

## 5. 店铺与分类

### 5.1 获取分类列表

`GET /shop-type/list` · 公开

响应 `data` 为按 `sort` 升序的数组：

```json
[{ "id": 1, "name": "美食", "icon": "/types/ms.png", "sort": 1 }]
```

### 5.2 获取店铺详情

`GET /shop/{id}` · 公开

响应 `data`：[`Shop`](#22-shop店铺)。店铺不存在时业务失败：`errorMsg: "店铺不存在！"`。

### 5.3 分类/附近店铺查询

`GET /shop/of/type` · 公开

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `typeId` | 是 | - | 店铺分类 ID |
| `current` | 否 | `1` | 页码，从 1 开始 |
| `x` | 否 | - | 经度；须与 `y` 同时传入 |
| `y` | 否 | - | 纬度；须与 `x` 同时传入 |

- 未同时传入坐标：按分类查询，每页 5 条，未指定排序。
- 同时传入坐标：在该分类的 Redis 地理索引中查询 5,000 米内店铺，按距离升序，每页 5 条，返回项含 `distance`（米）。
- 查询结果为空时 `data: []`。

示例：`GET /shop/of/type?typeId=1&current=1&x=121.481&y=31.236`

### 5.4 按名称搜索店铺

`GET /shop/of/name` · 公开

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | 否 | - | 店铺名模糊匹配关键字；不传时查询全部 |
| `current` | 否 | `1` | 页码，从 1 开始 |

响应 `data` 为 `Shop[]`，每页 10 条。

### 5.5 新增店铺

`POST /shop` · 登录

请求体为 `Shop`，最小示例：

```json
{
  "name": "示例餐厅",
  "typeId": 1,
  "images": "/blogs/a.jpg,/blogs/b.jpg",
  "area": "陆家嘴",
  "address": "示例路 1 号",
  "x": 121.5,
  "y": 31.2,
  "avgPrice": 88,
  "openHours": "10:00-22:00"
}
```

成功时 `data` 为新建店铺 ID。当前仅验证登录态，未实现商家/管理员角色限制；前端管理页须自行限制入口，但不能将其视为安全边界。

### 5.6 更新店铺

`PUT /shop` · 登录

请求体为要更新的 `Shop` 字段，`id` 必填；可提交部分字段。

```json
{ "id": 1, "name": "新店名", "openHours": "09:00-23:00" }
```

成功时 `data: null`。未提供 `id` 时返回业务失败：`店铺id不能为空`。

## 6. 笔记与关注

### 6.1 发布笔记

`POST /blog` · 登录

```json
{
  "shopId": 1,
  "title": "周末探店",
  "images": "/blogs/1/2/a.jpg,/blogs/1/2/b.jpg",
  "content": "环境很好，推荐。"
}
```

成功时 `data` 为笔记 ID。`userId` 由当前登录用户强制写入，前端不应传递或信任该字段。发布成功后会向该作者的粉丝写入关注流。

### 6.2 热门笔记

`GET /blog/hot` · 公开

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `current` | 否 | `1` | 页码，从 1 开始 |

响应 `data`：`Blog[]`，按 `liked` 降序，每页 10 条。携带有效 token 调用时，每项会带当前用户的 `isLike` 状态；匿名调用该字段可能缺失。

### 6.3 笔记详情与用户笔记

`GET /blog/{id}` · 登录

返回单个 `Blog`，附带作者 `name`、`icon` 及当前用户 `isLike`。笔记不存在时业务失败：`笔记不存在！`。

`GET /blog/of/me?current=1` · 登录

返回当前用户的 `Blog[]`，每页 10 条。该接口只直接查询表数据，因此返回项不会补充 `name`、`icon`、`isLike`。

`GET /blog/of/user?id={userId}&current=1` · 登录

返回指定用户的 `Blog[]`，每页 10 条；同样不补充作者信息和点赞状态。

### 6.4 点赞与点赞用户

`PUT /blog/like/{id}` · 登录

无请求体。此接口是**切换**操作：未点赞则点赞，已点赞则取消点赞；响应成功时 `data: null`。前端应以请求后重新拉取详情/热榜的 `isLike` 和 `liked` 为准，避免因重复点击导致状态相反。

`GET /blog/likes/{id}` · 登录

返回 `data: UserDTO[]`，为最早点赞的最多 5 位用户（Redis ZSet 正序），并非点赞数最高的用户。

### 6.5 关注流（滚动分页）

`GET /blog/of/follow?lastId={timestamp}&offset={offset}` · 登录

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lastId` | 是 | - | 首次加载传当前时间戳（毫秒），后续传上一响应的 `data.minTime` |
| `offset` | 否 | `0` | 首次传 `0`，后续传上一响应的 `data.offset` |

响应 `data`：[`ScrollResult`](#25-scrollresult关注流游标分页)。若没有更多数据，当前实现返回 `data: null`（不是空 `ScrollResult`），前端需要兼容。

### 6.6 关注关系

`PUT /follow/{id}/{isFollow}` · 登录

| 路径参数 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 被关注用户 ID |
| `isFollow` | boolean | `true` 关注，`false` 取关 |

成功时 `data: null`。当前没有阻止关注自己，也没有保证重复关注的幂等性，前端应避免重复提交。

`GET /follow/or/not/{id}` · 登录

返回 `data: boolean`，表示当前用户是否关注该用户。

`GET /follow/common/{id}` · 登录

返回 `data: UserDTO[]`，表示当前用户与路径中用户的共同关注列表；无共同关注时返回 `[]`。

## 7. 图片上传

### 7.1 上传笔记图片

`POST /upload/blog` · 公开

请求为 `multipart/form-data`：

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `file` | 是 | file | 图片文件 |

成功时 `data` 为相对图片路径，例如：

```json
{ "success": true, "errorMsg": null, "data": "/blogs/2/13/uuid.jpg", "total": null }
```

当前实现未校验 MIME、扩展名、文件大小，也没有上传鉴权；生产环境应在后端补齐校验、鉴权和对象存储策略。

### 7.2 删除笔记图片

`GET /upload/blog/delete?name=/blogs/2/13/uuid.jpg` · 公开

成功时 `data: null`。该接口使用 GET 执行删除且没有鉴权，属于当前实现的安全风险；前端仅在编辑取消图片时使用，生产发布前建议改为受保护的 `DELETE` 接口。

## 8. 优惠券与秒杀订单

### 8.1 新增普通券

`POST /voucher` · 当前实现公开

请求体为 `Voucher`，示例：

```json
{
  "shopId": 1,
  "title": "50 元代金券",
  "subTitle": "满 100 可用",
  "rules": "仅限工作日使用",
  "payValue": 50,
  "actualValue": 100,
  "type": 0,
  "status": 1
}
```

成功时 `data` 为优惠券 ID。

### 8.2 新增秒杀券

`POST /voucher/seckill` · 当前实现公开

请求体同时包含 `Voucher` 字段和秒杀扩展字段：

```json
{
  "shopId": 1,
  "title": "秒杀代金券",
  "payValue": 10,
  "actualValue": 100,
  "type": 1,
  "stock": 100,
  "beginTime": "2026-07-11T10:00:00",
  "endTime": "2026-07-11T12:00:00"
}
```

后端会创建基础券、秒杀券库存记录，并初始化 Redis 库存。成功时 `data` 为优惠券 ID。

### 8.3 查询店铺优惠券

`GET /voucher/list/{shopId}` · 当前实现公开

返回 `data: Voucher[]`。秒杀券会包含 `stock`、`beginTime`、`endTime`；普通券这些字段为 `null` 或不出现。

### 8.4 提交秒杀

`POST /voucher-order/seckill/{id}` · 登录

无请求体，`id` 是秒杀优惠券 ID。成功并不代表订单已落库，而是代表 Redis 预扣库存和消息投递成功：

```json
{
  "success": true,
  "data": { "orderId": "700000000000000001", "statusCode": 10, "statusText": "排队中" }
}
```

`orderId` 以十进制字符串返回。前端应保留原字符串查询订单，避免 JavaScript 数值转换丢失精度。库存不足或重复下单时 `success: false`，`errorMsg` 分别为“库存不足”或“不能重复下单”。

### 8.5 查询秒杀订单状态

`GET /voucher-order/status/{id}` · 登录

`id` 是上一步返回的 `orderId`。响应 `data` 为[秒杀订单状态](#26-秒杀订单状态)。建议每 1–2 秒轮询，收到终态（13、14、15）即停止。

当前实现由 Java 按登录用户校验订单归属；读取他人的订单返回不存在。

### 8.6 模拟支付（仅开发演示）

`POST /voucher-order/mock-pay/{id}/{success}` · 登录

| 路径参数 | 类型 | 说明 |
| --- | --- | --- |
| `id` | number | 订单 ID |
| `success` | boolean | `true` 将待支付订单改为已支付；`false` 仅返回“支付失败”，不修改订单 |

示例：`POST /voucher-order/mock-pay/123456789/true`

成功时 `data` 为“支付成功”或“支付失败”字符串。Java 校验当前用户与订单归属；此接口仅用于开发模拟，不是真实支付通道。

## 9. 当前未提供的页面接口

以下实体或页面需求在项目中尚没有对应 Controller 方法，前端应先隐藏入口、使用静态数据，或与后端约定新增接口：

- 笔记评论：`BlogCommentsController` 为空，没有评论列表、发布、回复、删除、点赞等 API。
- 用户资料编辑、头像修改、密码登录与真正登出：均未实现。
- 店铺删除、优惠券编辑/删除、完整订单明细页面接口：未实现；已有个人券包和订单状态查询。
- 真实支付、支付回调、退款、券核销：未实现；只有模拟支付和超时取消流程。
- 角色/商家/管理员授权：未实现。当前新增店铺、优惠券管理、上传和图片删除等高风险接口缺少相应权限控制。

## 10. 前端联调建议

1. 在请求拦截器中统一附加 `authorization`；收到 `401` 时清除 token 并跳转登录页。
2. 在响应拦截器中先判断 HTTP 状态，再判断 `body.success`。不要把 HTTP `200` 当作成功。
3. 对笔记点赞使用“请求中禁用按钮 + 请求后刷新数据”的方式，因为接口是 toggle，而不是显式的 `like/unlike`。
4. 秒杀页保存 `orderId` 并按状态码轮询；页面离开或得到终态时取消定时器。
5. 图片选择后先调用上传接口，将返回路径组成逗号分隔字符串，再提交笔记；在开发环境配置静态资源域名，例如 `VITE_IMAGE_BASE_URL`。
6. 管理功能即使当前能直接调用，也应在前端按角色/环境隐藏，并将接口权限缺口列入后端上线前改造项。


## 领券中心与个人券包

- `GET /voucher/market` 返回可展示的各店优惠券，包含 `shopName` 和 Redis 实时秒杀库存。
- `GET /voucher-order/mine?page=1&ownedOnly=false` 需要登录，返回 `data.records`、`total` 和 `availableCount`。每页 12 条；`ownedOnly=true` 仅显示已支付的券。
- 个人券包同时显示已落库订单及尚在排队或失败的消息记录。状态查询及模拟支付只允许操作当前用户自己的订单。
- 重复抢同一券时返回原订单，前端按原订单状态继续展示。每个用户每张券对应一笔订单。
- 活动时间以 Java 服务本地时间为准。库存预扣后发现活动无效会回滚预扣。
- 状态码 16、17、18 分别表示已核销、退款中、已退款，用于展示历史状态。
- 当前本地数据中秒杀券金额为分，历史普通券为元。前端按券类型兼容这批已有数据。
- `scripts/seed-seckill-vouchers.ps1` 给六家已有普通券的测试商家补充秒杀券，默认每家 30 张、活动持续 30 天；已有秒杀券的商家会跳过，重复运行保留现有库存。

## AI 助手（独立 Python 服务）

默认地址 `http://127.0.0.1:8090`，开发前端通过 `/agent-api` 代理。接口返回普通 JSON，不使用 Java 的 `success/data` 包装。

- `GET /agent/status`：返回模型配置是否完整、模型 ID、Redis/Java 连通性。`configured` 不等于已验证令牌有效。
- `POST /agent/chat`：请求 `{"message":"帮我查询咖啡店的券"}`，可加服务端返回的 `session_id`；返回 `session_id`、`answer`、`trace`。
- `POST /agent/chat/stream`：同上，返回 SSE；事件包括 `session`、`model_request`、`delegation`、`tool_start`、`tool_result`、`delegation_result`、`done`，失败返回 `error`。连接建立前的校验错误使用 HTTP 状态码和 `detail.code/message`。
- `GET /agent/conversations/{session_id}`：读取当前用户的完成轮次。会话保留最近八轮、24 小时过期；不存在或身份不符返回 404。

个人查询沿用原始 `authorization` 登录头，Python 每轮先向 Java 验证用户身份。模型密钥只在 Python 本地 `.env` 配置，前端不传模型密钥。工具只允许固定 Java 查询接口，没有任意 URL 或 SQL 执行能力。

测试可用 `$env:PLAYWRIGHT_CHANNEL='chrome'; npm run e2e` 使用已安装的 Chrome。

## AI 平台 v0.3：消费决策、任务与确认交易

原 `/agent/chat`、`/agent/chat/stream` 和直接店铺查询保留。请求可以附加结构化条件；所有字段由 Pydantic 验证，不接受 `user_id`：

```json
{
  "message": "推荐附近两个人吃饭人均100元以内最划算的三家",
  "constraints": {
    "location": {"longitude": 120.16477, "latitude": 30.25492},
    "people": 2,
    "budget_per_person": 100,
    "visit_time": "2026-11-05T19:00:00+08:00",
    "radius_m": 5000,
    "top_k": 3
  }
}
```

示例日期应替换为真实计划时间；过去时间会追问。可以只提供自然语言，让服务追问缺少条件。成功响应仍有 `answer/session_id/trace`，新增 `task_id/trace_id/state/intent/metrics`；消费决策附 `constraints/decision`。

`decision.recommendations` 为按明确规则校验的推荐；`unverified_candidates` 为使用条件未能确认的候选；`rejected/warnings/coverage/truncated` 解释排除、失败与覆盖。`face_saving` 为足额使用券面的差额；`estimated_cash_cost/estimated_per_person` 是基于店铺人均参考价的估算。

### 任务状态与恢复

登录用户可使用 `POST /agent/tasks` 异步受理：请求为 `{"request_id":"UUID","message":"查询店铺4详情","session_id":"可选UUID"}`，返回 HTTP 202 和 `task_id/session_id/state/duplicate`。相同用户和请求编号只保存一个任务；同编号改变消息、会话或条件返回 409。任务与幂等记录在同一 PostgreSQL 事务提交。游客继续使用原同步/SSE公开查询。

`WS /agent/tasks/{task_id}/ws` 只用于通知，连接成功后 5 秒内发送 `{"authorization":"Java登录令牌","after":0}`。令牌不放在 URL。事件附全局递增的 `seq`，终态发 `type=snapshot/state/output`；断线用下表中的 GET 状态和事件补偿。前端按当前身份重新读取 action 获取确认凭证，凭证不写任务事件。

登录异步任务使用 PostgreSQL 共享队列，按持久受理顺序保证同会话跨实例 FIFO。默认每实例16个执行者、全局256路、队列10000条、每用户32条，均可配置。短期令牌经认证加密存于 Redis，默认900秒；执行前重新鉴权，其他实例可接管未完成任务。凭据过期返回等待认证状态，原用户重新登录后以原请求编号重交；完成任务重复提交仅返回原结果。同步/SSE兼容入口仍使用会话锁，不属于共享 FIFO 队列。WebSocket 使用每实例一个 PostgreSQL LISTEN 连接通知，15秒补查兜底。[配置与测试口径](AGENT_SHARED_CONCURRENCY.md)

个人偏好接口：`GET /agent/preferences` 返回 `preferences/revision/updated/source`；`PUT /agent/preferences` 接收 `{"revision":0,"preferences":{"budget_per_person":80,"cuisine_preferences":["日料"],"excluded_categories":["火锅"],"answer_style":"concise"}}`；`DELETE /agent/preferences?revision=1` 清除偏好并推进版本。所有接口由 Java 登录身份确定所属用户，不接受用户ID；版本冲突返回409，游客返回401。支持预算、半径、偏好分类、排除分类和简洁/适中/详细风格；当前明确要求优先，不自动从浏览或聊天写入长期画像。

| 方法 | 路径 | 行为 |
|---|---|---|
| GET | `/agent/tasks/{task_id}` | 当前身份的任务状态、请求、输出和检查点 |
| GET | `/agent/tasks/{task_id}/events?after=0` | 按序读取最多500条持久化事件 |
| POST | `/agent/tasks/{task_id}/resume` | 与聊天相同请求格式，补充/修改条件；`message` 必填，`继续`复用原目标 |
| POST | `/agent/chat/stream` | 可附 `task_id` 恢复原任务并继续接收 SSE |

任务恢复校验原身份与会话；业务实时数据重查。Redis 缓存过期时可由 PostgreSQL 持久化任务恢复对话。未过期执行租约返回409；不存在/他人任务返回404。任务、检查点、事件、交易幂等和模型用量统一写入 `AGENT_STATE_DSN` 指向的 PostgreSQL；旧 SQLite 卷保留为迁移源，不再承载在线任务。[状态与调度配置](AGENT_SHARED_CONCURRENCY.md)

SSE 新增 `task/route/plan/node_start/node_result/checkpoint_restored/decision/explanation/rag/degradation/order_status`。`done` 表示本轮回答结束，业务 `state` 仍可能为 `waiting_input/waiting_confirmation/failed`，不能把 HTTP200 或 SSE建立成功当成业务成交。

`metrics` 包含实际模型调用次数、模型输入/输出/总 Token、缺失用量次数、工具调用和 Java请求数、Embedding Token、任务耗时。`cost.currency_amount` 在没有账单依据时为null，不虚构费用。

### 交易两步确认与结果核对

所有接口使用 LocalPerks 原始 `authorization` 头，每次向 Java 验证当前身份。确认接口不会调用 LLM。

1. `POST /agent/actions/prepare`：`{"operation":"seckill","voucher_id":8,"idempotency_key":"UUID"}`。取消使用 `operation=cancel/order_id="订单号"`；开发模拟支付使用 `operation=mock_pay` 且服务配置必须显式开启。
2. 返回 `action_id/state/preview/confirmation_token/expires_at`。客户端展示摘要，不能自动确认。
3. `POST /agent/actions/{id}/confirm`：`{"confirmed":true,"confirmation_token":"prepare返回值"}`。服务再查价格、规则、活动/订单状态，使用 CAS 幂等提交。
4. `GET /agent/actions/{id}` 查询结果；`POST /agent/actions/{id}/reconcile` 对 `unknown/running` 只读核对，不重复发 POST。`GET /agent/actions/{id}/events` 查看当前用户的交易审计与恢复事件。

确认凭证10分钟过期。相同用户与幂等键只对应同一操作，换目标返回409。`confirmed=false`、额外用户ID、不合法订单号被拒绝；未登录401，他人操作404。结果不明时不可自动生成新幂等键重试写操作。

Python 仅调用真实 Java 路径：`POST /voucher-order/seckill/{voucherId}`、`POST /voucher-order/cancel/{orderId}`、受控 `POST /voucher-order/mock-pay/{orderId}/true`。取消为本次唯一新增 Java 业务接口，校验本人及未支付状态，重复取消返回已取消状态。模拟支付不是实际扣款。

### 共享知识接口

- `GET /agent/knowledge/search?query=问题&baseline=false`：默认 BM25 + Dense + RRF + Rerank；`baseline=true` 为词法基线。返回证据、策略、用量和耗时。
- `GET /agent/knowledge/sources/{32位十六进制chunk_id}`：读取公开知识证据，不接收文件路径；旧版本来源仍可查询。
- 管理员在本机通过 `python -m rag.cli --repo ..` 显式导入。没有公开上传或任意 SQL/文件工具。

知识库只保存店铺说明、券规则和 FAQ，不存实时库存、价格或个人订单。Rerank 不可用会明确降级；没有证据时说明无法确认。

### 真实模型回答与证据标记

消费决策、上下文跟进和知识回答可附 `answer_origin=llm_grounded`、`answer_composition`、`model_composition_raw`、`answer_evidence`、`evidence_digest`。模型生成自然段与事实引用，服务校验后填入业务事实槽；可用保存的段落和事实逐字重建最终 `answer`。这些字段表达回答来源，不代表业务成交或永久库存保证。

`decision_context` 保存显示顺序及当前讨论店铺，用于“第二家”“这家”的指代。人数、预算或时间修改后重新查询业务事实；“今晚”没有具体钟点时不沿用此前日期作为确定使用保证。解释失败会明确记录 `degradation`，不能计为真实模型全流程成功。

知识回答附 `sufficiency` 和 `abstained`：检索命中只表示相关，具体店铺金额、库存、接待人数及指定日期资格仍需实时业务证据。一般知识文档不足时，回答应说明无法确认并引用适用边界。

## P4 管理员运维接口

此节保留为历史实验接口，已移出默认主系统。Python 需显式设 `AGENT_OPS_ENABLED=true`，Java 需设 `HMDP_OPS_ENABLED=true`；默认分别不注册路由和返回 404。实验页面需独立设置 `VITE_OPS_ENABLED=true`，主导航不展示入口。以下凭证校验仅在启用后生效。

Agent 运维接口使用 `X-Ops-Token` 和进程配置的 `OPS_ADMIN_TOKEN`。Java 内部接口使用独立凭证 `HMDP_OPS_ADMIN_TOKEN`，Agent 通过 `OPS_JAVA_TOKEN` 提供它。凭证需至少 32 字符。运维任务以管理员作用域保存，调查结论包含证据 ID、原始观测、核验结果和人工建议。

| 方法 | Agent 路径 | 用途 |
| --- | --- | --- |
| GET | `/agent/ops/access` | 验证管理员凭证及读取调查上限 |
| POST | `/agent/ops/investigations` | 发起只读调查，记录任务和模型用量 |
| GET | `/agent/ops/investigations` | 列出最近 50 条管理员调查 |
| GET | `/agent/ops/investigations/{task_id}` | 获取报告、时间线和请求用量 |
| POST | `/agent/ops/investigations/{task_id}/resume` | 关联原任务并重新采样 |

发起调查需提供 `symptom` 和 `scope`。`scope` 包含 Unix 毫秒 `start`、`end`、正整数 `voucher_id` 和有效 `ip`。事件范围最长 15 分钟，起点限最近一天。工具读取当前库存和 MQ 位点时会单独标记采样时间。一次调查最多 8 次工具读取、8 次模型请求和 300 秒，并发上限为 2。

Java 提供 `GET /internal/ops/{tool}`，接受 `start`、`end`、`voucherId` 和 `ip`。`tool` 取 `spring`、`events`、`traffic`、`mq`、`inventory`、`delivery`、`compensation`。证据包含来源、窗口、采样时间及 SHA-256。事件日志来自进程内有界记录，响应包含覆盖状态和截断信息。MQ 查询失败以查询失败证据呈现，位点值只来自 Broker 实际返回。

缺失或无效管理员凭证返回 `403`，Java 运维路径拒绝写方法并返回 `405`。Agent 的 POST 仅创建、关联和持久化调查，处置建议由管理员评审。（历史实验报告未随公开版上传）

P4.1 调查响应增加 `route`、`symptom`、`verified_categories`、`findings` 和 `trigger_operation`。`route.path` 为 `rules_fastpath` 或 `agent_deep`。`route.reasons` 说明直达或升级理由，调查中可从 `ops_route` 事件获取。深度路径的 `review` 区分模型选择通过、证据门槛拒答及等待人工复核。规则直达可在未配置模型时使用。（历史实验报告未随公开版上传）

## 秒杀直接确认与关单持久化

前置活动/历史订单核验默认使用`hmdp.seckill.eligibility-batch`：2个读取线程、每批最多32个用户/券、凑批最多1 ms、队列1,024、调用方等待2秒。每次读取新鲜SQL结果，按输入序号映射回本人请求，不缓存资格或省略校验；数据库连接总预算不变。核验队列满、超时或关闭返回429和Retry-After，发生在订单编号生成及Redis预留之前。`HMDP_ELIGIBILITY_BATCH_ENABLED=false`保留逐请求查询对照。[测试说明](TESTING.md)

2026-11-12移除新请求的受理Outbox，2026-11-13进一步改为Broker微批确认：Redis Lua原子预留库存、资格、原订单编号和UNKNOWN回执，随后进入有界发送队列，只有对应批次收到SEND_OK后才返回ACCEPTED。默认2个发送线程、每批最多32条、凑批最多1 ms、队列1,024条，调用方最多等待3秒，MQ发送本身超时3秒，SDK同步发送自动重试次数为0。队列满或等待超时返回UNKNOWN，不能将进入内存队列当成可靠受理；超时不取消可能已发送的消息。跨实例按订单编号领取10秒发送租约，Redis恢复扫描沿用原编号，处理预留后退出及Broker确认丢失。确认后Redis回执更新失败不撤销Broker确认，消费端仍按唯一键和订单状态幂等处理。Broker默认SYNC_FLUSH，副本策略以部署配置为准。

`hmdp.seckill.broker-batch`配置该队列和批次预算；`HMDP_BROKER_BATCH_ENABLED=false`保留原逐条发送用于受控对照，此时`max-inflight-sends=64`才限制逐条发送并发。正常微批路径不再先等待这64个名额。只合并同一普通秒杀Topic且无延迟级别的消息，订单编号和消息体仍逐条保留；关单延迟消息不进入该批次。

`GET /voucher-order/status/{id}`在订单尚未落库时读取本人Redis回执；`GET /voucher-order/mine`将本人最近最多100条Redis回执与SQL订单/历史消息合并分页，排除已落库的重复项。完成回执保留24小时，用户最近索引保留48小时，未完成回执不自动过期。Redis回执不代表消息已获Broker确认，只有状态ACCEPTED或真实订单才能确认相应结果。异常消费的终态失败记录在MySQL提交后才补偿Redis，并检查预留所属订单，避免旧消息释放新请求的库存。

兼容调度每5秒扫描升级前已提交的受理Outbox，继续投递原记录；新请求不新增这些记录，不删除历史数据。`HMDP_OUTBOX_BATCH_ENABLED`及受理合批器已移除。

关单Outbox保留，与订单创建在同一事务持久化；派发使用MySQL 8的`FOR UPDATE SKIP LOCKED`短事务批量领取租约，提交后执行网络发送，默认间隔100 ms、在途上限32。当前受理链路与压测见[测试说明](TESTING.md)，[测试说明](TESTING.md)及[测试说明](TESTING.md)作为历史对照保留。
