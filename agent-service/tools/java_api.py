from typing import Any

import httpx


class ShopLinkError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class JavaAPI:
    def __init__(self, client: httpx.AsyncClient, token: str = ""):
        self.client = client
        self._token = token

    async def get(self, path: str, *, params: dict | None = None,
                  authenticated: bool = False, evidence: list | None = None) -> Any:
        return await self.request("GET", path, params=params, authenticated=authenticated, evidence=evidence)

    async def post(self, path: str, *, evidence: list | None = None):
        # Mutation retries are intentionally absent. Workflow reconciles ambiguous results.
        return await self.request("POST", path, authenticated=True, evidence=evidence)

    async def request(self, method, path, *, params=None, authenticated=False, evidence=None):
        if authenticated and not self._token:
            raise ShopLinkError(401, "LOGIN_REQUIRED", "请先登录 LocalPerks，再查询个人券包或订单")
        headers = {"authorization": self._token} if authenticated else {}
        status = None
        try:
            response = await self.client.request(method, path, params=params, headers=headers)
            status = response.status_code
            if status == 401:
                raise ShopLinkError(401, "LOGIN_REQUIRED", "登录已过期，请重新登录 LocalPerks")
            if status == 429:
                raise ShopLinkError(429, "SHOPLINK_RATE_LIMITED", "查询过于频繁，请稍后再试")
            response.raise_for_status()
            result = response.json()
        except httpx.TimeoutException as exc:
            raise ShopLinkError(504, "SHOPLINK_TIMEOUT", "LocalPerks 请求超时") from exc
        except httpx.RequestError as exc:
            raise ShopLinkError(502, "SHOPLINK_UNAVAILABLE", "无法连接 LocalPerks，请确认 Java 后端已启动") from exc
        except httpx.HTTPStatusError as exc:
            raise ShopLinkError(502, "SHOPLINK_HTTP_ERROR", "LocalPerks 返回 HTTP 错误") from exc
        except ValueError as exc:
            raise ShopLinkError(502, "SHOPLINK_INVALID_RESPONSE", "LocalPerks 返回了无效响应") from exc
        finally:
            if evidence is not None:
                evidence.append({"method": method, "path": path, "status": status})

        if not isinstance(result, dict) or not isinstance(result.get("success"), bool):
            raise ShopLinkError(502, "SHOPLINK_INVALID_RESPONSE", "LocalPerks 返回了无效响应")
        if not result["success"]:
            raise ShopLinkError(502, "SHOPLINK_BUSINESS_ERROR", "LocalPerks 查询未成功，请在业务页面核对输入")
        return result.get("data")

