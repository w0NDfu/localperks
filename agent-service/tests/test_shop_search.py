import asyncio

import httpx
import pytest

from main import create_app


def request_search(respond, **params):
    async def search():
        app = create_app(transport=httpx.MockTransport(respond))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                return await client.get("/tools/shops/search", params=params)

    return asyncio.run(search())


def test_search_forwards_keyword_and_page_to_shoplink(monkeypatch):
    monkeypatch.setenv("SHOPLINK_BASE_URL", "http://localperks.test:8081")
    shops = [{"id": 2, "name": "餐厅", "avgPrice": 42}]

    def respond(request):
        assert request.url.host == "localperks.test"
        assert request.method == "GET"
        assert request.url.path == "/shop/of/name"
        assert dict(request.url.params) == {"name": "餐厅", "current": "2"}
        return httpx.Response(200, json={"success": True, "data": shops})

    response = request_search(respond, keyword=" 餐厅 ", page=2)

    assert response.status_code == 200
    assert response.json() == {"page": 2, "data": shops}


def test_empty_results_are_a_successful_search():
    def respond(request):
        return httpx.Response(200, json={"success": True, "data": []})

    response = request_search(respond)

    assert response.status_code == 200
    assert response.json() == {"page": 1, "data": []}


@pytest.mark.parametrize(
    ("upstream", "code"),
    [
        (httpx.Response(200, json={"success": False}), "SHOPLINK_BUSINESS_ERROR"),
        (httpx.Response(200, json={"success": True, "data": [42]}), "SHOPLINK_INVALID_RESPONSE"),
        (httpx.Response(200, text="<html>"), "SHOPLINK_INVALID_RESPONSE"),
        (httpx.Response(500), "SHOPLINK_HTTP_ERROR"),
    ],
)
def test_rejects_failed_or_invalid_shoplink_responses(upstream, code):
    def respond(request):
        return upstream

    response = request_search(respond)

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == code


@pytest.mark.parametrize(
    ("failure", "status", "code"),
    [
        (httpx.ConnectError, 502, "SHOPLINK_UNAVAILABLE"),
        (httpx.ReadTimeout, 504, "SHOPLINK_TIMEOUT"),
    ],
)
def test_reports_connection_failures(failure, status, code):
    def respond(request):
        raise failure("simulated connection failure", request=request)

    response = request_search(respond)

    assert response.status_code == status
    assert response.json()["detail"]["code"] == code


def test_rejects_invalid_page_before_contacting_shoplink():
    def respond(request):
        pytest.fail("invalid query reached LocalPerks")

    response = request_search(respond, page=0)

    assert response.status_code == 422
