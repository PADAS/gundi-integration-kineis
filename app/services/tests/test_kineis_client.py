"""Tests for Kineis API client (CONNECTORS-836)."""

import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx

from app.services.errors import format_error_message
from app.services.kineis_client import (
    get_access_token,
    retrieve_bulk_telemetry,
    retrieve_realtime_telemetry,
    retrieve_device_list,
    fetch_device_list,
    fetch_telemetry,
    fetch_telemetry_realtime,
)


@pytest.mark.asyncio
async def test_get_access_token_success(mocker):
    """Auth endpoint returns access_token and expires_in."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "test-token-123",
        "expires_in": 300,
        "refresh_token": "refresh-xyz",
        "token_type": "Bearer",
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_AUTH_BASE_URL", "https://account.example.com")
    mocker.patch("app.services.kineis_client._auth_path", return_value="/a")
    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await get_access_token(username="u", password="p")

    assert result["access_token"] == "test-token-123"
    assert result["expires_in"] == 300
    assert mock_post.called


def _token_endpoint_answering(mocker, response: httpx.Response):
    mocker.patch("app.services.kineis_client.settings.KINEIS_AUTH_BASE_URL", "https://account.example.com")
    mocker.patch("app.services.kineis_client._auth_path", return_value="/token")
    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value=response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)


@pytest.mark.asyncio
async def test_token_endpoint_rejection_quotes_the_response_body(mocker):
    """The activity log keeps only the first line of the error: it must say what the
    identity server answered, not just the status, or a WAF block and a Keycloak
    refusal read the same ("Client error '403 Forbidden' for url ...")."""
    request = httpx.Request("POST", "https://account.example.com/token")
    body = {"error": "access_denied", "error_description": "Blocked by policy"}
    _token_endpoint_answering(mocker, httpx.Response(403, json=body, request=request))

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        await get_access_token(username="u", password="p")

    exc = excinfo.value
    assert exc.response.status_code == 403  # is_retryable_failure and the refresh fallback read this
    first_line = str(exc).splitlines()[0]
    assert "403" in first_line
    assert "Blocked by policy" in first_line
    portal_text = format_error_message(exc)
    assert portal_text.startswith("Authentication failed — CLS token endpoint answered 403: ")
    assert '"access_denied"' in portal_text and "Blocked by policy" in portal_text
    assert portal_text.endswith("(HTTP 403)")


@pytest.mark.asyncio
async def test_token_endpoint_rejection_body_is_one_bounded_line(mocker):
    """An HTML block page is folded onto one line and cut short, so the portal entry
    stays readable and the full page never lands in an event."""
    request = httpx.Request("POST", "https://account.example.com/token")
    page = "<html>\n  <body>\n    <h1>Access denied</h1>\n" + ("    <p>filler</p>\n" * 100) + "</body></html>"
    _token_endpoint_answering(mocker, httpx.Response(403, text=page, request=request))

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        await get_access_token(username="u", password="p")

    first_line = str(excinfo.value).splitlines()[0]
    assert "<html> <body> <h1>Access denied</h1>" in first_line
    assert first_line.endswith("…")
    assert len(first_line) < 400


@pytest.mark.asyncio
async def test_retrieve_bulk_telemetry_single_page(mocker):
    """Bulk endpoint returns single page of messages."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceRef": "D1", "recordedAt": "2024-01-15T10:00:00.000Z", "gps": {"lat": -1.5, "lon": 30.2}},
        ],
        "pageInfo": {"hasNextPage": False},
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_bulk_telemetry(
        access_token="token",
        from_datetime="2024-01-15T00:00:00.000Z",
        to_datetime="2024-01-15T12:00:00.000Z",
        page_size=100,
    )

    assert len(result) == 1
    assert result[0]["deviceRef"] == "D1"
    assert result[0]["gps"]["lat"] == -1.5
    call_args = mock_post.call_args
    assert call_args[1]["json"]["fromDatetime"] == "2024-01-15T00:00:00.000Z"
    assert call_args[1]["json"]["pagination"]["first"] == 100
    assert call_args[1]["headers"]["Authorization"] == "Bearer token"


@pytest.mark.asyncio
async def test_retrieve_bulk_telemetry_paginated(mocker):
    """Bulk endpoint paginates until hasNextPage is false."""
    mock_post = AsyncMock()
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    def side_effect(*args, **kwargs):
        body = kwargs.get("json", {})
        after = body.get("pagination", {}).get("after")
        if after is None:
            return MagicMock(
                status_code=200,
                json=lambda: {
                    "contents": [{"deviceRef": "A", "recordedAt": "2024-01-15T10:00:00.000Z", "gps": {"lat": 0, "lon": 0}}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                },
                raise_for_status=MagicMock(),
            )
        return MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [{"deviceRef": "B", "recordedAt": "2024-01-15T11:00:00.000Z", "gps": {"lat": 1, "lon": 1}}],
                "pageInfo": {"hasNextPage": False},
            },
            raise_for_status=MagicMock(),
        )

    mock_post.side_effect = side_effect

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_bulk_telemetry(
        access_token="t",
        from_datetime="2024-01-15T00:00:00.000Z",
        to_datetime="2024-01-15T12:00:00.000Z",
        page_size=1,
    )

    assert len(result) == 2
    assert result[0]["deviceRef"] == "A"
    assert result[1]["deviceRef"] == "B"
    assert mock_post.call_count == 2


@pytest.mark.asyncio
async def test_retrieve_realtime_telemetry_returns_messages_and_checkpoint(mocker):
    """Realtime endpoint returns contents and new checkpoint."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceRef": "D1", "msgTs": 1705312800000, "gpsLocLat": -1.0, "gpsLocLon": 30.0},
        ],
        "checkpoint": 1727798490000,
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    messages, new_checkpoint = await retrieve_realtime_telemetry(
        access_token="token",
        checkpoint=0,
    )

    assert len(messages) == 1
    assert messages[0]["deviceRef"] == "D1"
    assert new_checkpoint == 1727798490000
    call_args = mock_post.call_args
    assert call_args[1]["json"]["fromCheckpoint"] == 0
    assert call_args[1]["json"]["retrieveGpsLoc"] is True
    assert "retrieve-realtime" in call_args[0][0]


@pytest.mark.asyncio
async def test_retrieve_realtime_telemetry_invalid_checkpoint_retries_with_zero(mocker):
    """When API returns 400 INVALID_CHECKPOINT, retry once with fromCheckpoint=0 and return result."""
    resp_400 = MagicMock()
    resp_400.status_code = 400
    resp_400.request = MagicMock()
    resp_400.json.return_value = {"code": "INVALID_CHECKPOINT", "msg": "Invalid checkpoint ..."}
    resp_400.raise_for_status = MagicMock()

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {"contents": [], "checkpoint": 12345}
    resp_200.raise_for_status = MagicMock()

    mock_post = AsyncMock(side_effect=[resp_400, resp_200])
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")
    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    messages, new_checkpoint = await retrieve_realtime_telemetry(
        access_token="token",
        checkpoint=999999,
    )

    assert messages == []
    assert new_checkpoint == 12345
    assert mock_post.call_count == 2
    assert mock_post.call_args_list[0][1]["json"]["fromCheckpoint"] == 999999
    assert mock_post.call_args_list[1][1]["json"]["fromCheckpoint"] == 0


@pytest.mark.asyncio
async def test_retrieve_realtime_telemetry_400_other_code_raises(mocker):
    """400 with code other than INVALID_CHECKPOINT does not retry; raise_for_status is called."""
    resp_400 = MagicMock()
    resp_400.status_code = 400
    resp_400.request = MagicMock()
    resp_400.json.return_value = {"code": "OTHER", "msg": "Some error"}
    resp_400.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError("Bad Request", request=resp_400.request, response=resp_400))

    mock_post = AsyncMock(return_value=resp_400)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")
    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        await retrieve_realtime_telemetry(access_token="token", checkpoint=999999)

    assert mock_post.call_count == 1
    assert mock_post.call_args[1]["json"]["fromCheckpoint"] == 999999


@pytest.mark.asyncio
async def test_retrieve_device_list_returns_contents(mocker):
    """Device list endpoint returns contents list."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "contents": [
            {"deviceUid": 67899, "deviceRef": "7896", "customerName": "WILDLIFE COMPUTER"},
        ],
    }
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mocker.patch("app.services.kineis_client.settings.KINEIS_API_BASE_URL", "https://api.example.com")

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("app.services.kineis_client.httpx.AsyncClient", return_value=mock_client)

    result = await retrieve_device_list(access_token="token")

    assert len(result) == 1
    assert result[0]["deviceUid"] == 67899
    assert result[0]["customerName"] == "WILDLIFE COMPUTER"
    call_args = mock_post.call_args
    assert "retrieve-device-list" in call_args[0][0]
    assert call_args[1]["headers"]["Authorization"] == "Bearer token"
