"""
Kineis/CLS API client for bulk telemetry retrieval (CONNECTORS-836).

- Authentication: username/password → Bearer token via account.groupcls.com
- Bulk telemetry: POST /telemetry/api/v1/retrieve-bulk with pagination
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar

import httpx
import stamina
# app.settings before gundi_client_v2: the first .env loader wins per key (see
# app/settings/base.py).
from app import settings
from gundi_client_v2.token_cache import (
    NO_REFRESH,
    CachedToken,
    MemoryTokenCache,
    TokenStore,
    token_cache_from_url,
)

from app.services.retry_policies import is_retryable_failure

logger = logging.getLogger(__name__)

T = TypeVar("T")

TOKEN_KEY_PREFIX = "kineis:token:"
TOKEN_MIN_TTL_SECONDS = 60

# Retries only what may pass on the next attempt (transport failures, 429, 5xx).
# A 400/401/403 is a definite answer: retrying it, with the re-login each
# attempt implies, is what gets an account throttled by the CLS identity server.
PROVIDER_RETRY = dict(on=is_retryable_failure, wait_initial=10.0, wait_jitter=10.0, wait_max=300.0)

# Kineis tokens get their own memory layer: the Gundi client's process cache is
# keyed for its credentials, and clearing one must never drop the other.
_memory = MemoryTokenCache()


def _auth_path() -> str:
    return getattr(
        settings,
        "KINEIS_AUTH_PATH",
        "/auth/realms/cls/protocol/openid-connect/token",
    )


def _token_url(auth_base_url: Optional[str] = None) -> str:
    base = auth_base_url or settings.KINEIS_AUTH_BASE_URL
    return base.rstrip("/") + _auth_path()


async def _post_token_request(url: str, data: Dict[str, str]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        return response.json()


async def get_access_token(
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Obtain a Bearer token from the CLS/Kineis auth endpoint.
    Uses password grant: grant_type=password, client_id, username, password.
    Returns dict with access_token, expires_in (seconds), and optionally refresh_token.
    """
    return await _post_token_request(
        _token_url(auth_base_url),
        {
            "grant_type": "password",
            "client_id": client_id,
            "username": username,
            "password": password,
        },
    )


async def refresh_access_token(
    refresh_token: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Exchange a refresh token for a new access token (refresh_token grant)."""
    return await _post_token_request(
        _token_url(auth_base_url),
        {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": refresh_token,
        },
    )


def _token_cache_key(
    integration_id: str, username: str, client_id: str, auth_base_url: Optional[str] = None
) -> str:
    # The integration id is part of the key so two integrations naming the same
    # CLS username never share a token: the cache cannot check the password, and
    # sharing would hand one integration's token to another that typed the wrong
    # one. The password stays out of the key (see gundi_client_v2.token_cache_key).
    material = "\x1f".join([integration_id, _token_url(auth_base_url), client_id or "", username])
    return TOKEN_KEY_PREFIX + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _token_store() -> TokenStore:
    # Backed by the runner's Redis token-cache db when one is configured, so a
    # token outlives the instance that fetched it: scheduled runs land on
    # whichever instance Pub/Sub reaches, often a cold one.
    return TokenStore(token_cache_from_url(settings.GUNDI_TOKEN_CACHE_URL), memory=_memory)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_cached_token(result: Dict[str, Any], now: datetime) -> CachedToken:
    refresh_token = result.get("refresh_token") or ""
    refresh_expires_in = result.get("refresh_expires_in")
    if refresh_token and refresh_expires_in:
        refresh_expires_at = now + timedelta(seconds=int(refresh_expires_in))
    else:
        refresh_expires_at = NO_REFRESH
    return CachedToken(
        access_token=result["access_token"],
        refresh_token=refresh_token,
        token_type=result.get("token_type") or "Bearer",
        expires_at=now + timedelta(seconds=int(result.get("expires_in", 300))),
        refresh_expires_at=refresh_expires_at,
    )


def _usable(token: Optional[CachedToken], now: datetime, min_ttl_seconds: int) -> bool:
    return token is not None and token.expires_at - now >= timedelta(seconds=min_ttl_seconds)


async def get_cached_token(
    integration_id: str,
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    min_ttl_seconds: int = TOKEN_MIN_TTL_SECONDS,
) -> str:
    """
    Return a Bearer token with at least min_ttl_seconds left: the shared cached
    one, else a refreshed one, else a fresh password login. Concurrent callers in
    this process wait for a single fetch.
    """
    store = _token_store()
    key = _token_cache_key(integration_id, username, client_id, auth_base_url)
    token = await store.get(key)
    if _usable(token, _now(), min_ttl_seconds):
        return token.access_token

    async with store.lock(key):
        token = await store.reload(key)
        now = _now()
        if _usable(token, now, min_ttl_seconds):
            return token.access_token

        result = None
        if token is not None and token.refresh_is_live(now):
            try:
                result = await refresh_access_token(token.refresh_token, client_id, auth_base_url)
            except httpx.HTTPStatusError as e:
                if e.response.status_code not in (400, 401):
                    raise
                logger.info("Kineis refresh token rejected (%d); logging in again", e.response.status_code)
        if result is None:
            logger.info("Kineis password login for integration %s", integration_id)
            result = await get_access_token(username, password, client_id, auth_base_url)

        fresh = _to_cached_token(result, _now())
        await store.set(key, fresh)
        return fresh.access_token


async def _discard_token(key: str, rejected_access_token: str) -> None:
    """Drop the cached token, unless it has already been replaced by another caller."""
    store = _token_store()
    async with store.lock(key):
        current = await store.reload(key)
        if current is not None and current.access_token == rejected_access_token:
            await store.delete(key)


def clear_token_cache() -> None:
    """Forget every Kineis token held in this process (the Redis copies remain)."""
    _memory.clear()


async def _call_with_token(
    integration_id: str,
    username: str,
    password: str,
    client_id: str,
    auth_base_url: Optional[str],
    call: Callable[[str], Awaitable[T]],
) -> T:
    """Run ``call`` with a cached token, retrying transient failures.

    A 401 discards that token and re-authenticates, at most once per fetch: the
    budget lives outside the retry loop so a transient failure in between does
    not grant another login, and a 401 on a replacement token propagates.
    """
    key = _token_cache_key(integration_id, username, client_id, auth_base_url)
    reauthenticated = False
    async for attempt in stamina.retry_context(**PROVIDER_RETRY):
        with attempt:
            token = await get_cached_token(integration_id, username, password, client_id, auth_base_url)
            try:
                return await call(token)
            except httpx.HTTPStatusError as e:
                if e.response.status_code != 401 or reauthenticated:
                    raise
            reauthenticated = True
            logger.info("Kineis API rejected the cached token for integration %s; re-authenticating once", integration_id)
            await _discard_token(key, token)
            token = await get_cached_token(integration_id, username, password, client_id, auth_base_url)
            return await call(token)


def _format_datetime_utc(dt: "datetime") -> str:
    """Format datetime as YYYY-MM-DDTHH:mm:ss.SSSZ (UTC)."""
    from datetime import datetime, timezone

    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def retrieve_bulk_telemetry(
    access_token: str,
    from_datetime: str,
    to_datetime: str,
    page_size: int = 100,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve all telemetry messages in the time window via the bulk endpoint.
    Request GPS and Doppler so responses include gpsLocLat/Lon, gpsLocDatetime, etc.
    Paginates until hasNextPage is false. Returns a flat list of telemetry messages.
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-bulk"

    all_messages: List[Dict[str, Any]] = []
    after: Optional[str] = None
    page_num = 0

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        while True:
            body: Dict[str, Any] = {
                "fromDatetime": from_datetime,
                "toDatetime": to_datetime,
                "datetimeFormat": "DATETIME",
                "pagination": {"first": page_size},
                "retrieveMetadata": retrieve_metadata,
                "retrieveRawData": retrieve_raw_data,
                "retrieveGpsLoc": retrieve_gps_loc,
                "retrieveDoppler": retrieve_doppler,
            }
            if after is not None:
                body["pagination"]["after"] = after
            # API allows only one of deviceRefs or deviceUids (manual 1.3.1.2); prefer refs
            if device_refs:
                body["deviceRefs"] = device_refs
            elif device_uids:
                body["deviceUids"] = device_uids

            page_num += 1
            logger.info(
                "Kineis retrieve-bulk request (page %d): from=%s to=%s page_size=%d devices=%s",
                page_num, from_datetime, to_datetime, page_size,
                f"{len(device_refs)} refs" if device_refs else f"{len(device_uids)} uids" if device_uids else "all",
            )
            logger.debug("Kineis retrieve-bulk request body (page %d): %s", page_num, body)

            response = await client.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

            logger.info(
                "Kineis retrieve-bulk response (page %d): status=%d",
                page_num, response.status_code,
            )

            if response.status_code == 401:
                raise httpx.HTTPStatusError(
                    "Unauthorized",
                    request=response.request,
                    response=response,
                )

            response.raise_for_status()
            data = response.json()

            # Collect messages from this page (structure may be data.contents or data.edges/node)
            contents = data.get("contents") or data.get("data") or []
            if isinstance(contents, list):
                all_messages.extend(contents)
            else:
                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node") if isinstance(edge, dict) else edge
                    if node:
                        all_messages.append(node)

            page_info = data.get("pageInfo") or data.get("page_info") or {}
            has_next = page_info.get("hasNextPage", page_info.get("has_next_page", False))

            page_message_count = len(contents) if isinstance(contents, list) else len(data.get("edges", []))
            logger.info(
                "Kineis retrieve-bulk page %d: %d messages, hasNextPage=%s",
                page_num, page_message_count, has_next,
            )

            if not has_next:
                break
            after = page_info.get("endCursor") or page_info.get("end_cursor")
            if not after:
                break

    logger.info("Kineis retrieve-bulk complete: %d total messages across %d pages", len(all_messages), page_num)
    return all_messages


async def retrieve_realtime_telemetry(
    access_token: str,
    checkpoint: int = 0,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    api_base_url: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Retrieve realtime telemetry since the given checkpoint (pull interface).
    First call with checkpoint=0 returns messages from the last 6 hours.
    Returns (list of message dicts, new_checkpoint for next call).
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-realtime"
    body: Dict[str, Any] = {
        "fromCheckpoint": checkpoint,
        "retrieveMetadata": retrieve_metadata,
        "retrieveRawData": retrieve_raw_data,
        "retrieveGpsLoc": retrieve_gps_loc,
        "retrieveDoppler": retrieve_doppler,
        "datetimeFormat": "DATETIME",
    }
    if device_refs:
        body["deviceRefs"] = device_refs
    elif device_uids:
        body["deviceUids"] = device_uids

    logger.info(
        "Kineis retrieve-realtime request: checkpoint=%d devices=%s",
        checkpoint,
        f"{len(device_refs)} refs" if device_refs else f"{len(device_uids)} uids" if device_uids else "all",
    )
    logger.debug("Kineis retrieve-realtime request body: %s", body)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        response = await client.post(url, json=body, headers=headers)
        logger.info("Kineis retrieve-realtime response: status=%d", response.status_code)
        if response.status_code == 401:
            raise httpx.HTTPStatusError(
                "Unauthorized",
                request=response.request,
                response=response,
            )
        if response.status_code == 400:
            try:
                err_data = response.json()
                logger.warning("Kineis retrieve-realtime 400 error: %s", err_data)
                if err_data.get("code") == "INVALID_CHECKPOINT" and checkpoint != 0:
                    logger.warning("Kineis INVALID_CHECKPOINT (checkpoint=%d), retrying with checkpoint=0", checkpoint)
                    body_retry = {**body, "fromCheckpoint": 0}
                    response = await client.post(url, json=body_retry, headers=headers)
                    logger.info("Kineis retrieve-realtime retry response: status=%d", response.status_code)
            except (ValueError, TypeError):
                pass
        response.raise_for_status()
        data = response.json()

    contents = data.get("contents") or []
    new_checkpoint = data.get("checkpoint", checkpoint)
    message_count = len(contents) if isinstance(contents, list) else 0
    logger.info(
        "Kineis retrieve-realtime complete: %d messages, checkpoint %d -> %d",
        message_count, checkpoint, new_checkpoint,
    )
    return list(contents) if isinstance(contents, list) else [], new_checkpoint


async def retrieve_device_list(
    access_token: str,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve the list of accessible devices (device list).
    Returns list of device dicts with deviceUid, deviceRef, customerName, etc.
    """
    base = api_base_url or settings.KINEIS_API_BASE_URL
    url = base.rstrip("/") + "/telemetry/api/v1/retrieve-device-list"

    logger.info("Kineis retrieve-device-list request: POST %s", url)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            url,
            json={},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        logger.info("Kineis retrieve-device-list response: status=%d", response.status_code)
        if response.status_code == 401:
            raise httpx.HTTPStatusError(
                "Unauthorized",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        data = response.json()

    contents = data.get("contents") or []
    device_list = list(contents) if isinstance(contents, list) else []
    logger.info("Kineis retrieve-device-list complete: %d devices", len(device_list))
    return device_list


async def fetch_device_list(
    integration_id: str,
    username: str,
    password: str,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get a Bearer token (cached) and fetch the device list."""
    return await _call_with_token(
        integration_id, username, password, client_id, auth_base_url,
        lambda token: retrieve_device_list(access_token=token, api_base_url=api_base_url),
    )


async def fetch_telemetry(
    integration_id: str,
    username: str,
    password: str,
    from_datetime: str,
    to_datetime: str,
    page_size: int = 100,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get a Bearer token (cached) and fetch all bulk telemetry in the time window.
    Requests GPS and Doppler by default so responses include location fields.
    """
    return await _call_with_token(
        integration_id, username, password, client_id, auth_base_url,
        lambda token: retrieve_bulk_telemetry(
            access_token=token,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            page_size=page_size,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=retrieve_metadata,
            retrieve_raw_data=retrieve_raw_data,
            retrieve_gps_loc=retrieve_gps_loc,
            retrieve_doppler=retrieve_doppler,
            api_base_url=api_base_url,
        ),
    )


async def fetch_telemetry_realtime(
    integration_id: str,
    username: str,
    password: str,
    checkpoint: int = 0,
    device_refs: Optional[List[str]] = None,
    device_uids: Optional[List[int]] = None,
    retrieve_metadata: bool = True,
    retrieve_raw_data: bool = True,
    retrieve_gps_loc: bool = True,
    retrieve_doppler: bool = True,
    client_id: str = "api-telemetry",
    auth_base_url: Optional[str] = None,
    api_base_url: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Get a Bearer token (cached) and fetch realtime telemetry since checkpoint."""
    return await _call_with_token(
        integration_id, username, password, client_id, auth_base_url,
        lambda token: retrieve_realtime_telemetry(
            access_token=token,
            checkpoint=checkpoint,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=retrieve_metadata,
            retrieve_raw_data=retrieve_raw_data,
            retrieve_gps_loc=retrieve_gps_loc,
            retrieve_doppler=retrieve_doppler,
            api_base_url=api_base_url,
        ),
    )
