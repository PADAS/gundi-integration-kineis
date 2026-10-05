"""Kineis token reuse: one CLS login per token lifetime, shared across instances.

Each test builds "instances" as separate in-memory layers over one shared fake
backend (standing in for the runner's Redis token-cache db), and counts the
requests that reach the CLS token endpoint.
"""
import asyncio
from unittest.mock import MagicMock

import httpx
import pytest
import stamina
from gundi_client_v2.token_cache import MemoryTokenCache, TokenStore

import app.services.kineis_client as kineis_client


class FakeBackend:
    def __init__(self):
        self.entries = {}

    async def get(self, key):
        return self.entries.get(key)

    async def set(self, key, token):
        self.entries[key] = token

    async def delete(self, key):
        self.entries.pop(key, None)


class BrokenBackend:
    async def get(self, key, *args):
        raise ConnectionError("redis down")

    set = delete = get


def _status_error(status_code):
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=MagicMock(), response=MagicMock(status_code=status_code)
    )


class TokenEndpoint:
    """Stands in for _post_token_request; issues numbered tokens and records grants."""

    def __init__(self, expires_in=300, refresh_expires_in=1800, fail_with=None, refresh_fail_with=None):
        self.grants = []
        self.expires_in = expires_in
        self.refresh_expires_in = refresh_expires_in
        self.fail_with = fail_with
        self.refresh_fail_with = refresh_fail_with

    async def __call__(self, url, data):
        await asyncio.sleep(0)  # let concurrent callers interleave
        self.grants.append(data["grant_type"])
        if data["grant_type"] == "refresh_token" and self.refresh_fail_with:
            raise _status_error(self.refresh_fail_with)
        if self.fail_with:
            raise _status_error(self.fail_with)
        n = len(self.grants)
        return {
            "access_token": f"access-{n}",
            "refresh_token": f"refresh-{n}",
            "expires_in": self.expires_in,
            "refresh_expires_in": self.refresh_expires_in,
            "token_type": "Bearer",
        }

    @property
    def password_logins(self):
        return self.grants.count("password")


@pytest.fixture
def backend():
    return FakeBackend()


@pytest.fixture
def use_instance(mocker, backend):
    """Switch which simulated instance the client runs on; returns its memory layer."""

    def _use(memory=None):
        memory = memory or MemoryTokenCache()
        mocker.patch.object(kineis_client, "_token_store", lambda: TokenStore(backend, memory=memory))
        return memory

    return _use


@pytest.fixture
def token_endpoint(mocker):
    endpoint = TokenEndpoint()
    mocker.patch.object(kineis_client, "_post_token_request", endpoint)
    return endpoint


@pytest.fixture
def no_retry_waits():
    # Three attempts with no sleeping: a retried call shows up as extra requests.
    stamina.set_testing(True, attempts=3)
    yield
    stamina.set_testing(False)


async def _token(integration_id="int-1", username="u"):
    return await kineis_client.get_cached_token(integration_id, username, "p")


@pytest.mark.asyncio
async def test_token_reused_within_instance(use_instance, token_endpoint):
    use_instance()

    assert await _token() == "access-1"
    assert await _token() == "access-1"
    assert token_endpoint.grants == ["password"]


@pytest.mark.asyncio
async def test_token_shared_across_instances(use_instance, token_endpoint, backend):
    use_instance()
    first = await _token()

    use_instance()  # a cold instance: empty memory, same backend
    second = await _token()

    assert first == second == "access-1"
    assert token_endpoint.grants == ["password"]
    assert len(backend.entries) == 1
    (key,) = backend.entries
    assert key.startswith(kineis_client.TOKEN_KEY_PREFIX)
    assert "u" not in key[len(kineis_client.TOKEN_KEY_PREFIX):]


@pytest.mark.asyncio
async def test_concurrent_callers_share_one_login(use_instance, token_endpoint):
    use_instance()

    tokens = await asyncio.gather(*(_token() for _ in range(5)))

    assert set(tokens) == {"access-1"}
    assert token_endpoint.grants == ["password"]


@pytest.mark.asyncio
async def test_expiring_token_is_refreshed_not_relogged(use_instance, token_endpoint):
    use_instance()
    token_endpoint.expires_in = kineis_client.TOKEN_MIN_TTL_SECONDS - 1  # inside the margin at once

    await _token()
    token_endpoint.expires_in = 300
    token = await _token()

    assert token == "access-2"
    assert token_endpoint.grants == ["password", "refresh_token"]


@pytest.mark.parametrize("status_code", [400, 401])
@pytest.mark.asyncio
async def test_rejected_refresh_falls_back_to_password(use_instance, token_endpoint, status_code):
    use_instance()
    token_endpoint.expires_in = 0
    await _token()

    token_endpoint.expires_in = 300
    token_endpoint.refresh_fail_with = status_code
    token = await _token()

    assert token == "access-3"
    assert token_endpoint.grants == ["password", "refresh_token", "password"]


@pytest.mark.asyncio
async def test_refresh_outage_propagates_without_password_login(use_instance, token_endpoint):
    use_instance()
    token_endpoint.expires_in = 0
    await _token()
    token_endpoint.refresh_fail_with = 503

    with pytest.raises(httpx.HTTPStatusError):
        await _token()
    assert token_endpoint.grants == ["password", "refresh_token"]


@pytest.mark.parametrize("refresh_response", [{"refresh_token": None}, {"refresh_expires_in": None}])
@pytest.mark.asyncio
async def test_no_usable_refresh_token_logs_in_again(use_instance, mocker, refresh_response):
    use_instance()
    grants = []

    async def endpoint(url, data):
        grants.append(data["grant_type"])
        return {"access_token": f"a{len(grants)}", "expires_in": 0, "refresh_token": "r",
                "refresh_expires_in": 1800, **refresh_response}

    mocker.patch.object(kineis_client, "_post_token_request", endpoint)
    await _token()
    await _token()

    assert grants == ["password", "password"]


@pytest.mark.asyncio
async def test_integrations_sharing_a_username_do_not_share_tokens(use_instance, token_endpoint):
    use_instance()

    a = await _token(integration_id="int-a")
    b = await _token(integration_id="int-b")

    assert a != b
    assert token_endpoint.password_logins == 2


@pytest.mark.asyncio
async def test_backend_outage_degrades_to_memory(mocker, token_endpoint):
    memory = MemoryTokenCache()
    mocker.patch.object(kineis_client, "_token_store", lambda: TokenStore(BrokenBackend(), memory=memory))

    assert await _token() == "access-1"
    assert await _token() == "access-1"
    assert token_endpoint.grants == ["password"]


@pytest.mark.asyncio
async def test_401_from_api_relogs_once_and_succeeds(use_instance, token_endpoint, backend, no_retry_waits, mocker):
    use_instance()
    calls = []

    async def device_list(access_token, api_base_url=None):
        calls.append(access_token)
        if access_token == "access-1":
            raise _status_error(401)
        return [{"deviceUid": 1}]

    mocker.patch.object(kineis_client, "retrieve_device_list", device_list)

    result = await kineis_client.fetch_device_list("int-1", "u", "p")

    assert result == [{"deviceUid": 1}]
    assert calls == ["access-1", "access-2"]
    assert token_endpoint.grants == ["password", "password"]
    (stored,) = backend.entries.values()
    assert stored.access_token == "access-2"


@pytest.mark.asyncio
async def test_repeated_401_from_api_stops_after_one_relogin(use_instance, token_endpoint, no_retry_waits, mocker):
    use_instance()
    retrieve = mocker.patch.object(
        kineis_client, "retrieve_device_list", side_effect=_status_error(401)
    )

    with pytest.raises(httpx.HTTPStatusError):
        await kineis_client.fetch_device_list("int-1", "u", "p")

    assert retrieve.call_count == 2
    assert token_endpoint.password_logins == 2


@pytest.mark.asyncio
async def test_401_does_not_discard_a_token_another_caller_replaced(use_instance, token_endpoint, backend):
    use_instance()
    rejected = await _token()
    key = next(iter(backend.entries))
    await kineis_client._discard_token(key, rejected)
    replacement = await _token()

    await kineis_client._discard_token(key, rejected)  # a slow caller reporting the old token

    assert await _token() == replacement
    assert token_endpoint.password_logins == 2


@pytest.mark.parametrize("auth_status", [400, 401, 403])
@pytest.mark.asyncio
async def test_rejected_credentials_are_not_retried(use_instance, mocker, no_retry_waits, auth_status):
    use_instance()
    endpoint = TokenEndpoint(fail_with=auth_status)
    mocker.patch.object(kineis_client, "_post_token_request", endpoint)
    retrieve = mocker.patch.object(kineis_client, "retrieve_bulk_telemetry")

    with pytest.raises(httpx.HTTPStatusError):
        await kineis_client.fetch_telemetry(
            "int-1", "u", "bad", from_datetime="2024-01-15T00:00:00.000Z", to_datetime="2024-01-15T12:00:00.000Z",
        )

    assert endpoint.grants == ["password"]
    retrieve.assert_not_called()


@pytest.mark.parametrize("status_code", [400, 403, 404])
@pytest.mark.asyncio
async def test_definite_api_errors_are_not_retried(use_instance, token_endpoint, no_retry_waits, mocker, status_code):
    use_instance()
    retrieve = mocker.patch.object(
        kineis_client, "retrieve_realtime_telemetry", side_effect=_status_error(status_code)
    )

    with pytest.raises(httpx.HTTPStatusError):
        await kineis_client.fetch_telemetry_realtime("int-1", "u", "p", checkpoint=5)

    assert retrieve.call_count == 1
    assert token_endpoint.grants == ["password"]


@pytest.mark.parametrize("error", [_status_error(429), _status_error(503), httpx.ConnectError("boom")])
@pytest.mark.asyncio
async def test_transient_api_errors_retry_with_the_same_token(use_instance, token_endpoint, no_retry_waits, mocker, error):
    use_instance()
    retrieve = mocker.patch.object(
        kineis_client, "retrieve_realtime_telemetry", side_effect=[error, ([{"m": 1}], 6)]
    )

    result = await kineis_client.fetch_telemetry_realtime("int-1", "u", "p", checkpoint=5)

    assert result == ([{"m": 1}], 6)
    assert [c.kwargs["access_token"] for c in retrieve.call_args_list] == ["access-1", "access-1"]
    assert token_endpoint.grants == ["password"]


@pytest.mark.asyncio
async def test_token_store_uses_runner_token_cache_url(mocker):
    sentinel = FakeBackend()
    from_url = mocker.patch.object(kineis_client, "token_cache_from_url", return_value=sentinel)
    mocker.patch.object(kineis_client.settings, "GUNDI_TOKEN_CACHE_URL", "redis://cache:6379/2")

    store = kineis_client._token_store()

    from_url.assert_called_once_with("redis://cache:6379/2")
    assert store._backend is sentinel
    assert store._memory is kineis_client._memory


FETCH_WRAPPERS = [
    ("fetch_device_list", "retrieve_device_list", {}),
    (
        "fetch_telemetry",
        "retrieve_bulk_telemetry",
        {"from_datetime": "2024-01-15T00:00:00.000Z", "to_datetime": "2024-01-15T12:00:00.000Z"},
    ),
    ("fetch_telemetry_realtime", "retrieve_realtime_telemetry", {"checkpoint": 5}),
]


@pytest.mark.parametrize("fetch_name,retrieve_name,kwargs", FETCH_WRAPPERS)
@pytest.mark.asyncio
async def test_transient_retry_does_not_reset_reauth_budget(
    use_instance, token_endpoint, no_retry_waits, mocker, fetch_name, retrieve_name, kwargs
):
    use_instance()
    stamina.set_testing(True, attempts=5)
    retrieve = mocker.patch.object(
        kineis_client,
        retrieve_name,
        side_effect=[_status_error(401), _status_error(503), _status_error(401), _status_error(401)],
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await getattr(kineis_client, fetch_name)("int-1", "u", "p", **kwargs)

    assert exc_info.value.response.status_code == 401
    assert retrieve.call_count == 3
    assert token_endpoint.password_logins == 2
