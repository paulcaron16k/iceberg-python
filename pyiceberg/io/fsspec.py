# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""FileIO implementation for reading and writing table files that uses fsspec compatible filesystems."""

import abc
import asyncio
import atexit
import contextlib
import errno
import functools
import hashlib
import inspect
import json
import logging
import os
import threading
import weakref
from collections.abc import Callable
from copy import copy
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
)
from urllib.parse import ParseResult, urlparse

import requests
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from requests import HTTPError
from typing_extensions import override

from pyiceberg.catalog import TOKEN, URI
from pyiceberg.catalog.rest.auth import AUTH_MANAGER
from pyiceberg.exceptions import SignError
from pyiceberg.io import (
    ADLS_ACCOUNT_HOST,
    ADLS_ACCOUNT_KEY,
    ADLS_ACCOUNT_NAME,
    ADLS_ANON,
    ADLS_CLIENT_ID,
    ADLS_CLIENT_SECRET,
    ADLS_CONNECTION_STRING,
    ADLS_CREDENTIAL,
    ADLS_SAS_TOKEN,
    ADLS_TENANT_ID,
    ADLS_TOKEN,
    AWS_ACCESS_KEY_ID,
    AWS_PROFILE_NAME,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    GCS_ACCESS,
    GCS_CACHE_TIMEOUT,
    GCS_CONSISTENCY,
    GCS_DEFAULT_LOCATION,
    GCS_PROJECT_ID,
    GCS_REQUESTER_PAYS,
    GCS_SERVICE_HOST,
    GCS_SESSION_KWARGS,
    GCS_TOKEN,
    GCS_VERSION_AWARE,
    HF_ENDPOINT,
    HF_TOKEN,
    S3_ACCESS_KEY_ID,
    S3_ANONYMOUS,
    S3_CONNECT_TIMEOUT,
    S3_ENDPOINT,
    S3_FORCE_VIRTUAL_ADDRESSING,
    S3_PROFILE_NAME,
    S3_PROXY_URI,
    S3_REGION,
    S3_REQUEST_TIMEOUT,
    S3_SECRET_ACCESS_KEY,
    S3_SERVER_SIDE_ENCRYPTION,
    S3_SESSION_TOKEN,
    S3_SIGNER,
    S3_SIGNER_ENDPOINT,
    S3_SIGNER_ENDPOINT_DEFAULT,
    S3_SIGNER_URI,
    S3_SSE_KMS_KEY_ID,
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
    _is_local_path,
)
from pyiceberg.typedef import Properties
from pyiceberg.types import strtobool
from pyiceberg.utils.properties import get_first_property_value, get_header_properties, property_as_bool

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from botocore.awsrequest import AWSRequest


class S3RequestSigner(abc.ABC):
    """Abstract base class for S3 request signers."""

    properties: Properties

    def __init__(self, properties: Properties) -> None:
        self.properties = properties

    @abc.abstractmethod
    def __call__(self, request: "AWSRequest", **_: Any) -> None:
        pass


class S3V4RestSigner(S3RequestSigner):
    """An S3 request signer that uses an external REST signing service to sign requests."""

    _session: requests.Session

    def __init__(self, properties: Properties) -> None:
        super().__init__(properties)
        self._session = requests.Session()

    def __call__(self, request: "AWSRequest", operation_name: str | None = None, **_: Any) -> None:
        signer_url = self.properties.get(S3_SIGNER_URI, self.properties[URI]).rstrip("/")  # type: ignore
        signer_endpoint = self.properties.get(S3_SIGNER_ENDPOINT, S3_SIGNER_ENDPOINT_DEFAULT)

        signer_headers: dict[str, str] = {}

        auth_header: str | None = None
        if auth_manager := self.properties.get(AUTH_MANAGER):
            auth_header = auth_manager.auth_header()
        elif token := self.properties.get(TOKEN):
            auth_header = f"Bearer {token}"

        if auth_header:
            signer_headers["Authorization"] = auth_header

        signer_headers.update(get_header_properties(self.properties))

        signer_body = {
            "method": request.method,
            "region": request.context["client_region"],
            "uri": request.url,
            "headers": {key: [val] for key, val in request.headers.items()},
        }
        # The signing service needs the DeleteObjects body to authorize the keys being deleted
        if operation_name == "DeleteObjects":
            signer_body["body"] = request.body.decode("utf-8")

        response = self._session.post(f"{signer_url}/{signer_endpoint.strip()}", headers=signer_headers, json=signer_body)
        try:
            response.raise_for_status()
            response_json = response.json()
        except HTTPError as e:
            raise SignError(f"Failed to sign request {response.status_code}: {signer_body}") from e

        for key, value in response_json["headers"].items():
            request.headers.add_header(key, ", ".join(value))

        request.url = response_json["uri"]


SIGNERS: dict[str, type[S3RequestSigner]] = {"S3V4RestSigner": S3V4RestSigner}


def _signer_cache_key(properties: Properties) -> str:
    """Return a digest identifying the signer built from these properties, without retaining the secrets in them."""
    items = sorted((key, repr(value)) for key, value in properties.items())
    return hashlib.sha256(repr(items).encode()).hexdigest()


#: The signed filesystems some FileIO still holds, one per signer: tables signed through different endpoints get
#: separate clients, and identical ones share one. Weak, so a filesystem goes, with its client, its signer and
#: what the signer holds (an auth manager and its HTTP session), when the last table using it does. fsspec's
#: instance cache kept them all until exit, three open sockets for every catalog and table.
_SIGNED_FILESYSTEMS: weakref.WeakValueDictionary[tuple[int, str], AbstractFileSystem] = weakref.WeakValueDictionary()
_SIGNED_FILESYSTEMS_LOCK = threading.Lock()

#: How many signing calls may run at once, process-wide. `S3V4RestSigner` posts with no timeout, so a sign endpoint
#: that never answers holds a slot for good; ten of them stall all signing, as one stalled the event loop before
#: signing moved off it. Ten is also `requests`' default pool size, so no signer's session opens a connection only to
#: discard it.
_SIGN_THREADS = 10
#: Taken by each signing thread. Replaced in a forked child, where the parent's in-flight calls never release theirs.
_SIGN_SLOTS = threading.BoundedSemaphore(_SIGN_THREADS)
#: Taken on the loop before a signing thread is started, and given back when it finishes, so a batch of requests waits
#: on the loop rather than as one blocked thread each: fsspec fans a batch out up to `RLIMIT_NOFILE // 8` requests at
#: once, and 500 pending signs started 500 threads (found by review).
_SIGN_GATES: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = weakref.WeakKeyDictionary()


#: The one `requests.Session` every `S3V4RestSigner` of a signed filesystem posts through, keeping as many connections
#: per host as can sign at once. Each signer kept its own before, so each table held a connection to the sign endpoint.
#: Created on first use under `_SIGNED_FILESYSTEMS_LOCK`.
_SIGNING_SESSION: requests.Session | None = None

#: The object-store connectors every signed filesystem's clients share, per event loop and per connection settings.
#: Each entry is the task building the connector. Read and written only on the loop it belongs to, so unlocked.
_SHARED_CONNECTORS: dict[asyncio.AbstractEventLoop, dict[tuple[Any, ...], "asyncio.Task[Any]"]] = {}


def _reset_signing_after_fork() -> None:
    """Give a forked child its own signing slots, signing session and object-store connectors.

    The parent's connectors are abandoned, never closed. Collected in the child, aiohttp's `BaseConnector.__del__`
    closes their sockets, and on Linux that unregisters each from the epoll instance the child shares with the parent,
    whose next request on that connection then waits for its read timeout. Marked closed, they are collected with
    nothing to do.
    """
    global _SIGN_SLOTS, _SIGNING_SESSION
    _SIGN_SLOTS = threading.BoundedSemaphore(_SIGN_THREADS)
    _SIGN_GATES.clear()
    _SIGNING_SESSION = None
    for connector in _built_connectors(_SHARED_CONNECTORS):
        connector._closed = True
    _SHARED_CONNECTORS.clear()


def _built_connectors(connectors: dict[asyncio.AbstractEventLoop, dict[tuple[Any, ...], "asyncio.Task[Any]"]]) -> list[Any]:
    return [
        build.result()
        for builds in connectors.values()
        for build in builds.values()
        if build.done() and not build.cancelled() and build.exception() is None
    ]


@atexit.register
def _close_shared_connectors() -> None:
    """Close the shared connectors, which no client owns and so none closes; aiohttp warns about one left open.

    While their loop still runs (fsspec's loop thread is a daemon, alive while atexit handlers run) they are collected
    and closed on it, since only that thread touches its entries, and waited for briefly, so a stuck loop cannot hold
    the exit up.
    """
    for loop, builds in list(_SHARED_CONNECTORS.items()):
        if loop.is_running():

            async def close_all(loop: asyncio.AbstractEventLoop = loop, builds: dict[Any, Any] = builds) -> None:
                connectors = _built_connectors({loop: builds})
                builds.clear()
                for connector in connectors:
                    with contextlib.suppress(Exception):
                        await connector.close()

            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(close_all(), loop).result(timeout=1)
            continue
        connectors = _built_connectors({loop: builds})
        builds.clear()
        if not loop.is_closed():
            # `close` is a coroutine and there is no loop to run it on; `_close` does the work synchronously, as
            # s3fs's own `close_session` falls back to.
            for connector in connectors:
                with contextlib.suppress(Exception):
                    connector._close()


def _signing_session() -> requests.Session:
    """Return the session every signer posts through. Call under `_SIGNED_FILESYSTEMS_LOCK`.

    A signer adds its own `Authorization` header to each request, so the session holds no credential of its own. It
    keeps cookies a sign endpoint sets, as each signer's own session did, and replays them only to the domain that set
    them.
    """
    global _SIGNING_SESSION
    if _SIGNING_SESSION is None:
        from requests.adapters import HTTPAdapter

        session = requests.Session()
        adapter = HTTPAdapter(pool_maxsize=_SIGN_THREADS)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _SIGNING_SESSION = session
    return _SIGNING_SESSION


def _shared_pool_size() -> int:
    """How many object-store connections the shared pool keeps.

    As many as one client kept before (botocore's `MAX_POOL_CONNECTIONS`, which `_s3` never overrides), for the
    caller's own threads, plus one per worker of PyIceberg's executor, which reads manifests and writes data files in
    parallel. A request beyond the limit waits for a connection rather than failing.
    """
    from botocore.endpoint import MAX_POOL_CONNECTIONS

    from pyiceberg.utils.concurrent import ExecutorFactory

    # Typed as an `Executor`, built as a `ThreadPoolExecutor`.
    return MAX_POOL_CONNECTIONS + getattr(ExecutorFactory.get_or_create(), "_max_workers", 0)


#: What `_shared_pool_session`'s subclass overrides in, or reads from, aiobotocore's `AIOHTTPSession`. Private, and
#: aiobotocore floats with s3fs, so they are checked before the subclass is used.
_AIOHTTP_SESSION_METHODS = ("_get_session", "_create_connector")
_AIOHTTP_SESSION_ATTRIBUTES = (
    "_sessions",
    "_proxy_config",
    "_exit_stack",
    "_timeout",
    "_verify",
    "_cert_file",
    "_key_file",
    "_connector_args",
    "_max_pool_connections",
)


def _missing_internals() -> list[str]:
    """Return the aiobotocore names the shared pool needs that the installed version lacks."""
    from aiobotocore.config import AioConfig
    from aiobotocore.httpsession import AIOHTTPSession

    missing = [] if "http_session_cls" in inspect.signature(AioConfig.__init__).parameters else ["AioConfig(http_session_cls)"]
    missing += [name for name in _AIOHTTP_SESSION_METHODS if not hasattr(AIOHTTPSession, name)]
    probe = AIOHTTPSession()
    return missing + [name for name in _AIOHTTP_SESSION_ATTRIBUTES if not hasattr(probe, name)]


@functools.lru_cache
def _shared_pool_session() -> type | None:
    """Return an aiobotocore HTTP session class whose clients share one connection pool, or None if it cannot be built.

    A signing catalog vends each table its own sign endpoint, so each table has its own signer and its own client, and
    aiobotocore builds a connector per client (`AIOHTTPSession._get_session`): a connection pool per table, all to the
    same object store. This keeps each client's own `aiohttp.ClientSession`, which holds no sockets, but hands it a
    shared connector it does not own, so closing a client, as releasing its filesystem does, leaves the pool open for
    the others. Connectors are shared per event loop and per connection settings, so a client never borrows a
    connection made under different TLS, client-certificate, proxy or connector settings.

    None, with a warning, when the installed aiobotocore lacks a name the subclass relies on: each client then keeps
    its own pool, which costs sockets, where a subclass built on a renamed internal could fail every request.
    """
    import aiohttp
    from aiobotocore.httpsession import AIOHTTPSession

    if missing := _missing_internals():
        logger.warning("aiobotocore lacks %s, so each signed S3 filesystem keeps its own connection pool", ", ".join(missing))
        return None

    class SharedPoolAIOHTTPSession(AIOHTTPSession):
        async def _get_session(self, proxy_url: str | None) -> aiohttp.ClientSession:
            # Set by `__aenter__`, as aiobotocore's own `_get_session` assumes.
            sessions: dict[str | None, aiohttp.ClientSession] = self._sessions
            # Held across creation where aiobotocore has it (3.9.1 on), as its own `_get_session` holds it, so closing
            # the client waits for a session being created instead of leaving it unclosed.
            lock: contextlib.AbstractAsyncContextManager[Any] = getattr(self, "_sessions_lock", None) or contextlib.nullcontext()
            async with lock:
                if not (session := sessions.get(proxy_url)):
                    session = await self._exit_stack.enter_async_context(
                        aiohttp.ClientSession(
                            connector=await _shared_connector(self, proxy_url),
                            connector_owner=False,
                            timeout=self._timeout,
                            skip_auto_headers={"CONTENT-TYPE"},
                            auto_decompress=False,
                        )
                    )
                    sessions[proxy_url] = session
            return session

    return SharedPoolAIOHTTPSession


async def _shared_connector(session: Any, proxy_url: str | None) -> Any:
    """Return the connector shared under this session's settings on the running loop, building it once.

    The build is a task of its own, which concurrent first requests all await: shielded, so a cancelled request never
    cancels it for the others, and forgotten if it fails, so a later request tries again.
    """
    loop = asyncio.get_running_loop()
    builds = _SHARED_CONNECTORS.setdefault(loop, {})
    key = (
        session._verify,
        session._cert_file,
        session._key_file,
        proxy_url,
        repr(sorted(session._proxy_config.settings.items())),
        repr(sorted((session._connector_args or {}).items())),
    )
    if (build := builds.get(key)) is None:
        # The shared pool's limit replaces the client's own `max_pool_connections`.
        session._max_pool_connections = _shared_pool_size()
        build = builds[key] = loop.create_task(_build_connector(session, proxy_url))
        build.add_done_callback(functools.partial(_forget_failed_build, builds, key))
    return await asyncio.shield(build)


async def _build_connector(session: Any, proxy_url: str | None) -> Any:
    # A coroutine from aiobotocore 3.8, which builds TLS contexts on a thread; a plain method before, back to 2.19,
    # where awaiting it unconditionally failed every signed request (found by review).
    connector = session._create_connector(proxy_url)
    return await connector if inspect.isawaitable(connector) else connector


def _forget_failed_build(
    builds: dict[tuple[Any, ...], "asyncio.Task[Any]"], key: tuple[Any, ...], build: "asyncio.Task[Any]"
) -> None:
    # `exception()` also marks a failure as retrieved: every waiter is handed it.
    if (build.cancelled() or build.exception() is not None) and builds.get(key) is build:
        del builds[key]


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_signing_after_fork)


class _SignOffTheLoop:
    """A `before-sign` handler that runs a synchronous signer on a thread of its own.

    aiobotocore runs a synchronous handler inline on the event loop and awaits only an awaitable result
    (`AioHierarchicalEmitter._emit`). s3fs drives every filesystem from fsspec's one loop thread, so a signer that
    waits on the network, as `S3V4RestSigner` does, stalled all object-store I/O in the process for each signature.
    This returns a coroutine instead, so the loop serves other requests while the signer waits. An exception the
    signer raises is re-raised to the request, which is then not sent.

    A daemon thread per call rather than a `ThreadPoolExecutor`: an executor's workers are joined at exit
    (`concurrent.futures.thread._python_exit`), so a sign endpoint that never answered would keep a process from
    exiting. fsspec's loop thread, where signing ran before, is a daemon too.
    """

    def __init__(self, signer: Callable[..., None]) -> None:
        self.signer = signer

    async def __call__(self, **kwargs: Any) -> None:
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        slots = _SIGN_SLOTS
        if (gate := _SIGN_GATES.get(loop)) is None:
            gate = _SIGN_GATES[loop] = asyncio.Semaphore(_SIGN_THREADS)
        await gate.acquire()

        def sign() -> None:
            outcome: BaseException | None = None
            try:
                with slots:
                    self.signer(**kwargs)
            except Exception as exc:
                outcome = exc
            except BaseException as exc:
                # Re-raised on fsspec's loop, an interrupt would stop the loop every filesystem shares; dropped, the
                # request would wait forever. So it fails just this request.
                outcome = RuntimeError(f"S3 request signer raised {exc!r}")
                outcome.__cause__ = exc
            if isinstance(outcome, StopIteration):  # A future refuses one as its exception.
                outcome = RuntimeError(f"S3 request signer raised {outcome!r}")
            with contextlib.suppress(RuntimeError):  # The loop has closed, so nobody is waiting.
                loop.call_soon_threadsafe(_settle_signing, done, outcome, gate)

        try:
            threading.Thread(target=sign, name="s3-signer", daemon=True).start()
        except BaseException:
            gate.release()
            raise
        await done


def _settle_signing(done: "asyncio.Future[None]", outcome: BaseException | None, gate: asyncio.Semaphore) -> None:
    gate.release()  # Here rather than when the request ends: a cancelled request's signer is still running.
    if done.done():  # The request was cancelled while its signer ran.
        return
    if outcome is None:
        done.set_result(None)
    else:
        done.set_exception(outcome)


@lru_cache
def _signed_s3_filesystem() -> type[AbstractFileSystem]:
    from s3fs import S3FileSystem

    class SignedS3FileSystem(S3FileSystem):
        """An S3FileSystem whose clients are created from a session carrying a request signer.

        Kept out of fsspec's instance cache, which holds every filesystem for the life of the process;
        `_SIGNED_FILESYSTEMS` shares them instead.
        """

        cachable = False  # codespell:ignore cachable

    return SignedS3FileSystem


def _file(_: Properties) -> LocalFileSystem:
    return LocalFileSystem(auto_mkdir=True)


def _s3(properties: Properties) -> AbstractFileSystem:
    from s3fs import S3FileSystem

    client_kwargs = {
        "endpoint_url": properties.get(S3_ENDPOINT),
        "aws_access_key_id": get_first_property_value(properties, S3_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
        "aws_secret_access_key": get_first_property_value(properties, S3_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
        "aws_session_token": get_first_property_value(properties, S3_SESSION_TOKEN, AWS_SESSION_TOKEN),
        "region_name": get_first_property_value(properties, S3_REGION, AWS_REGION),
    }
    config_kwargs = {}
    s3_additional_kwargs = {}
    register_events: dict[str, Callable[..., Any]] = {}

    if signer := properties.get(S3_SIGNER):
        logger.info("Loading signer %s", signer)
        if signer_cls := SIGNERS.get(signer):
            signer = signer_cls(properties)
            register_events["before-sign.s3"] = _SignOffTheLoop(signer)

            # Disable the AWS Signer
            from botocore import UNSIGNED

            config_kwargs["signature_version"] = UNSIGNED
        else:
            raise ValueError(f"Signer not available: {signer}")

    if proxy_uri := properties.get(S3_PROXY_URI):
        config_kwargs["proxies"] = {"http": proxy_uri, "https": proxy_uri}

    if connect_timeout := properties.get(S3_CONNECT_TIMEOUT):
        config_kwargs["connect_timeout"] = float(connect_timeout)

    if request_timeout := properties.get(S3_REQUEST_TIMEOUT):
        config_kwargs["read_timeout"] = float(request_timeout)

    if property_as_bool(properties, S3_FORCE_VIRTUAL_ADDRESSING, False):
        config_kwargs["s3"] = {"addressing_style": "virtual"}

    if s3_anonymous := properties.get(S3_ANONYMOUS):
        anon = strtobool(s3_anonymous)
    else:
        anon = False

    if server_side_encryption := properties.get(S3_SERVER_SIDE_ENCRYPTION):
        s3_additional_kwargs["ServerSideEncryption"] = server_side_encryption

    if sse_kms_key_id := properties.get(S3_SSE_KMS_KEY_ID):
        s3_additional_kwargs["SSEKMSKeyId"] = sse_kms_key_id

    s3_fs_kwargs = {
        "anon": anon,
        "client_kwargs": client_kwargs,
        "config_kwargs": config_kwargs,
    }

    if profile_name := get_first_property_value(properties, S3_PROFILE_NAME, AWS_PROFILE_NAME):
        s3_fs_kwargs["profile"] = profile_name

    if s3_additional_kwargs:
        s3_fs_kwargs["s3_additional_kwargs"] = s3_additional_kwargs

    if not register_events:
        return S3FileSystem(**s3_fs_kwargs)

    # The signer goes on the session rather than on a client: s3fs creates every client, including
    # the ones it recreates on refresh, from the session it is given, so none is ever without the
    # signer. Replacing the signer on a shared client instead leaves a window in which requests from
    # other threads go out unsigned.
    from aiobotocore.session import AioSession

    # The pid, as fsspec keys its own cache: a forked child must not reuse a client bound to its parent's
    # event-loop thread. The digest covers an auth manager by its repr, which carries its address; that
    # cannot be reused while the filesystem lives, because the filesystem's signer holds it.
    key = (os.getpid(), _signer_cache_key(properties))
    with _SIGNED_FILESYSTEMS_LOCK:
        if (fs := _SIGNED_FILESYSTEMS.get(key)) is not None:
            return fs
        if (shared_pool := _shared_pool_session()) is not None:
            s3_fs_kwargs["config_kwargs"] = {**config_kwargs, "http_session_cls": shared_pool}
        if isinstance(signer, S3V4RestSigner):
            signer._session = _signing_session()
        session = AioSession()
        for event_name, event_function in register_events.items():
            session.get_component("event_emitter").register_last(event_name, event_function, unique_id=1925)
        fs = _signed_s3_filesystem()(**s3_fs_kwargs, session=session)
        _SIGNED_FILESYSTEMS[key] = fs
        return fs


def _gs(properties: Properties) -> AbstractFileSystem:
    # https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem
    from gcsfs import GCSFileSystem

    return GCSFileSystem(
        project=properties.get(GCS_PROJECT_ID),
        access=properties.get(GCS_ACCESS, "full_control"),
        token=properties.get(GCS_TOKEN),
        consistency=properties.get(GCS_CONSISTENCY, "none"),
        cache_timeout=properties.get(GCS_CACHE_TIMEOUT),
        requester_pays=property_as_bool(properties, GCS_REQUESTER_PAYS, False),
        session_kwargs=json.loads(properties.get(GCS_SESSION_KWARGS, "{}")),
        endpoint_url=properties.get(GCS_SERVICE_HOST),
        default_location=properties.get(GCS_DEFAULT_LOCATION),
        version_aware=property_as_bool(properties, GCS_VERSION_AWARE, False),
    )


def _adls(properties: Properties, hostname: str | None = None) -> AbstractFileSystem:
    # https://fsspec.github.io/adlfs/api/

    from adlfs import AzureBlobFileSystem
    from azure.core.credentials import AccessToken
    from azure.core.credentials_async import AsyncTokenCredential

    account_name = properties.get(ADLS_ACCOUNT_NAME)
    sas_token = properties.get(ADLS_SAS_TOKEN)

    for key, per_account_sas_token in {
        key.replace(f"{ADLS_SAS_TOKEN}.", ""): value for key, value in properties.items() if key.startswith(f"{ADLS_SAS_TOKEN}.")
    }.items():
        if account_name is None:
            account_name = key.split(".")[0]
        if sas_token is None:
            sas_token = per_account_sas_token

    # Fallback: extract account_name from URI hostname (e.g. "account.dfs.core.windows.net" -> "account")
    if hostname and account_name is None:
        account_name = hostname.split(".")[0]

    class StaticTokenCredential(AsyncTokenCredential):
        _DEFAULT_EXPIRY_SECONDS = 3600

        def __init__(self, token_string: str) -> None:
            self._token = token_string

        async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
            import time

            # Set expiration 1 hour from now
            expires_on = int(time.time()) + self._DEFAULT_EXPIRY_SECONDS
            return AccessToken(self._token, expires_on)

    if token := properties.get(ADLS_TOKEN):
        credential = StaticTokenCredential(token)
    else:
        credential = properties.get(ADLS_CREDENTIAL)  # type: ignore

    return AzureBlobFileSystem(
        connection_string=properties.get(ADLS_CONNECTION_STRING),
        credential=credential,
        account_name=account_name,
        account_key=properties.get(ADLS_ACCOUNT_KEY),
        sas_token=sas_token,
        tenant_id=properties.get(ADLS_TENANT_ID),
        client_id=properties.get(ADLS_CLIENT_ID),
        client_secret=properties.get(ADLS_CLIENT_SECRET),
        account_host=properties.get(ADLS_ACCOUNT_HOST),
        anon=properties.get(ADLS_ANON),
    )


def _hf(properties: Properties) -> AbstractFileSystem:
    from huggingface_hub import HfFileSystem

    return HfFileSystem(
        endpoint=properties.get(HF_ENDPOINT),
        token=properties.get(HF_TOKEN),
    )


SCHEME_TO_FS: dict[str, Callable[..., AbstractFileSystem]] = {
    "": _file,
    "file": _file,
    "s3": _s3,
    "s3a": _s3,
    "s3n": _s3,
    "abfs": _adls,
    "abfss": _adls,
    "gs": _gs,
    "gcs": _gs,
    "hf": _hf,
}

_ADLS_SCHEMES = frozenset({"abfs", "abfss", "wasb", "wasbs"})


class FsspecInputFile(InputFile):
    """An input file implementation for the FsspecFileIO.

    Args:
        location (str): A URI to a file location.
        fs (AbstractFileSystem): An fsspec filesystem instance.
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    @override
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        object_info = self._fs.info(self.location)
        if "Size" in object_info:
            return object_info["Size"]
        elif "size" in object_info:
            return object_info["size"]
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    @override
    def exists(self) -> bool:
        """Check whether the location exists."""
        return self._fs.lexists(self.location)

    @override
    def open(self, seekable: bool = True) -> InputStream:
        """Create an input stream for reading the contents of the file.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            OpenFile: An fsspec compliant file-like object.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        try:
            return self._fs.open(self.location, "rb")
        except FileNotFoundError as e:
            # To have a consistent error handling experience, make sure exception contains missing file location.
            raise e if e.filename else FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.location) from e


class FsspecOutputFile(OutputFile):
    """An output file implementation for the FsspecFileIO.

    Args:
        location (str): A URI to a file location.
        fs (AbstractFileSystem): An fsspec filesystem instance.
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    @override
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        object_info = self._fs.info(self.location)
        if "Size" in object_info:
            return object_info["Size"]
        elif "size" in object_info:
            return object_info["size"]
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    @override
    def exists(self) -> bool:
        """Check whether the location exists."""
        return self._fs.lexists(self.location)

    @override
    def create(self, overwrite: bool = False) -> OutputStream:
        """Create an output stream for reading the contents of the file.

        Args:
            overwrite (bool): Whether to overwrite the file if it already exists.

        Returns:
            OpenFile: An fsspec compliant file-like object.

        Raises:
            FileExistsError: If the file already exists at the location and overwrite is set to False.

        Note:
            If overwrite is set to False, a check is first performed to verify that the file does not exist.
            This is not thread-safe and a possibility does exist that the file can be created by a concurrent
            process after the existence check yet before the output stream is created. In such a case, the default
            behavior will truncate the contents of the existing file when opening the output stream.
        """
        if not overwrite and self.exists():
            raise FileExistsError(f"Cannot create file, file already exists: {self.location}")
        return self._fs.open(self.location, "wb")

    @override
    def to_input_file(self) -> FsspecInputFile:
        """Return a new FsspecInputFile for the location at `self.location`."""
        return FsspecInputFile(location=self.location, fs=self._fs)


class FsspecFileIO(FileIO):
    """A FileIO implementation that uses fsspec."""

    def __init__(self, properties: Properties):
        self._scheme_to_fs: dict[str, Callable[..., AbstractFileSystem]] = dict(SCHEME_TO_FS)
        self._thread_locals = threading.local()
        super().__init__(properties=properties)

    @override
    def new_input(self, location: str) -> FsspecInputFile:
        """Get an FsspecInputFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            FsspecInputFile: An FsspecInputFile instance for the given location.
        """
        uri = urlparse(location)
        fs = self._get_fs_from_uri(uri, location)
        return FsspecInputFile(location=location, fs=fs)

    @override
    def new_output(self, location: str) -> FsspecOutputFile:
        """Get an FsspecOutputFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            FsspecOutputFile: An FsspecOutputFile instance for the given location.
        """
        uri = urlparse(location)
        fs = self._get_fs_from_uri(uri, location)
        return FsspecOutputFile(location=location, fs=fs)

    @override
    def delete(self, location: str | InputFile | OutputFile) -> None:
        """Delete the file at the given location.

        Args:
            location (Union[str, InputFile, OutputFile]): The URI to the file--if an InputFile instance or an
                OutputFile instance is provided, the location attribute for that instance is used as the location
                to delete.
        """
        if isinstance(location, (InputFile, OutputFile)):
            str_location = location.location  # Use InputFile or OutputFile location
        else:
            str_location = location

        uri = urlparse(str_location)
        fs = self._get_fs_from_uri(uri, str_location)
        fs.rm(str_location)

    def _get_fs_from_uri(self, uri: "ParseResult", location: str = "") -> AbstractFileSystem:
        """Get a filesystem from a parsed URI, using hostname for ADLS account resolution."""
        if _is_local_path(location):
            return self.get_fs("file")
        if uri.scheme in _ADLS_SCHEMES:
            return self.get_fs(uri.scheme, uri.hostname)
        return self.get_fs(uri.scheme)

    def get_fs(self, scheme: str, hostname: str | None = None) -> AbstractFileSystem:
        """Get a filesystem for a specific scheme, cached per thread."""
        if not hasattr(self._thread_locals, "get_fs_cached"):
            self._thread_locals.get_fs_cached = lru_cache(self._get_fs)

        return self._thread_locals.get_fs_cached(scheme, hostname)

    def _get_fs(self, scheme: str, hostname: str | None = None) -> AbstractFileSystem:
        """Get a filesystem for a specific scheme."""
        if scheme not in self._scheme_to_fs:
            raise ValueError(f"No registered filesystem for scheme: {scheme}")

        if scheme in _ADLS_SCHEMES:
            return _adls(self.properties, hostname)

        return self._scheme_to_fs[scheme](self.properties)

    def __getstate__(self) -> dict[str, Any]:
        """Create a dictionary of the FsSpecFileIO fields used when pickling."""
        fileio_copy = copy(self.__dict__)
        del fileio_copy["_thread_locals"]
        return fileio_copy

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Deserialize the state into a FsSpecFileIO instance."""
        self.__dict__ = state
        self._thread_locals = threading.local()
