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

"""Concurrent writes to a remote-signing catalog go out unsigned and fail 403.

**Impact.** Table commits fail, intermittently, on a supported and unexceptional
configuration. Not a theoretical race: this was hit in production against Lakekeeper + MinIO,
and cost several days to track down because the failure surfaces as an opaque
`PermissionError` a long way from its cause. Any PyIceberg caller writing concurrently to a
catalog that vends remote signing is exposed, and PyIceberg's own writer is concurrent by
default.

**Where:** `pyiceberg/io/fsspec.py`, in `_s3()` — the `unregister`/`register_last` pair at the
end of the function (lines 237-238 as of `pyiceberg-0.12.0rc2`). The fix is one line in the
same place. A second, independent defect in the same file is noted at the end.

**Versions:** reproduced on `main` (`pyiceberg-0.12.0rc2-25-g885c0b87`), on the 0.12.0 release
and on 0.11.1.

## Symptom

Against a remote-signing catalog (Lakekeeper 0.13.1 + MinIO, path-style), roughly **3-4% of
appends** fail under PyIceberg's own concurrent write path. It surfaces as
`PermissionError: Access Denied` raised out of `s3fs`, several frames from its cause, with
nothing tying it to signing.

Every unsigned request caught on the wire was a **`PUT` of a manifest** (`*-m0.avro`) during
commit — 5 of 5 and 3 of 3 across two runs — so what fails is the commit, not a stray read.

The response code is the clue. It is always `AccessDenied` and never `SignatureDoesNotMatch`
— which is what an **unsigned** request produces, not a mis-signed one.

## Cause

`pyiceberg/io/fsspec.py::_s3` ends with:

```python
fs = S3FileSystem(**s3_fs_kwargs)
...
for event_name, event_function in register_events.items():
    fs.s3.meta.events.unregister(event_name, unique_id=1925)
    fs.s3.meta.events.register_last(event_name, event_function, unique_id=1925)
```

fsspec caches `S3FileSystem` instances, so it returns **one shared instance** for a given set
of kwargs — along with one botocore client and one event emitter, shared by every thread.

Between the `unregister` and the `register_last` no signer is installed. And `_s3()` has
already set `config_kwargs["signature_version"] = UNSIGNED`, so botocore will not sign
either. A request signed in that window leaves with **no `Authorization` header at all**.

`FsspecFileIO.get_fs` caches per thread, so every thread calls `_s3()` at least once, and
each new table means a new `FileIO` and another call. A short run makes over a hundred
unregister/register cycles, each racing whatever writes are in flight.

## Measurements

90 writes per run, against the live stack:

| configuration                                       | append failures | requests sent unsigned |
|-----------------------------------------------------|-----------------|------------------------|
| as shipped                                           | 2 / 4 / 5       | 2 / 4 / 5              |
| fsspec instance cache disabled (133 clients)         | 0               | 0                      |
| signer registered once, never unregistered (1 client)| 0               | 0                      |
| `PYICEBERG_MAX_WORKERS=1`                            | 0               | 0                      |

Requests-sent-unsigned equals append failures **exactly**, run after run — counted with a
`before-send.s3` hook that flags any request leaving without an `Authorization` header.

Note the third row: the failures stop while the client is **still shared**, which is what
distinguishes this from a general concurrency problem. Serialising the writer also stops
them, and so does giving each caller its own client — both consistent with the same window.

## Suggested fix

One line, in `pyiceberg/io/fsspec.py::_s3`: drop the `unregister`. Botocore's
`HierarchicalEmitter._register_section` returns early for a `unique_id` it already holds:

```python
if unique_id in self._unique_id_handlers:
    # We've already registered a handler using this unique_id
    # so we don't need to register it again.
    ...
    return
```

so re-registering an equivalent signer was already a no-op, and the `unregister` only opens
the window. Every signer `_s3()` builds comes from the same `properties`, so which instance
stays installed does not matter — only that one always is.

```python
for event_name, event_function in register_events.items():
    fs.s3.meta.events.register_last(event_name, event_function, unique_id=1925)
```

## A second defect in the same function, worth deciding separately

Also in `pyiceberg/io/fsspec.py::_s3`: the fsspec cache key comes from `s3_fs_kwargs`, which
does **not** include the signer's URI or endpoint. Two catalogs sharing an S3 endpoint and credentials but signing through
different services therefore share one filesystem. Today the last `_s3()` caller's signer
wins; with the fix above the first one does. Neither is correct. The complete answer is for
the signer configuration to participate in the filesystem's identity — but that changes
which objects are cached, so it is left as a separate call rather than folded in here.

## What these tests do

`test_s3_unregisters_the_signer_on_a_client_it_shares` is the one that reads the library:
it calls `_s3()` twice with a signer configured, shows fsspec handing back the **same**
filesystem, and catches `unregister(before-sign.s3, unique_id=1925)` on **both** calls — the
second while a signer is already installed. It needs no credentials and no network, and it
fails once the `unregister` is dropped.

The rest characterise the window and its consequence on a real `HierarchicalEmitter`, and
establish the botocore behaviour the suggested fix relies on. The race itself is timing
dependent and is deliberately not asserted directly: a test that loses a race on demand
would be flaky in CI and would localise nothing.
"""

from __future__ import annotations

import threading
from typing import Any

from botocore.hooks import HierarchicalEmitter

from pyiceberg.io.fsspec import S3V4RestSigner

SIGNER_EVENT = "before-sign.s3"
SIGNER_UNIQUE_ID = 1925
TEST_URI = "https://iceberg-test-signer"


def _emitter_with_signer(signer: Any) -> HierarchicalEmitter:
    """A real botocore emitter carrying a signer, as `_s3()` leaves it."""
    emitter = HierarchicalEmitter()
    emitter.register_last(SIGNER_EVENT, signer, unique_id=SIGNER_UNIQUE_ID)
    return emitter


def _installed_signer(emitter: HierarchicalEmitter) -> Any:
    """The signer botocore currently holds under `_s3`'s unique id, or None.

    `_unique_id_handlers` is the registry `_register_section` consults, so this is the same
    question botocore asks itself when deciding whether a registration is a duplicate.
    """
    entry = emitter._unique_id_handlers.get(SIGNER_UNIQUE_ID)
    return entry["handler"] if entry else None


# -- the defect -----------------------------------------------------------------------------


def test_s3_unregisters_the_signer_on_a_client_it_shares() -> None:
    """The defect, read straight off `_s3()`. No credentials, no network.

    Two calls, as two `FileIO`s would make: fsspec hands back the same filesystem — so the
    same botocore client and the same event emitter — and `_s3()` unregisters the signer on
    **both**, the second time while a perfectly good signer is already installed.

    Dropping the `unregister` makes this fail, which is what ties it to the fix.
    """
    from unittest import mock

    from pyiceberg.io.fsspec import _s3

    properties = {
        "s3.signer": "S3V4RestSigner",
        "uri": TEST_URI,
        # A port nothing listens on: `_s3` builds a client, it does not connect.
        "s3.endpoint": "http://127.0.0.1:1",
        "s3.access-key-id": "x",
        "s3.secret-access-key": "y",
        "s3.region": "us-east-1",
    }

    unregistered: list[tuple[str, Any]] = []
    real_unregister = HierarchicalEmitter.unregister

    def spy(
        self: HierarchicalEmitter,
        event_name: str,
        handler: Any = None,
        unique_id: Any = None,
        unique_id_uses_count: bool = False,
    ) -> Any:
        unregistered.append((event_name, unique_id))
        return real_unregister(self, event_name, handler, unique_id, unique_id_uses_count)

    with mock.patch.object(HierarchicalEmitter, "unregister", spy):
        first = _s3(properties)
        second = _s3(properties)

    assert first is second, "S3FileSystem is cached by fsspec, so both callers share one client and one event emitter"
    assert unregistered.count((SIGNER_EVENT, SIGNER_UNIQUE_ID)) == 2, (
        f"expected the signer to be unregistered on both calls, saw {unregistered} — the "
        "second happens while a signer is already installed, and opens the window for "
        "anything signing concurrently on the shared client"
    )


def test_the_signer_is_absent_between_unregister_and_register() -> None:
    """The window. `_s3()` does exactly these two calls, on a client shared by every thread.

    Anything signed here gets no `Authorization` header, because `_s3()` has also set
    `signature_version=UNSIGNED` and botocore will not sign in the signer's place.
    """
    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(signer)
    assert _installed_signer(emitter) is signer, "precondition: a signer is installed"

    emitter.unregister(SIGNER_EVENT, unique_id=SIGNER_UNIQUE_ID)

    assert _installed_signer(emitter) is None, (
        "no signer is installed at this instant; a concurrent request signed now leaves "
        "unsigned and the object store answers 403 AccessDenied"
    )


def test_a_request_signed_in_that_window_gets_no_authorization_header() -> None:
    """What the window costs, spelled out: the emitter has nothing to add to the request.

    Emitting `before-sign.s3` with no handler is silent — there is no error to notice, which
    is why this reaches the wire and comes back as an opaque permission failure.
    """
    from botocore.awsrequest import AWSRequest

    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(signer)
    emitter.unregister(SIGNER_EVENT, unique_id=SIGNER_UNIQUE_ID)

    request = AWSRequest(method="PUT", url="https://bucket/key", data=b"", params={})
    emitter.emit(SIGNER_EVENT, request=request)

    assert not any(name.lower() == "authorization" for name in dict(request.headers)), "the request would be sent unsigned"


def test_every_new_fileio_repeats_the_cycle() -> None:
    """Why the window is hit often rather than rarely.

    `FsspecFileIO.get_fs` caches per thread and each table gets its own `FileIO`, so `_s3()`
    runs again and again — over a hundred times in a short workload — against one shared
    emitter. Each repetition is another opportunity.
    """
    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(signer)

    absent = 0
    for _ in range(100):
        emitter.unregister(SIGNER_EVENT, unique_id=SIGNER_UNIQUE_ID)
        if _installed_signer(emitter) is None:
            absent += 1
        emitter.register_last(SIGNER_EVENT, signer, unique_id=SIGNER_UNIQUE_ID)

    assert absent == 100, "each _s3() call opens the window once"


# -- the behaviour the suggested fix relies on ---------------------------------------------


def test_registering_without_unregistering_is_a_noop() -> None:
    """Botocore already makes registration idempotent by `unique_id`.

    `_register_section` returns early for an id it holds, so the `unregister` in `_s3()` is
    not merely harmful — it is unnecessary. Dropping it leaves a signer installed at every
    instant, and the later `register_last` calls do nothing.
    """
    first = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    second = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(first)

    emitter.register_last(SIGNER_EVENT, second, unique_id=SIGNER_UNIQUE_ID)

    assert _installed_signer(emitter) is first, (
        "the first registration stays and the second is ignored — botocore does not stack a duplicate unique_id"
    )


def test_dropping_the_unregister_keeps_a_signer_installed_throughout() -> None:
    """The suggested fix, exercised over the same hundred cycles as the defect test."""
    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(signer)

    for i in range(100):
        # The fix: register only. No unregister, so no window.
        emitter.register_last(SIGNER_EVENT, signer, unique_id=SIGNER_UNIQUE_ID)
        assert _installed_signer(emitter) is signer, f"signer went missing at cycle {i}"


def test_threads_repeating_the_cycle_never_leave_it_unregistered_once_fixed() -> None:
    """The concurrent shape, with the fix: many threads calling `_s3()` against one emitter.

    Deliberately not the reverse test — asserting that the *unfixed* sequence loses a race is
    timing-dependent and would be flaky in CI. The defect is pinned deterministically above.
    """
    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    emitter = _emitter_with_signer(signer)
    missing: list[int] = []
    start = threading.Barrier(8)

    def repeat() -> None:
        start.wait()
        for _ in range(50):
            emitter.register_last(SIGNER_EVENT, signer, unique_id=SIGNER_UNIQUE_ID)
            if _installed_signer(emitter) is None:
                missing.append(1)

    workers = [threading.Thread(target=repeat) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    assert not missing, "a signer was installed at every observation"
