# `feature/maintenance` — the ExperienceFlow integration branch

**This branch is not upstream Apache PyIceberg.** It is a fork branch carrying
table-maintenance patches that Zamboni needs and upstream does not yet have.

It exists so those patches live *in the library*, where its own tests and type
checker can see them, instead of being reached for past the fence at runtime by
`zamboni`'s private-API subclasses (see `docs/pyiceberg-private-api.md` in the
Zamboni repository).

| | |
|---|---|
| Base | `apache/iceberg-python` **`main` at `9299bdb8`**, pinned |
| Consumers | ExperienceFlow IWS, via a pinned commit — not via this branch name |
| Purpose | Hold what Zamboni currently reaches past the fence for |

## Why `main` and not the `pyiceberg-0.12.x` release branch

Both were measured on 2026-09-09 before choosing:

* **`pyiceberg-0.12.x` is currently the 0.12.0 tag with zero commits past it.**
  The usual argument for tracking a release branch — that it carries upstream's
  own backported fixes — buys nothing today because there are none.
* **Every patch here is destined for `main`.** A pull request must target it, so
  authoring against anything else means maintaining each change twice and
  reconciling them forever.
* **The in-flight PRs we care about only apply to `main`.**
  [#3131](https://github.com/apache/iceberg-python/pull/3131) and
  [#3624](https://github.com/apache/iceberg-python/pull/3624) merge onto `main`
  cleanly — including into `pyiceberg/table/update/snapshot.py`, which both
  touch — and neither applies to `0.12.x` without a backport.
* **Zamboni passes equally on both.** 815 passed / 1 failed on `main`, on the
  0.12.0 release, and on `main` plus both PRs. The failure is Zamboni's own
  `pyiceberg-core` packaging assertion, not a regression.

**The base is pinned, not tracked.** "Off `main`" here means "off one commit of
`main`, advanced deliberately". Nothing consumes this branch by name: consumers
pin a commit SHA, so a rebase is a decision someone makes rather than something
that happens to a deploy. That is also what makes the dependency reproducible,
which a path dependency to a sibling checkout never was.

## How a patch gets here

One branch per override being moved, each self-contained and each carrying the
tests that prove it:

```text
enh/<short-name>          branched from the same pinned base
  - the behaviour, implemented in the library
  - its tests, which fail without it
                          -> merged into feature/maintenance
                          -> the matching override deleted from Zamboni,
                             in the same change that adopts it
```

**Nothing here is proposed upstream.** No pull requests, no issues, no comments
on existing ones. The patches are written in upstream's own style and tested
with upstream's own fixtures because that is how they stay reviewable and
rebasable — not because they are going anywhere.

Keeping them separate is what lets each override move independently, and what
would let any of them be dropped if upstream ever implements the same thing.

## What is on this branch

Nothing yet beyond the base and this file. Patches are added as their Zamboni
stories are worked.

| Patch | Zamboni story | Retires | Upstream |
|---|---|---|---|
| *(none yet)* | | | |

### Deliberately **not** carried

Third-party pull requests are not merged here without a decision, however useful
they look. [#3131](https://github.com/apache/iceberg-python/pull/3131) would
cover the same ground as Zamboni's `_ReplaceFiles` and its `_summary` relabel,
and was measured against Zamboni's suite (815 passed, 1 pre-existing failure) —
but carrying 1130 lines of somebody else's unmerged work in a production
dependency is a bigger commitment than carrying our own, and is a call for the
humans.

## Retiring a patch

A patch leaves this branch when upstream ships the same behaviour, which is not
something anyone here is arranging. If that happens:

1. Confirm by measurement that the released library does what the patch did.
2. Drop the patch from this branch and rebase onto the release.
3. Delete its row from the table above.

When the table is empty, delete the branch and pin the consumer back to a
released PyIceberg. Until then this branch is the dependency, and every row in
the table is something Zamboni no longer has to do behind the library's back.

## Verifying a change against Zamboni

From the Zamboni checkout, **not** from this one — `uv pip install` resolves the
virtualenv from the working directory, so running it here installs into this
repository's own `.venv` and the suite then tests whatever Zamboni already had:

```bash
cd /path/to/Zamboni
uv pip install -e ../iceberg-python
.venv/bin/python -c "import pyiceberg, importlib.metadata as m; \
    print(m.version('pyiceberg'), pyiceberg.__file__)"   # verify before trusting
.venv/bin/python -m pytest -q --ignore=tests/test_dev_stack.py
uv sync                                                   # back to the pinned line
```

`uv run` re-syncs from `uv.lock` and silently undoes the install, which is why
the interpreter is invoked directly.
