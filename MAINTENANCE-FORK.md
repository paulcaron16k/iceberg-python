# `feature/maintenance` — the ExperienceFlow integration branch

**This branch is not upstream Apache PyIceberg.** It is a fork branch carrying
table-maintenance patches that Zamboni needs and upstream does not yet have.

It exists so those patches live *in the library*, where its own tests and type
checker can see them, instead of being reached for past the fence at runtime by
`zamboni`'s private-API subclasses (see `docs/pyiceberg-private-api.md` in the
Zamboni repository).

| | |
|---|---|
| Base | `apache/iceberg-python` **`main` at `042165ac`**, pinned (rebased 2026-10-27 from `9299bdb8`; the old tip is kept as tag `maintenance/base-9299bdb8`) |
| Consumers | Zamboni's development build and ExperienceFlow IWS, via a pinned commit — not via this branch name. target-iceberg does not pin it; its mirrors are verified against it by hand. pip users of either get the mirrors instead |
| Purpose | Hold what Zamboni and target-iceberg currently reach past the fence for |

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
                          -> mirrored in Zamboni as a private-API override
                             against the released 0.12 line
```

## Why every change exists twice

Each patch here has a twin in Zamboni, target-iceberg or both, wherever that
consumer needs it, and that is deliberate, not drift.

Zamboni's consumers install it with `pip install iceberg-zamboni`. pip resolves
`pyiceberg` from PyPI and has no way to follow Zamboni to a git fork, so nothing
on this branch reaches them. Meltano installs target-iceberg the same way, with
pip, and its `pyiceberg` likewise comes from PyPI. For them, the fix or enhancement has to ship inside
the consumer itself, as an override of PyIceberg's private API against the released
0.12 line, which does not move. So there are two paths for the same change:

1. **This branch**, where the change lives in the library with the library's own
   tests and type checker. Zamboni's development build pins it by SHA (see
   `[tool.uv.sources]` in Zamboni's `pyproject.toml`).
2. **The consumer's override**, which is what pip users actually run.

This branch is kept anyway, even though pip users never see it, because the end
goal is to have every one of these changes in a released iceberg-python. A patch
written in the library, in upstream's style and against upstream's fixtures, is
the form that can get there. An override in a consumer is not. When a release
carries the change, each consumer raises its minimum PyIceberg and drops its override,
and the patch leaves this branch (see "Retiring a patch").

**Nothing here is proposed upstream at this time. At some point in the future
(date TBD), an effort to push our bug fixes and enhancements upstream will be
started. This is why we maintain separate branches for each one.** The patches
are written in upstream's own style and tested with upstream's own fixtures for
the same reason: so they stay reviewable and rebasable until then.

Keeping them separate is what lets each override move independently, and what
would let any of them be dropped if upstream ever implements the same thing.

## What is on this branch

Patches are added as their Zamboni stories or target-iceberg issues are worked.
The right-hand columns are the point: each row is something a consumer stops
doing behind the library's back once a released PyIceberg carries the patch.
Until then it stays in that consumer as the twin described above. A cell says
*not needed* where that consumer never takes the code path, and *test helper*
where the mirror exists only for Zamboni's tests and demo, never in production.

| Patch | Driven by | Mirrored in Zamboni | Mirrored in target-iceberg |
|---|---|---|---|
| Added data files are written under their own partition spec (`_write_added_manifest`, `_summary`) | [ZMBNI-59](https://github.com/paulcaron16k/Zamboni/issues/59) | `MultiSpecReplaceFiles._manifests` and `._summary` in `evolution.py` | **Gap:** `_AppendDeleteFiles._manifests` groups by spec, but on 0.12 a delete file's `spec_id` is lost (`DataFile.from_args` drops it), so the default spec is used. ELT-1270 |
| A file rewrite no longer conflicts with an unrelated concurrent append (`_validate_concurrency`) | [ZMBNI-79](https://github.com/paulcaron16k/Zamboni/issues/79) | **Gap:** none since ZMBNI-79 removed the `expected_snapshot_id` guard (2026-09-16); on 0.12 such a rewrite is refused and retried. [ZMBNI-239](https://github.com/paulcaron16k/Zamboni/issues/239) | Not needed on 0.12. On this branch its merge-on-read commit goes through `_OverwriteFiles` and depends on this patch |
| A manifest can be written with `content: deletes` (`write_manifest`, `ManifestWriterV2`) | ELT-1118 — see below | `_DeleteManifestWriter` in `testing.py` (test helper) | `_DeleteManifestWriter` in `_pyiceberg_mor.py` |
| Positional delete files can be written, with `pos` typed `long` and both columns required, as the spec requires (`write_positional_deletes`, `POSITIONAL_DELETE_SCHEMA`) | ELT-1121 — see below | Hand-written delete Parquet in `testing.py` (test helper; not equivalent: no column bounds) | `write_position_deletes` in `_pyiceberg_mor.py` |
| Delete files are committed into delete manifests rather than data manifests (`_manifests`, `new_manifest_writer`) | ELT-1124 — see below | `_AppendDeleteFiles` in `testing.py` (test helper, delete files only) | `_AppendDeleteFiles._manifests` in `_pyiceberg_mor.py` (ELT-1266) |
| Delete files are committed with an overwrite, never an append, and are refused if a data file they name was concurrently rewritten (`_FastAppendFiles.append_data_file`, `_validate_referenced_data_files_exist`) | ELT-1267, ELT-1268 | Not needed in production. **Gap:** `testing.py` commits delete files through an append, which this patch refuses; Zamboni's pin predates it. [ZMBNI-240](https://github.com/paulcaron16k/Zamboni/issues/240) | `_AppendDeleteFiles._validate_concurrency` (ELT-1268); `_OverwriteFiles` on this branch (ELT-1267) |
| The S3 remote signer is registered on the session, once per signer: concurrent writes no longer go out unsigned (403), and each table signs through its own endpoint (`_s3`, `_signed_s3_filesystem`) | [ZMBNI-51](https://github.com/paulcaron16k/Zamboni/issues/51), ELT-1269 | `signer.py`, installed into `SCHEME_TO_FS` by `CatalogSession` (in review, [#242](https://github.com/paulcaron16k/Zamboni/pull/242)) | `_pyiceberg_signer.py`, installed into `SCHEME_TO_FS` before `load_catalog` (in review, ELT-1269) |

Rows three to five together are what let PyIceberg write a merge-on-read table at all: five rows, a delete
file naming two positions, and a scan that returns the other three.

### The delete-manifest patch broadens this branch, deliberately

The rows before it each move something **Zamboni** reaches past the fence for, and are driven by a
Zamboni story. The delete-manifest writer is the first that is not: it is driven by
**`target-iceberg`'s merge-on-read initiative (ELT-1107)**, which cannot commit a delete file of any
kind without it — `ManifestWriterV2.content()` returned `ManifestContent.DATA` unconditionally, so
there was nowhere to put one.

It still earns its row on the original terms, because Zamboni already carries the same override:
`_DeleteManifestWriter` in `src/zamboni/testing.py`, which exists only so the merge-on-read path can
be exercised at all. Like the others, that override stays in Zamboni until a released PyIceberg
carries this patch.

What changes is the branch's stated purpose: it now carries patches **two** consumers need, not one.
Worth knowing before the next patch is proposed, because "does Zamboni need it?" is no longer the
whole test.

### Deliberately **not** carried

Third-party pull requests are not merged here without a decision, however useful
they look. [#3131](https://github.com/apache/iceberg-python/pull/3131) would
cover the same ground as Zamboni's `_ReplaceFiles` and its `_summary` relabel,
and was measured against Zamboni's suite (815 passed, 1 pre-existing failure) —
but carrying 1130 lines of somebody else's unmerged work in a production
dependency is a bigger commitment than carrying our own, and is a call for the
humans.

## Retiring a patch

A patch leaves this branch when a released upstream ships the same behaviour,
which is the goal for every row. When that happens:

1. Confirm by measurement that the released library does what the patch did.
2. Drop the patch from this branch and rebase onto the release.
3. In each consumer that mirrors it, raise the minimum PyIceberg to that
   release and delete the matching override, so pip users get the behaviour from the library.
4. Delete its row from the table above.

Merged to upstream `main` is not enough. pip users only get what is released,
so the consumers' overrides stay until a release carries the change. When the table
is empty, delete the branch and pin Zamboni back to a released PyIceberg.

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
