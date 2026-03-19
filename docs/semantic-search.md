# Semantic Search Roadmap Correction

This document explains why the project is moving away from mandatory
`sqlite-vec` as the foundation for semantic retrieval, and what the corrected
target architecture should look like.

## Status

The current codebase still contains a legacy semantic search path built around
runtime `sqlite-vec` loading. This document is a roadmap and specification
correction that should guide the next implementation step. It does not claim
that the portable backend is already shipped.

## Why mandatory `sqlite-vec` is being retired

The problem is not search quality. The problem is installability and operator
reliability.

`sqlite-vec` can be installed successfully and still remain unusable at runtime.
The package alone is not enough. The active Python/SQLite build must also expose
`sqlite3.enable_load_extension(...)` and allow loading external SQLite
extensions. That behavior depends on how Python and SQLite were built, not just
on the Python version number.

In practice this creates a bad contract for a core feature:

- the same `pip install` can behave differently on different machines;
- semantic search may silently degrade to unavailable after a nominally
  successful install;
- CI and local development have to special-case and skip semantic paths when the
  interpreter cannot load extensions;
- operators get a feature that appears configured but remains disabled because
  of the host Python build;
- support burden increases because the failure mode is environment-specific
  rather than application-specific.

For a foundational feature, that is too fragile. The product should not require
users to rebuild Python or switch Python distributions just to make semantic
search available.

## Corrected target architecture

The target is a portable SQLite-first semantic backend that works on stock
Python builds without `enable_load_extension`.

### Storage model

- store embeddings in ordinary SQLite tables rather than a `vec0` virtual table;
- keep one active embedding dimension per instance;
- track metadata needed for reset and reindexing in normal settings/state rows;
- treat `sqlite-vec` as a future optional accelerator backend behind the same
  internal interface, not as the only supported backend.

### Retrieval model

- use a backend-agnostic semantic retrieval interface;
- generate a candidate set without scanning the full corpus on every query;
- rerank the selected candidates in Python with exact similarity scoring;
- keep hybrid search as the combination of FTS5 keyword retrieval and semantic
  retrieval, rather than tying hybrid mode to a `sqlite-vec` KNN operator.

### Indexing flow

- preserve incremental indexing from `messages` via `semantic_last_embedded_id`;
- keep provider/model settings from `EmbeddingService`;
- allow reset and full reindex from the existing message corpus;
- require reset and reindex after an incompatible model or embedding dimension
  change.

## Operator behavior

The corrected operator contract is:

- semantic search should eventually work on a normal Python install;
- no Python rebuild should be required;
- no `enable_load_extension` support should be required;
- no external vector database should be required for the first portable version;
- the public UX remains the same: semantic indexing, semantic search, and
  hybrid search stay part of the product contract.

Until the portable backend lands in code, the current `sqlite-vec` path should
be treated as legacy behavior, not as the long-term installation story.

## Migration, reset, and reindex

The corrected roadmap assumes a conservative migration story:

- do not require direct migration from the legacy `vec_messages` table;
- allow the portable backend to rebuild its index from the canonical `messages`
  table;
- expose reset and reindex as the standard recovery path;
- keep current embedding-provider settings so operator configuration does not
  need to be reinvented.

This avoids coupling the new backend to the legacy SQLite extension path.

## Limits and honest tradeoffs

The first portable implementation prioritizes installability and portability
over native KNN acceleration.

That implies:

- one active embedding model and dimension per instance;
- reindex required after incompatible model switches;
- semantic retrieval may use candidate generation plus Python reranking rather
  than native vector search;
- future accelerator backends remain possible, but they must be optional and
  hidden behind the same internal interface.

## FAQ

### Why not just tell operators to install `sqlite-vec`?

Because that does not solve the real problem. The package may be present while
the active Python build still cannot load SQLite extensions. Requiring users to
debug or rebuild their interpreter is not an acceptable baseline for a core
feature.

### Why not require a custom Python build?

Because that makes onboarding, support, packaging, CI, and reproducible
deployment harder. It also creates a "works on my machine" class of failures
that the application cannot control.

### Why not move straight to an external vector database?

The first correction is about portability, not infrastructure expansion. A
local portable backend keeps deployment simple and preserves the product's
single-node/operator-friendly model.

### What happens to `sqlite-vec`?

It may remain useful as an optional accelerator path later. The correction is
that it stops being the required foundation for semantic search.

### Does this mean semantic search quality is being downgraded?

No. The motivation is not retrieval quality. The motivation is to keep semantic
search available on ordinary installs while preserving the same product-level
capabilities.
