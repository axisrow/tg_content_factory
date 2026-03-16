# Scheduler (operator guide)

This document describes how to enable and operate the pipeline scheduler.

Overview

The SchedulerManager runs periodic jobs (collection and pipeline runs) using APScheduler. Each active pipeline with a positive `generate_interval_minutes` has a named job `pipeline_run_{pipeline_id}` which enqueues a `PIPELINE_RUN` background task. Jobs only enqueue work; heavy processing is performed by background workers via the TaskEnqueuer/CollectionQueue and UnifiedDispatcher.

Enabling the scheduler

- Ensure the application is started with background task support (the usual `serve` command does this).
- Scheduler starts automatically if the app container is constructed with a `scheduler` instance. No extra flag is required, but `scheduler.start()` must be called by the application bootstrap (this is already wired).

Environment & secrets

- Do NOT commit provider API keys or `SESSION_ENCRYPTION_KEY` to the repo. The system requires provider secrets to be configured in the web UI or database settings and, when present, may require `SESSION_ENCRYPTION_KEY` for encrypted secret handling.
- If you plan to run external LLM providers, place keys in the environment or in the configuration management system — avoid storing plaintext in git.

Running & testing

- Manual trigger: the UI exposes a "Run now" button that enqueues a PIPELINE_RUN task for the selected pipeline; this returns quickly and the work proceeds via background workers.
- To verify scheduled jobs, check the web UI pipeline list — the next scheduled run time is displayed where available (or "Не запланировано").
- Background workers and a task runner must be running to process enqueued tasks; run the application worker process as you normally do for other background tasks.

Troubleshooting

- Duplicate jobs: the scheduler uses deterministic job ids (`pipeline_run_{id}`) and `replace_existing=True` when registering jobs. If duplicate behavior is observed, verify there are not multiple scheduler instances running against the same DB state.
- No runs being processed: confirm there is a TaskEnqueuer and a worker processing CollectionTasks/PIPELINE_RUN. Check UnifiedDispatcher logs for errors.
- Missing provider configuration: generation tasks will fail early and be marked `failed` if provider configuration is missing.

Maintenance

- To unschedule a pipeline, either disable it in the UI or delete it; scheduler.sync_pipeline_jobs() will remove the job.
- For large fleets of pipelines, tune `generate_interval_minutes` conservatively to avoid overloading downstream providers or workers.
