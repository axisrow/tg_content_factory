# Scheduler (operator guide)

This document describes how to enable and operate the pipeline scheduler.

Overview

The SchedulerManager runs periodic jobs (collection and pipeline generation) using APScheduler inside the worker daemon. Each active pipeline with a positive `generate_interval_minutes` has a named job `content_generate_{pipeline_id}` which enqueues a `CONTENT_GENERATE` background task (the former periodic `pipeline_run_{id}` job was retired in #835; `PIPELINE_RUN` stays for explicit one-off runs). Jobs only enqueue work; heavy processing is performed by the worker via the TaskEnqueuer/CollectionQueue and UnifiedDispatcher. An enqueue wakes the worker's queue loops at once (#1868); polling is only a safety net.

Enabling the scheduler

- Run the worker daemon: `tg-agent restart` (stops any previous daemon, then runs the worker in this process — keep it in the background, under tmux or systemd; `tg-agent worker` is the same runtime without the stop-first step). The web panel's `serve` process was removed in #1868.
- Enable the scheduler with `tg-agent scheduler start`: it writes `scheduler_autostart=1` and reconciles the running worker; the worker also starts the scheduler on boot while that setting is on. `tg-agent scheduler stop` turns it off.

Environment & secrets

- Do NOT commit provider API keys or `SESSION_ENCRYPTION_KEY` to the repo. Provider secrets are configured with `tg-agent provider add NAME --api-key KEY` (stored encrypted in the database, which requires `SESSION_ENCRYPTION_KEY`) or come from environment variables.
- If you plan to run external LLM providers, place keys in the environment or in the configuration management system — avoid storing plaintext in git.

Running & testing

- Manual run: `tg-agent pipeline run <id>` runs one generation right away in the CLI process (`--preview` / `--publish`); while the daemon runs, its Telegram steps ride the daemon's connection through the local relay.
- To verify scheduled jobs, run `tg-agent scheduler status`: with a live worker it lists each job (`content_generate_<id>`, `collect_all`, …) with its next run time.
- The worker daemon must be running to process enqueued tasks; check it with `tg-agent daemon status`.

Troubleshooting

- Duplicate jobs: the scheduler uses deterministic job ids (`content_generate_{id}`) and `replace_existing=True` when registering jobs. If duplicate behavior is observed, verify there are not multiple scheduler instances running against the same DB state.
- No runs being processed: confirm the worker daemon is up (`tg-agent daemon status`) and processing CONTENT_GENERATE tasks. Check UnifiedDispatcher logs (`tg-agent debug logs`) for errors.
- Missing provider configuration: generation tasks will fail early and be marked `failed` if provider configuration is missing.

Maintenance

- To unschedule a pipeline, disable it (`tg-agent pipeline toggle <id>`), disable its job (`tg-agent scheduler job-toggle content_generate_<id>`) or delete it (`tg-agent pipeline delete <id>`); scheduler.sync_pipeline_jobs() will remove the job.
- For large fleets of pipelines, tune `generate_interval_minutes` (`tg-agent scheduler set-interval content_generate_<id> MINUTES`) conservatively to avoid overloading downstream providers or the worker.
