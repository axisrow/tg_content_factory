# CLI bug audit evidence

Audited version: `tg-agent 0.2.4.1`, commit `3c6fcd03176319fd6bd92f26b36e5f571bc1b1ef`.

The PNGs render the actual saved CLI stdout/stderr from separate invocations. The matching `.txt` files preserve the captured transcripts. Screenshots were captured against an isolated fixture at server time `2026-10-22 16:49 UTC`. Credentials in this evidence are fake. No Telegram accounts or external API calls were used.

- `01-secret`: the fake `tg_api_hash` is printed without masking.
- `02-count`: a last-24-hour read returns 1 message while analytics reports 3.
- `03-timezone`: equivalent UTC and UTC+03 bounds return different results.
- `04-length`: the five-character message `abcde` disappears at both length boundaries.

## Reproduction

With the audited revision installed in a Python environment, run:

```bash
python reproduce.py
```

The script creates a new temporary database and config, prints their paths, and runs the CLI for each case. It uses dates relative to the current UTC clock for the rolling-day counter so that this case remains reproducible after the original audit date. Fixed dates are used for the timezone and length cases. It does not modify the project's database or use live Telegram connections.
