# Release Metadata

Create one JSON file per release tag in this directory before pushing the tag.

Example:

```json
{
  "tag": "v0.1.11",
  "title": "Short Release Title",
  "highlights": [
    {
      "title": "Highlight One",
      "description": "One-sentence summary for the release notes."
    }
  ]
}
```

The release workflow reads `.github/release-metadata/<tag>.json`, generates the
`## What's Changed` section from GitHub, inserts `### Highlights` from this file,
and publishes the release title as `<tag> — <title>`.
