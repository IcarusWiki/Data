# Icarus Data

This repository is the **data backbone for [icarus.wiki.gg](https://icarus.wiki.gg)**.
It automatically tracks the game *Icarus* (Steam App `1149460`), extracts its
in-game data files whenever a new build ships, and feeds that data — plus
official Steam community news — to the wiki.

The repo's job is to keep a clean, up-to-date mirror of the raw game data 
the wiki and its downstream tools build on.

## What's in the repo

- [InGameFiles/](InGameFiles/) — the **fully unpacked contents of the game's
  `data.pak`**. This pak is small and holds the game's configuration data
  (item definitions, recipes, stats, talent trees, etc.).
  The contents are committed to this repo so wiki
  editors and consumer repos can read from it directly without any extraction
  step. It is **not** the full game — large content paks (meshes, textures,
  audio, world data) are processed separately (see below).
- [wiki/news/](wiki/news/) — Cargo-backed templates and the landing page used
  to render the wiki's `News:` namespace.
- [scripts/](scripts/) — Python scripts that drive extraction, Steam polling,
  pak processing, and wiki publishing.
- [.github/workflows/](.github/workflows/) — the automation that ties it all
  together (see below).

## How the automation works

There are three GitHub Actions workflows:

1. **[poll-icarus-update.yml](.github/workflows/poll-icarus-update.yml)** —
   checks Steam for a new public build of Icarus. When it sees one, it
   dispatches the heavy workflow.
2. **[process-icarus-data.yml](.github/workflows/process-icarus-data.yml)** —
   the workhorse. See "The data update pipeline" below for what it does.
3. **[publish-icarus-steam-news.yml](.github/workflows/publish-icarus-steam-news.yml)** —
   mirrors Steam community announcements into the wiki's `News:` namespace,
   including hero/body images.

## The data update pipeline

`process-icarus-data.yml` handles two different kinds of pak file, in
two distinct stages.

### Stage 1 - `data.pak` -> `InGameFiles/`

`data.pak` is the small configuration pak. The workflow downloads it,
extracts everything into [InGameFiles/](InGameFiles/), and commits the diff.
If anything actually changed, it then fires a `repository_dispatch` event
(`icarus-updated`) at consumer repos like
[IcarusWiki/TreeUI](https://github.com/IcarusWiki/TreeUI) with the new
commit SHA, so they can pull the fresh data and rebuild their own artifacts
on their own schedule.

### Stage 2 - content paks -> per-pak processors -> finalizers

The other paks under `Icarus/Content/Paks/` are large (textures, meshes,
audio, world data). They are far too big to keep in this repo, so they are
processed transiently:

1. **Discover.** A separate job downloads only the depot manifest (no file
   contents), then [scripts/build_pak_matrix.py](scripts/build_pak_matrix.py)
   reads it and uses the rules in
   [.github/icarus-pak-processors.yml](.github/icarus-pak-processors.yml) to
   build a matrix of `(pak, processors that want it)` jobs. Rules can be
   `run_on_all` (every pak) or `targeted` by glob on the depot-relative pak
   path.
2. **Process one pak at a time.** For each scheduled pak the workflow
   downloads only that one pak, extracts it to a scratch directory, and
   runs each matching processor. **A processor is a script that lives in a
   consumer repo** (e.g.
   [IcarusWiki/DataMaps](https://github.com/IcarusWiki/DataMaps)'s
   `extract_plant_map_partials.py`). It reads the unpacked pak and writes
   small **partial artifacts** which are uploaded as
   `consumer-artifacts-<pak-slug>`. The pak itself is then deleted —
   nothing about content paks is committed to this repo.
3. **Finalize.** Once every pak has been processed, the **finalizer** jobs
   defined in [.github/icarus-consumers.yml](.github/icarus-consumers.yml)
   run. Each finalizer checks out its consumer repo, downloads all the
   partial artifacts the per-pak processors produced, runs a merge command
   in the consumer repo (e.g. `merge_plant_maps.py`), and commits the
   final result to the consumer repo.

The split lets a consumer like DataMaps stitch together output from many
content paks (e.g. one partial plant-map per voxel pak) into a single
finished artifact, without this repo ever needing to know what plant maps
are. Both config files (`icarus-pak-processors.yml` and
`icarus-consumers.yml`) keep the workflow generic — adding a new
processor or consumer is a config change plus consumer-repo scripts, not
a workflow edit.

## Maintainer notes

### Build tracking

`ICARUS_BUILD_ID` tracks the last build the poller successfully **dispatched**
to the heavy workflow — not the last build that completed successfully. This
is intentional: if the heavy workflow later fails or is cancelled, polling
will not auto-retry the same build ID. Rerun the heavy workflow manually with
`workflow_dispatch` instead.

### Steam news cursor

`ICARUS_STEAM_NEWS_LAST_GID` tracks the newest Steam announcement that
`publish-icarus-steam-news.yml` has fully published. Bootstrap this workflow
with a manual `workflow_dispatch` run in `backfill_all` mode before scheduled
incremental runs can succeed.

For staged backfills or manual testing, set the workflow's `max_items` input
to a small positive number (e.g. `1` or `5`). Each chunked run picks up the
**oldest unpublished posts first**, and on success the cursor advances to the
newest item it just published — so repeated chunked runs walk the backlog
forward in order. The publisher also throttles its wiki edits to stay safely
under the wiki's anonymous edit rate limit.

### News landing page

Before republishing historical `News:` articles, publish `Template:NewsInfo`,
`Template:NewsYearSection`, and the `News` page from [wiki/news/](wiki/news/)
to populate the `NewsArticles` Cargo table. The publisher also mirrors Steam
hero/body images to wiki files on a best-effort basis, so the automation
account needs **file-upload permission** in addition to page-edit permission.
