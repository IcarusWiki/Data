Extracted Icarus Data for Github Actions

`ICARUS_BUILD_ID` tracks the last build that automation successfully dispatched to the
heavy workflow, not the last build that completed successfully. That is intentional:
if the heavy workflow later fails or is cancelled, polling will not auto-retry the
same build ID. Maintainers can still rerun the heavy workflow manually with
`workflow_dispatch`.

`ICARUS_STEAM_NEWS_LAST_GID` tracks the newest Steam community announcement that the
`publish-icarus-steam-news.yml` workflow has fully published into the wiki.gg `News:`
namespace. Bootstrap that workflow with a manual `workflow_dispatch` run in
`backfill_all` mode before scheduled incremental runs can succeed.

Manual Steam-news testing and staged backfills can be done in chunks by setting the
workflow's `max_items` input to a small positive number such as `1` or `5`. Successful
non-dry-run chunked publishes advance `ICARUS_STEAM_NEWS_LAST_GID` to the newest item
included in that staged batch. To keep the cursor moving safely, chunked runs consume
the oldest pending batch first.
