Extracted Icarus Data for Github Actions

`ICARUS_BUILD_ID` tracks the last build that automation successfully dispatched to the
heavy workflow, not the last build that completed successfully. That is intentional:
if the heavy workflow later fails or is cancelled, polling will not auto-retry the
same build ID. Maintainers can still rerun the heavy workflow manually with
`workflow_dispatch`.
