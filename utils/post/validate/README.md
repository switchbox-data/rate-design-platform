# Run Validation Framework

This package validates CAIRO runs 1-8 for HP rate design.

## Status

🚧 **In Progress** - See issue #324 for implementation plan.

## Planned Structure

- `config.py` - RunConfig and RunBlock dataclasses
- `discover.py` - S3 batch discovery
- `load.py` - CAIRO output readers
- `checks.py` - Validation assertions
- `tables.py` - Summary table builders
- `plots.py` - Plotnine plot functions
- `__main__.py` - CLI entrypoint
