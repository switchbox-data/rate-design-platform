# rate-design-platform

[![Release](https://img.shields.io/github/v/release/switchbox-data/rate-design-platform)](https://img.shields.io/github/v/release/switchbox-data/rate-design-platform)
[![Build status](https://img.shields.io/github/actions/workflow/status/switchbox-data/rate-design-platform/main.yml?branch=main)](https://github.com/switchbox-data/rate-design-platform/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/switchbox-data/rate-design-platform/branch/main/graph/badge.svg)](https://codecov.io/gh/switchbox-data/rate-design-platform)
[![Commit activity](https://img.shields.io/github/commit-activity/m/switchbox-data/rate-design-platform)](https://img.shields.io/github/commit-activity/m/switchbox-data/rate-design-platform)
[![License](https://img.shields.io/github/license/switchbox-data/rate-design-platform)](https://img.shields.io/github/license/switchbox-data/rate-design-platform)

A simulation testbed for analyzing the impact of electric rate designs on energy bills and burden for households adopting DERs and all-electric appliances.

- **Github repository**: <https://github.com/switchbox-data/rate-design-platform/>
- **Documentation** <https://switchbox-data.github.io/rate-design-platform/>

## Getting started with your project


### 1. Set Up Your Development Environment

The easiest way to set up the library's dev environment is to use devcontainers. To do so, open up the repo in VSCode or a VSCode fork like Cursor or Positron. The editor will auto-detect the presence of the repo's devcontainer (configured in `.devcontainer/devcontainer.json`). Click "Reopen in Container" to launch the devcontainer.

Alternatively, you can install the environment and the pre-commit hooks on your laptop with

```bash
make install
```

You are now ready to start development on the library!

The github action CI/CD pipeline will be triggered when you open a pull request, merge to main, or when you create a new release.

### 2. Set up PyPI publishing

To finalize the set-up for publishing to PyPI, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/publishing/#set-up-for-pypi).
For activating the automatic documentation with MkDocs, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/mkdocs/#enabling-the-documentation-on-github).
To enable the code coverage reports, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/codecov/).

## Releasing a new version

- Create an API Token on [PyPI](https://pypi.org/).
- Add the API Token to your projects secrets with the name `PYPI_TOKEN` by visiting [this page](https://github.com/switchbox-data/rate-design-platform/settings/secrets/actions/new).
- Create a [new release](https://github.com/switchbox-data/rate-design-platform/releases/new) on Github.
- Create a new tag in the form `*.*.*`.

For more details, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/cicd/#how-to-trigger-a-release).

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
