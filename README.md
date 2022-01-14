# ` tap-googleads` ![Build and Tests](https://github.com/AutoIDM/tap-googleads/actions/workflows/ci.yml/badge.svg?branch=main) [![PyPI download month](https://img.shields.io/pypi/dm/tap-googleads.svg)](https://pypi.python.org/pypi/tap-googleads/) 

`tap-googleads` is a Singer tap for GoogleAds.

THIS IS NOT READY FOR PRODUCTION. Bearer tokens sometimes slip out to logs. Use at your own Peril :D


Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-googleads
```

## Configuration

### Get refresh token
1. GET https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=client_id&redirect_uri=http://127.0.0.1&scope=https://www.googleapis.com/auth/adwords&state=autoidm&access_type=offline&prompt=select_account&include_granted_scopes=true
1. POST https://www.googleapis.com/oauth2/v4/token?code={code}&client_id={client_id}&client_secret={client_secret}&redirect_uri=http://127.0.0.1&grant_type=authorization_code
1. POST https://www.googleapis.com/oauth2/v4/token?refresh_token={refres_token}&client_id={client_id}&client_secret={client_secret]&grant_type=refresh_token
### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-googleads --about
```

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-googleads` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-googleads --version
tap-googleads --help
tap-googleads --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_googleads/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-googleads` CLI interface directly using `poetry run`:

```bash
poetry run tap-googleads --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-googleads
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-googleads --version
# OR run a test `elt` pipeline:
meltano elt tap-googleads target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
