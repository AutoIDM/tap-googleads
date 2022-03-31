# ` tap-googleads` ![Build and Tests](https://github.com/AutoIDM/tap-googleads/actions/workflows/ci.yml/badge.svg?branch=main) [![PyPI download month](https://img.shields.io/pypi/dm/tap-googleads.svg)](https://pypi.python.org/pypi/tap-googleads/) 

THIS IS NOT READY FOR PRODUCTION. Bearer tokens sometimes slip out to logs. Use at your own Peril :D

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`

## Settings

| Setting          | Required | Default | Description |
|:-----------------|:--------:|:-------:|:------------|
| client_id        | True     | None    | ClientID from Oauth Setup |
| client_secret    | True     | None    | ClientSecret from Oauth Setup |
| developer_token  | True     | None    | Developer Token from Google Ads Console |
| refresh_token    | True     | None    | Refresh Token from Oauth dance |
| customer_id      | True     | None    | Customer ID from Google Ads Console, note this should be the top level client. This tap will pull all subaccounts |
| login_customer_id| True     | None    | Customer ID that has access to the customer_id, note that they can be the same, but they don't have to be as this could be a Manager account |
| start_date       | True     | 2022-03-24T00:00:00Z | Date to start our search from, applies to Streams where there is a filter date. Note that Google responds to Data in buckets of 1 Day increments |
| end_date         | True     | 2022-03-31T00:00:00Z | Date to end our search on, applies to Streams where there is a filter date. Note that the query is BETWEEN start_date AND end_date |

### Get refresh token
1. GET https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=client_id&redirect_uri=http://127.0.0.1&scope=https://www.googleapis.com/auth/adwords&state=autoidm&access_type=offline&prompt=select_account&include_granted_scopes=true
1. POST https://www.googleapis.com/oauth2/v4/token?code={code}&client_id={client_id}&client_secret={client_secret}&redirect_uri=http://127.0.0.1&grant_type=authorization_code
1. POST https://www.googleapis.com/oauth2/v4/token?refresh_token={refres_token}&client_id={client_id}&client_secret={client_secret]&grant_type=refresh_token


## Installation

```bash
pipx install tap-googleads
```

## Configuration

### Accepted Config Options


A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-googleads --about
```

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

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.
