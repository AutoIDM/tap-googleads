# tap-googleads

`tap-googleads` is a Singer tap for GoogleAds.

THIS IS NOT READY FOR PRODUCTION. Bearer tokens sometimes slip out to logs. Use at your own Peril :D


Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-googleads
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

### This tap supports two sets of configs:

### Using Your Own Credentials

Settings required to run this tap.

- `client_id` (required)
- `client_secret` (required)
- `developer_token` (required)
- `refresh_token` (required)
- `customer_login_id` (required)
- `customer_id` (required)
- `start_date` (optional)

How to get these settings can be found in the following Google Ads documentation:

https://developers.google.com/adwords/api/docs/guides/authentication

If you have installed the tap you can run the following commands to see more information about the tap.
```bash
tap-googleads --about
```

### Getting A Refresh Token
1. GET https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=client_id&redirect_uri=http://127.0.0.1&scope=https://www.googleapis.com/auth/adwords&state=autoidm&access_type=offline&prompt=select_account&include_granted_scopes=true
1. POST https://www.googleapis.com/oauth2/v4/token?code={code}&client_id={client_id}&client_secret={client_secret}&redirect_uri=http://127.0.0.1&grant_type=authorization_code
1. POST https://www.googleapis.com/oauth2/v4/token?refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token


### Proxy OAuth Credentials

To run the tap yourself It is highly recommended to use the [Using Your Own Credentials](#using-your-own-credentials) section listed above.

These settings for handling your credentials through a Proxy OAuth Server, these settings are used by default in a [Matatika](https://www.matatika.com/) workspace.

The benefit to using these settings in your [Matatika](https://www.matatika.com/) workspace is that you do not have to get or provide any of the OAuth credentials. All a user needs to do it allow the Matatika App permissions to access your GoogleAds data, and choose what `customer_login_id` and `customer_id` you want to get data from.

All you need to provide in your [Matatika](https://www.matatika.com/) workspace are:
- Permissions for our app to access your google account through an OAuth screen
- `customer_login_id` (required)
- `customer_id` (required)
- `start_date` (optional)

These are not intended for a user to set manually, as such setting them could cause some config conflicts that will now allow the tap to work correctly.

Also set in by default in your [Matatika](https://www.matatika.com/) workspace environment:

- `oauth_credentials.client_id`
- `oauth_credentials_client_secret`
- `oauth_credentials.authorization_url`
- `oauth_credentials.scope`
- `oauth_credentials.access_token`
- `oauth_credentials.refresh_token`
- `oauth_credentials.refresh_proxy_url`


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
