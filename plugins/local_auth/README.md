# Local Auth Plugin

This plugin implements the Syfon AuthenticationPlugin interface for local (basic auth) authentication.

## Usage

Build the plugin binary:

```
go build -o local_auth_plugin .
```

Set the following environment variables as needed:
- `DRS_BASIC_AUTH_USER` and `DRS_BASIC_AUTH_PASSWORD` for basic auth credentials

Configure Syfon to use the plugin:

```
export SYFON_AUTHN_PLUGIN_PATH=/path/to/local_auth_plugin
syfon serve
```

