# Troubleshooting

## Common issues

### MkDocs warnings about missing files

Ensure all files referenced in `mkdocs.yml` under `nav` exist under `docs/`.
- Docker mapping will use docs/ as the default docs directory, so paths must be relative to that. (`docs/` prefix not needed in `mkdocs.yml`)

### Documentation does not update
- Ensure you have rebuilt the docs after making changes:

      ```bash
      make docs
      ```

### Server does not start

- Check that Go and dependencies are installed.
- Run `make test` to ensure the code passes tests.

### test failures

- Ensure a server is not already running on the configured port.

      ```bash
      ps -ef | grep go | grep server
      ```
