# eso-s3-openapi-webapp (v8)

Key updates vs v7:

- Adds `cmd/openapi-normalize` which fixes nonstandard constructs (notably `tags[].description: { $ref: ... }`)
  by inlining referenced markdown into a string description before running `oapi-codegen`.
- Enhances `cmd/openapi-fetchgen` with `-with-refs` mode to pull a GitHub repo archive for the given `ref` and
  extract the OpenAPI file **plus its relative `$ref` files**, so multi-file specs like GA4GH DRS work.

## For GA4GH DRS-style specs
The GA4GH DRS root spec uses `tags[].description` objects with `$ref` to markdown, e.g.:

```yaml
tags:
  - name: Introduction
    description:
      $ref: ./tags/Introduction.md
```

This is not compatible with oapi-codegen/kin-openapi, which expects `Tag.description` to be a string.
This scaffold normalizes it automatically.

## Commands

```bash
make tools
make fetch OPENAPI_URL=https://github.com/ga4gh/data-repository-service-schemas/blob/develop/openapi/openapi.yaml
# or if you already have the repo checked out locally:
make gen OPENAPI=/path/to/openapi.yaml
```


## Notes

`make fetch` supports branch names with slashes (e.g. `feature/foo`) by using the GitHub tarball API and URL-escaping the ref.
