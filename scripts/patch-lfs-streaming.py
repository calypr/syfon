#!/usr/bin/env python3
"""Use Fiber's request stream for the generated LFS upload binding."""

import pathlib
import sys


def main() -> None:
    path = pathlib.Path(sys.argv[1])
    original = "\trequest.Body = bytes.NewReader(ctx.Request().Body())\n"
    replacement = (
        "\trequest.Body = ctx.Request().BodyStream()\n"
        "\tif request.Body == nil {\n"
        "\t\trequest.Body = bytes.NewReader(ctx.Request().Body())\n"
        "\t}\n"
    )
    source = path.read_text()
    if source.count(replacement) == 1 and original not in source.replace(replacement, ""):
        return
    if source.count(original) != 1:
        raise SystemExit(f"expected one LFS upload body binding in {path}")
    path.write_text(source.replace(original, replacement, 1))


if __name__ == "__main__":
    main()
