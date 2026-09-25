import os
from pathlib import Path
import subprocess
import tempfile
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release.yaml"


def release_shell():
    lines = WORKFLOW.read_text().splitlines()
    step = lines.index("      - name: Create GitHub Release")
    run = lines.index("        run: |", step) + 1
    end = next(index for index in range(run, len(lines))
               if lines[index].startswith("      - name: "))
    return "\n".join(line[10:] if line.startswith("          ") else ""
                      for line in lines[run:end]) + "\n"


class ReleasePublishingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="syfon-release-workflow-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.release_tag = "v1.2.3"
        self.assets = [
            f"syfon-linux-amd64-{self.release_tag}.tar.gz",
            f"syfon-darwin-arm64-{self.release_tag}.tar.gz",
            f"syfon-{self.release_tag}-checksums.txt",
        ]
        for asset in self.assets:
            (self.dist / asset).write_text("fixture")

        self.tools = self.root / "tools"
        self.tools.mkdir()
        gh = self.tools / "gh"
        gh.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "printf '%s\\n' \"$*\" >> \"$GH_CALLS\"\n"
            "case \"$1 $2\" in\n"
            "  'release view')\n"
            "    if [[ \"$RELEASE_EXISTS\" == true ]]; then\n"
            "      printf '%s\\n' \"$RELEASE_STATE\"\n"
            "      exit 0\n"
            "    fi\n"
            "    exit 1\n"
            "    ;;\n"
            "  'release upload'|'release edit'|'release create') exit 0 ;;\n"
            "  *) echo \"unexpected gh call: $*\" >&2; exit 64 ;;\n"
            "esac\n")
        gh.chmod(0o755)
        self.calls = self.root / "gh-calls"

    def run_release_step(self, exists, release_state=""):
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", release_shell()],
            cwd=self.root,
            env=dict(os.environ, RELEASE_TAG=self.release_tag,
                     RELEASE_EXISTS=str(exists).lower(), RELEASE_STATE=release_state,
                     GH_CALLS=str(self.calls), PATH=f"{self.tools}:{os.environ['PATH']}"),
            text=True,
            capture_output=True,
        )

    def test_incomplete_draft_uploads_only_missing_assets_then_publishes(self):
        existing = [self.assets[0], self.assets[2]]
        state = "true\t" + "\t".join(existing)
        result = self.run_release_step(exists=True, release_state=state)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        calls = self.calls.read_text().splitlines()
        uploads = [call for call in calls if call.startswith("release upload ")]
        self.assertEqual(len(uploads), 1, calls)
        self.assertIn(self.assets[1], uploads[0])
        self.assertNotIn(self.assets[0], uploads[0])
        self.assertNotIn(self.assets[2], uploads[0])
        self.assertIn("release edit v1.2.3 --draft=false", calls)
        self.assertFalse(any(call.startswith("release create ") for call in calls))

    def test_complete_published_release_is_left_untouched(self):
        state = "false\t" + "\t".join(self.assets)
        result = self.run_release_step(exists=True, release_state=state)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        calls = self.calls.read_text().splitlines()
        self.assertFalse(any(call.startswith("release upload ") for call in calls))
        self.assertFalse(any(call.startswith("release edit ") for call in calls))
        self.assertFalse(any(call.startswith("release create ") for call in calls))


if __name__ == "__main__":
    unittest.main()
