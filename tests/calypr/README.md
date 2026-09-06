# Run the Calypr integration test

This test deploys a small Calypr installation in a disposable kind cluster. It runs the pull request's Syfon image behind Calypr revproxy. Real Fence tokens and Arborist policies authorize the requests.

The test runs git-drs's pinned remote Gen3 test in a Kubernetes Job against the
fixture's Gogs and MinIO services. It uploads a tiny single-part file and a
6 MiB multipart file, then verifies both through a fresh clone. Later Jobs
check project isolation, read-only access, expired identity, signed URLs,
private storage, and Syfon restart persistence.

## Run the complete test

Install Docker, kind, kubectl, Helm, Git, jq, and OpenSSL. The Docker server must
use amd64 or arm64. The runner fails fast on other architectures.

Then run:

```sh
tests/calypr/run.sh all
```

The command builds `syfon-calypr:ci` from the current checkout. It builds git-drs from the pinned commit on the `development` branch. The runner creates a cluster, runs every scenario, writes artifacts, and deletes the cluster.

The disposable PostgreSQL pod provisions the Fence and Arborist roles and databases directly. Helm database creation is disabled and the generated credential secrets are marked ready after install, so this fixture does not pull the gen3-helm `awshelper` image.

Set `CALYPR_KEEP_CLUSTER=1` to retain a failed cluster:

```sh
CALYPR_KEEP_CLUSTER=1 tests/calypr/run.sh all
```

The final log prints the kubeconfig path. Use that exact file for inspection. The runner never changes your current Kubernetes context.

## Run one phase

Use the same `CALYPR_RUN_ID` for each command:

```sh
export CALYPR_RUN_ID=debug
tests/calypr/run.sh up
tests/calypr/run.sh test
tests/calypr/run.sh diagnose
tests/calypr/run.sh down
```

`up` creates the cluster, deploys the services, runs database migrations and Fence usersync, creates the Gogs fixture, and issues tokens. `test` runs the scenarios in dependency order. `diagnose` collects pod state, events, and service logs. `down` deletes only the kind cluster named in this run.

Artifacts are written to `${TMPDIR:-/tmp}/syfon-calypr-$CALYPR_RUN_ID/artifacts`. The result file lists every required scenario. A missing PASS marker fails the test even when its Kubernetes Job exits successfully.

## Update pinned components

Edit `values-ci.yaml` to update Fence, Arborist, revproxy, or Syfon image tags. Infrastructure image tags live beside their resources in `infrastructure.yaml` and `scenario-jobs.yaml`. Edit `versions.env` when updating a source branch or commit, and keep each named branch and commit together. The required Calypr components are:

- `calypr/gen3-helm` from `ohsu-develop`
- `calypr/git-drs` from `development`
- `quay.io/ohsu-comp-bio/fence:ci_native-multiarch-images`, built from
  `calypr/fence` `ci/native-multiarch-images`
- `quay.io/ohsu-comp-bio/arborist:master`, built from `calypr/arborist` `master`
- the Syfon image built from the current checkout

The Fence and Arborist image tags are intentionally mutable because they match Calypr's deployment convention. `versions.env` records the source commits that were compatible when this fixture was updated. The result and image artifacts record the image names and runtime image IDs that Kubernetes actually ran.

## Read a failure

The runner names each failure stage in the log. Start with the matching artifact:

- `pods.txt` and `events.txt` show scheduling, image, and readiness failures.
- `job-useryaml.log` shows identity and policy synchronization failures.
- `calypr-git-drs.log` and `calypr-reader.log` show git-drs transfer failures.
- `deployment-fence-deployment.log`, `deployment-arborist-deployment.log`, and `deployment-syfon.log` show service failures.
- `images.json` records the image names and runtime image IDs.
- `timings.tsv` shows where the run spent time.

Artifacts exclude Kubernetes Secrets, tokens, private keys, and full signed URLs. Set `CALYPR_KEEP_CLUSTER=1` when logs do not contain enough evidence. Then inspect the retained namespace with the printed kubeconfig.
