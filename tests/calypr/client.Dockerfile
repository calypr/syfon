FROM golang:1.26.6-alpine AS build

ARG GIT_DRS_REPOSITORY=https://github.com/calypr/git-drs.git
ARG GIT_DRS_BRANCH=development
ARG GIT_DRS_COMMIT
RUN apk add --no-cache git build-base
WORKDIR /src
RUN git init \
    && git remote add origin "${GIT_DRS_REPOSITORY}" \
    && git fetch --depth 1 origin "${GIT_DRS_COMMIT}" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "${GIT_DRS_COMMIT}" \
    && git config calypr.sourceBranch "${GIT_DRS_BRANCH}" \
    && CGO_ENABLED=1 go build -trimpath -o /out/git-drs .

FROM alpine:3.21
RUN apk add --no-cache bash ca-certificates curl git git-lfs jq openssl
COPY --from=build /out/git-drs /usr/local/bin/git-drs
COPY --from=build /src/tests/e2e-gen3-remote-full.sh /usr/local/share/git-drs/tests/e2e-gen3-remote-full.sh
COPY tests/calypr/git-drs-calypr.patch /tmp/git-drs-calypr.patch
RUN cd /usr/local/share/git-drs \
    && git apply --check /tmp/git-drs-calypr.patch \
    && git apply /tmp/git-drs-calypr.patch \
    && rm /tmp/git-drs-calypr.patch
ENTRYPOINT ["/bin/bash"]
