# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# See
# https://docs.docker.com/engine/reference/builder/#automatic-platform-args-in-the-global-scope
# for info on BUILDPLATFORM, TARGETOS, TARGETARCH, etc.
FROM --platform=$BUILDPLATFORM golang:1.20 AS builder
WORKDIR /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver
COPY go.* .
ARG GOPROXY
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION

RUN apt-get update && apt-get install -y libcryptsetup-dev

RUN OS=$TARGETOS ARCH=$TARGETARCH make $TARGETOS/$TARGETARCH

# Driver image
FROM debian:bullseye AS linux-amazon

RUN apt-get update && apt-get install -y libcryptsetup-dev

COPY --from=builder /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver/bin/aws-ebs-csi-driver /bin/aws-ebs-csi-driver

ENTRYPOINT ["/bin/aws-ebs-csi-driver"]
