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
FROM registry.k8s.io/build-image/debian-base:bullseye-v1.4.3 AS builder
WORKDIR /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver
COPY go.* .
ARG GOPROXY
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION

RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz
RUN wget https://golang.org/dl/go1.20.5.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

RUN apt-get update && apt-get install -y libcryptsetup-dev build-essential

RUN OS=$TARGETOS ARCH=$TARGETARCH make $TARGETOS/$TARGETARCH

# Driver image
FROM registry.k8s.io/build-image/debian-base:bullseye-v1.4.3 AS linux-amazon

RUN apt-get update && apt-get install -y util-linux e2fsprogs mount ca-certificates udev xfsprogs btrfs-progs libcryptsetup-dev

COPY --from=builder /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver/bin/aws-ebs-csi-driver /bin/aws-ebs-csi-driver

ENTRYPOINT ["/bin/aws-ebs-csi-driver"]
