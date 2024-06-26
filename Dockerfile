# Copyright (c) Edgeless Systems GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# This file incorporates work covered by the following copyright and
# permission notice:
#
#
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

RUN apt-get update && apt-get install -y wget libcryptsetup-dev build-essential tar git pkgconf
RUN wget https://golang.org/dl/go1.22.3.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.22.3.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

WORKDIR /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver
COPY go.* .
ARG GOPROXY
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION

RUN OS=$TARGETOS ARCH=$TARGETARCH make

# Driver image
FROM registry.k8s.io/build-image/debian-base:bullseye-v1.4.3 AS linux-amazon

RUN apt-get update && apt-get install -y util-linux e2fsprogs mount ca-certificates udev xfsprogs btrfs-progs libcryptsetup-dev

COPY --from=builder /go/src/github.com/kubernetes-sigs/aws-ebs-csi-driver/bin/aws-ebs-csi-driver /bin/aws-ebs-csi-driver

ENTRYPOINT ["/bin/aws-ebs-csi-driver"]
