/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package options

import (
	flag "github.com/spf13/pflag"
)

// KMSOptions contains options and configuration settings for Constellation's Key Management Service (KMS).
type KMSOptions struct {
	// Addr is the address of the KMS.
	Addr string
}

func (o *KMSOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.Addr, "kms-addr", "kms.kube-system:9000", "Address of Constellation's KMS. Used to request keys (default: kms.kube-system:9000)")
}
