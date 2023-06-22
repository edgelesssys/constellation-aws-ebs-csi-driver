/*
Copyright (c) Edgeless Systems GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, version 3 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
