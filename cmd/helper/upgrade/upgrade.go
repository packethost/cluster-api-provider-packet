/*
Copyright 2021 The Kubernetes Authors.
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

package upgrade

import (
	"github.com/spf13/cobra"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/upgrade/cloudprovider"
)

type Command struct {
	ToolConfig *base.ToolConfig
}

func (c *Command) Command() *cobra.Command {
	upgradeCmd := &cobra.Command{ //nolint:exhaustivestruct
		Use:   "upgrade",
		Short: "Upgrade utilities for CAPP",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help() //nolint:wrapcheck
		},
	}

	upgradeCmd.AddCommand((&cloudprovider.Command{ToolConfig: c.ToolConfig}).Command())

	return upgradeCmd
}
