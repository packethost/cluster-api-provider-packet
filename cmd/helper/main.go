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

package main

import (
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/migrate"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/upgrade"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	var logFile string

	config := new(base.ToolConfig)
	config.Logger = klogr.New()
	klogFlags := new(flag.FlagSet)
	klog.InitFlags(klogFlags)
	klogFlags.Set("logtostderr", "false")
	klogFlags.Set("stderrthreshold", "FATAL")

	rootCmd := &cobra.Command{ //nolint:exhaustivestruct
		Use:          "capp-helper",
		Short:        "Helper utilties for CAPP",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			klogFlags.Set("log_file", logFile)

			// log to stderr if NoTUI is set or we are not running in a tty
			if !isatty.IsTerminal(os.Stdout.Fd()) && !config.NoTUI {
				config.NoTUI = true
			}

			if config.NoTUI {
				klogFlags.Set("alsologtostderr", "true")
			}

			klogFlags.Parse(nil)

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help() //nolint:wrapcheck
		},
	}

	rootCmd.PersistentFlags().StringVar(&config.Kubeconfig, "kubeconfig", "",
		"Path to the kubeconfig for the management cluster. If unspecified, default discovery rules apply.")
	rootCmd.PersistentFlags().StringVar(&config.Context, "kubeconfig-context", "",
		"Context to be used within the kubeconfig file. If empty, current context will be used.")
	rootCmd.PersistentFlags().StringVar(&config.TargetNamespace, "target-namespace", base.DefaultTargetNamespace,
		"The namespace where cluster-api-provider-packet is deployed.")
	rootCmd.PersistentFlags().StringVar(&config.WatchingNamespace, "watching-namespace", base.DefaultWatchingNamespace,
		"The namespace that the packet provider is configured to watch.")
	rootCmd.PersistentFlags().BoolVar(&config.DryRun, "dry-run", false, "Dry run.")
	rootCmd.PersistentFlags().BoolVar(&config.NoTUI, "no-tui", false, "Do not run TUI.")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "output.log",
		"Logfile to use.")

	rootCmd.AddCommand((&migrate.Command{ToolConfig: config}).Command())
	rootCmd.AddCommand((&upgrade.Command{ToolConfig: config}).Command())

	if err := rootCmd.Execute(); err != nil {
		flushLogs(config)
		os.Exit(1)
	}

	flushLogs(config)
}

func flushLogs(config *base.ToolConfig) {
	klog.Flush()
}
