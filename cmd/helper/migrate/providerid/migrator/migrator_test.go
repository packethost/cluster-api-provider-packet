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

package migrator_test

import (
	"context"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base/testutils"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/migrate/providerid/migrator"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	oldPrefix = "packet://"
	newPrefix = "equinixmetal://"
)

func TestMigrator_CheckPrerequisitesManagement(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()
	mgmtNamespace := util.RandomString(16)

	happyCluster := testutils.GenerateCluster("", "HappyCluster")
	sadWithCCM := testutils.GenerateCluster("", "SadWithCCM")
	sadWithOldCPEM := testutils.GenerateCluster("", "SadWihtOldCPEM")
	sadNoCCMOrCPEM := testutils.GenerateCluster("", "SadNoCCMOrCPEM")

	clusters := []runtime.Object{
		happyCluster,
		sadWithCCM,
		sadWithOldCPEM,
		sadNoCCMOrCPEM,
	}

	workloadResources := map[client.ObjectKey][]runtime.Object{
		{Namespace: happyCluster.Namespace, Name: happyCluster.Name}: {
			testutils.GenerateDeployment(metav1.NamespaceSystem, migrator.CPEMDeploymentName, "cpem:"+migrator.CPEMMinVersion),
		},
		{Namespace: sadWithCCM.Namespace, Name: sadWithCCM.Name}: {
			testutils.GenerateDeployment(metav1.NamespaceSystem, migrator.CCMDeploymentName, "ccm:latest"),
		},
		{Namespace: sadWithOldCPEM.Namespace, Name: sadWithOldCPEM.Name}: {
			testutils.GenerateDeployment(metav1.NamespaceSystem, migrator.CPEMDeploymentName, "cpem:3.0.0"),
		},
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, workloadResources, clusters...)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		TargetNamespace:      mgmtNamespace,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	m, err := migrator.New(ctx, toolConfig)
	g.Expect(err).NotTo(HaveOccurred())

	// If mgmt cluster is missing capp, then do not expect to be able to proceed
	g.Expect(m.CheckPrerequisites(ctx)).To(MatchError(migrator.ErrMissingCAPPDeployment))

	imageBaseName := "docker.io/packethost/cluster-api-provider-packet"
	cappDeployment := testutils.GenerateDeployment(mgmtNamespace, migrator.CAPPDeploymentName, imageBaseName+":v0.3.9")
	g.Expect(fakeEnv.MgmtClient.Create(ctx, cappDeployment.DeepCopy())).To(Succeed())

	// Verify that pre-requisites fail when CAPP is too old
	g.Expect(m.CheckPrerequisites(ctx)).To(MatchError(migrator.ErrCAPPTooOld))

	// Verify that pre-requisites get further with latest tag
	cappDeployment.Spec.Template.Spec.Containers[0].Image = imageBaseName
	g.Expect(fakeEnv.MgmtClient.Update(ctx, cappDeployment.DeepCopy())).To(Succeed())

	g.Expect(m.CheckPrerequisites(ctx)).To(Succeed())

	// Verify that pre-requisites get further with the minimum version tag
	cappDeployment.Spec.Template.Spec.Containers[0].Image = imageBaseName + ":v" + migrator.CAPPMinVersion
	g.Expect(fakeEnv.MgmtClient.Update(ctx, cappDeployment.DeepCopy())).To(Succeed())

	g.Expect(m.CheckPrerequisites(ctx)).To(Succeed())

	g.Expect(m.GetErrorFor(happyCluster)).NotTo(HaveOccurred())
	g.Expect(m.GetErrorFor(sadWithCCM)).To(MatchError(migrator.ErrPacketCloudProviderFound))
	g.Expect(m.GetErrorFor(sadWithOldCPEM)).To(MatchError(migrator.ErrCPEMTooOld))
	g.Expect(m.GetErrorFor(sadNoCCMOrCPEM)).To(MatchError(migrator.ErrMissingCPEMDeployment))
}

func TestMigrator_CalculatePercentage(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	numClusters := 10 + rand.Intn(41) //nolint:gosec
	clusters := make([]runtime.Object, 0, numClusters)
	clusterNodes := make(map[client.ObjectKey][]runtime.Object, numClusters)
	totalNodes := 0

	for i := 0; i < numClusters; i++ {
		c := testutils.GenerateCluster("", "")
		clusters = append(clusters, c)
		numNodes := 10 + rand.Intn(41) //nolint:gosec
		totalNodes += numNodes
		nodes := make([]runtime.Object, 0, numNodes)

		for i := 0; i < numNodes; i++ {
			nodes = append(nodes, testutils.GenerateNode("", ""))
		}

		clusterKey, err := client.ObjectKeyFromObject(c)
		g.Expect(err).NotTo(HaveOccurred())

		clusterNodes[clusterKey] = nodes
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, clusterNodes, clusters...)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	m, err := migrator.New(ctx, toolConfig)
	g.Expect(err).NotTo(HaveOccurred())

	// Before any migrations have been done, the percentage should be 0%
	expectedPercent := 0.0
	g.Expect(m.CalculatePercentage()).To(BeNumerically("~", expectedPercent))

	for _, c := range clusters {
		clusterKey, err := client.ObjectKeyFromObject(c)
		g.Expect(err).NotTo(HaveOccurred())

		expectedPercentForCluster := float64(len(clusterNodes[clusterKey])) / float64(totalNodes)
		expectedPercent += expectedPercentForCluster

		m.MigrateWorkloadCluster(ctx, c.(*clusterv1.Cluster))

		g.Expect(m.CalculatePercentage()).To(BeNumerically("~", expectedPercent))
	}
}

func TestMigrator_MigrateNode(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()
	providerIDSuffix := util.RandomString(24)

	cluster := testutils.GenerateCluster("", "")
	clusterKey := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}

	node := testutils.GenerateNode("", oldPrefix+providerIDSuffix)
	clusterNodes := map[client.ObjectKey][]runtime.Object{
		clusterKey: {node},
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, clusterNodes, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	m, err := migrator.New(ctx, toolConfig)
	g.Expect(err).NotTo(HaveOccurred())

	expectedNode := node.DeepCopy()
	expectedNode.Spec.ProviderID = newPrefix + providerIDSuffix

	postMigrateNode := node.DeepCopy()
	preMigrateOutput := m.GetOutputFor(cluster)
	g.Expect(m.MigrateNode(ctx, postMigrateNode, cluster)).To(Succeed())
	postMigrateOutput := m.GetOutputFor(cluster)

	g.Expect(postMigrateNode).To(testutils.BeDerivativeOf(expectedNode))

	postMigrateOutputDiff := strings.TrimPrefix(postMigrateOutput, preMigrateOutput)
	postMigrateOutputLines := strings.Split(postMigrateOutputDiff, "\n")
	g.Expect(postMigrateOutputLines).To(HaveLen(3))
	testutils.VerifySuccessOutputChanged(t, postMigrateOutputLines[0])
	testutils.VerifySuccessOutputChanged(t, postMigrateOutputLines[1])

	actualPostMigrateNode := new(corev1.Node)
	nodeKey := client.ObjectKey{Name: node.Name} //nolint:exhaustivestruct
	g.Expect(m.WorkloadGet(ctx, cluster, nodeKey, actualPostMigrateNode)).To(Succeed())
	g.Expect(actualPostMigrateNode).To(testutils.BeDerivativeOf(expectedNode))
}

// Dry Run on Deletion is not handled properly with the fake client, so
// we need to test dry run using a real client.
func TestMigrator_MigrateNodeDry(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()
	providerIDSuffix := util.RandomString(24)

	cluster := testutils.GenerateCluster("", "")
	clusterKey := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}

	node := testutils.GenerateNode("", oldPrefix+providerIDSuffix)
	clusterNodes := map[client.ObjectKey][]runtime.Object{
		clusterKey: {node},
	}

	testEnv := testutils.NewTestEnv(ctx, t, clusterNodes, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		DryRun:               true,
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	m, err := migrator.New(ctx, toolConfig)
	g.Expect(err).NotTo(HaveOccurred())

	expectedNode := node.DeepCopy()
	expectedNode.Spec.ProviderID = newPrefix + providerIDSuffix

	postDryRunNode := node.DeepCopy()
	preDryRunOutput := m.GetOutputFor(cluster)
	g.Expect(m.MigrateNode(ctx, postDryRunNode, cluster)).To(Succeed())
	postDryRunOutput := m.GetOutputFor(cluster)

	g.Expect(postDryRunNode).To(testutils.BeDerivativeOf(expectedNode))

	postDryRunOutputDiff := strings.TrimPrefix(postDryRunOutput, preDryRunOutput)
	g.Expect(postDryRunOutputDiff).To(MatchRegexp("^(%s.*\n){2}?", regexp.QuoteMeta(base.DryRunPrefix)))

	actualPostDryRunNode := new(corev1.Node)
	nodeKey := client.ObjectKey{Name: node.Name} //nolint:exhaustivestruct
	g.Expect(m.WorkloadGet(ctx, cluster, nodeKey, actualPostDryRunNode)).To(Succeed())
	g.Expect(actualPostDryRunNode).To(testutils.BeDerivativeOf(node))
}

func TestMigrator_MigrateNodeAlreadyMigrated(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()
	providerIDSuffix := util.RandomString(24)

	cluster := testutils.GenerateCluster("", "")
	clusterKey := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}

	expectedNode := testutils.GenerateNode("", newPrefix+providerIDSuffix)
	clusterNodes := map[client.ObjectKey][]runtime.Object{
		clusterKey: {expectedNode},
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, clusterNodes, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	m, err := migrator.New(ctx, toolConfig)
	g.Expect(err).NotTo(HaveOccurred())

	preMigrateOutput := m.GetOutputFor(cluster)
	g.Expect(m.MigrateNode(ctx, expectedNode.DeepCopy(), cluster)).To(Succeed())
	postMigrateOutput := m.GetOutputFor(cluster)

	postMigrateOutputDiff := strings.TrimPrefix(postMigrateOutput, preMigrateOutput)
	testutils.VerifySuccessOutputUnchanged(t, postMigrateOutputDiff)

	actualPostMigrateNode := new(corev1.Node)
	nodeKey := client.ObjectKey{Name: expectedNode.Name} //nolint:exhaustivestruct
	g.Expect(m.WorkloadGet(ctx, cluster, nodeKey, actualPostMigrateNode)).To(Succeed())
	g.Expect(actualPostMigrateNode).To(testutils.BeDerivativeOf(expectedNode))
}
