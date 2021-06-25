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

package base_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base/testutils"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// TODO: unstructured namespaced lifecycle
// TODO: unstructured non-namespaced lifecycle
// TODO: are test cases needed for gvk failure and getWorkloadClient failure???
// TODO: tests with targetnamespace/watchingnamespace
// TODO: maybe tests for setting kubeconfig/context, but will require a live client instead of fake client
// TODO: tests for WorkloadList
// TODO: tests for HasError/GetErrorFor/AddErrorFor
// TODO: tests for GetOutputFor/GetBufferFor/flushing of buffers to output

func TestTool_WorkloadPatchDryRunRedactsSecrets(t *testing.T) {
	t.Parallel()

	g := NewWithT(t)
	ctx := context.TODO()
	initialSecret := &corev1.Secret{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Namespace: fmt.Sprintf("test-%s", util.RandomString(6)),
			Name:      fmt.Sprintf("test-secret-%s", util.RandomString(6)),
		},
		Data: map[string][]byte{
			"color": []byte("yellow"),
			"shape": []byte("square"),
		},
	}

	cluster := testutils.GenerateCluster("", "")
	clusterKey, err := client.ObjectKeyFromObject(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	workloadResources := map[client.ObjectKey][]runtime.Object{
		clusterKey: {initialSecret},
	}

	testEnv := testutils.NewTestEnv(ctx, t, workloadResources, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		DryRun:               true,
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	patchInput := initialSecret.DeepCopy()
	patchInput.Data["size"] = []byte("large")
	unstructuredSecret := new(unstructured.Unstructured)
	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(patchInput)
	g.Expect(err).NotTo(HaveOccurred())
	unstructuredSecret.SetUnstructuredContent(unstructuredContent)
	unstructuredSecret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))

	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, cluster, unstructuredSecret)).To(Succeed())

	output := tool.GetOutputFor(cluster)

	for _, value := range patchInput.Data {
		g.Expect(output).NotTo(ContainSubstring(string(value)))
	}
}

func TestTool_WorkloadCreateDryRunRedactsSecrets(t *testing.T) {
	t.Parallel()

	g := NewWithT(t)
	ctx := context.TODO()
	secret := &corev1.Secret{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Namespace: fmt.Sprintf("test-%s", util.RandomString(6)),
			Name:      fmt.Sprintf("test-secret-%s", util.RandomString(6)),
		},
		Data: map[string][]byte{
			"color": []byte("yellow"),
			"shape": []byte("square"),
		},
	}

	cluster := testutils.GenerateCluster("", "")

	testEnv := testutils.NewTestEnv(ctx, t, nil, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		DryRun:               true,
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, cluster, secret)).To(Succeed())
	output := tool.GetOutputFor(cluster)

	for _, value := range secret.Data {
		g.Expect(output).NotTo(ContainSubstring(string(value)))
	}

	unstructuredSecret := new(unstructured.Unstructured)
	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(secret)
	g.Expect(err).NotTo(HaveOccurred())
	unstructuredSecret.SetUnstructuredContent(unstructuredContent)
	unstructuredSecret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))

	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, cluster, unstructuredSecret)).To(Succeed())
	output = strings.TrimPrefix(tool.GetOutputFor(cluster), output)

	for _, value := range secret.Data {
		g.Expect(output).NotTo(ContainSubstring(string(value)))
	}
}

func TestTool_WorkloadPatchOrCreateUnstructured(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	expectedData := map[string]interface{}{
		"color": base64.StdEncoding.EncodeToString([]byte("red")),
	}
	expectedResource := new(unstructured.Unstructured)
	expectedResource.SetUnstructuredContent(map[string]interface{}{"data": expectedData})
	expectedResource.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	expectedResource.SetNamespace(fmt.Sprintf("test-%s", util.RandomString(6)))
	expectedResource.SetName(fmt.Sprintf("test-secret-%s", util.RandomString(6)))

	preMutatedData := map[string]string{
		"color": base64.StdEncoding.EncodeToString([]byte("purple")),
	}
	preMutatedResource := expectedResource.DeepCopy()
	g.Expect(unstructured.SetNestedStringMap(preMutatedResource.UnstructuredContent(), preMutatedData, "data")).
		To(Succeed())

	clusterWithoutResource := testutils.GenerateCluster("", "withoutResource")
	clusterWithResource := testutils.GenerateCluster("", "withResource")
	clusterWithResourceDiff := testutils.GenerateCluster("", "withResourceDiff")

	workloadResources := map[client.ObjectKey][]runtime.Object{
		{Namespace: clusterWithResource.Namespace, Name: clusterWithResource.Name}: {
			expectedResource.DeepCopy(),
		},
		{Namespace: clusterWithResourceDiff.Namespace, Name: clusterWithResourceDiff.Name}: {
			preMutatedResource.DeepCopy(),
		},
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, workloadResources, clusterWithResource,
		clusterWithResourceDiff, clusterWithoutResource)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}

	tool := &base.Tool{}
	tool.Configure(toolConfig)

	// Test Create
	preCreateOutput := tool.GetOutputFor(clusterWithoutResource)
	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, clusterWithoutResource,
		expectedResource.DeepCopy())).To(Succeed())

	postCreateOutput := tool.GetOutputFor(clusterWithoutResource)
	testutils.VerifySuccessOutputChanged(t, strings.TrimPrefix(postCreateOutput, preCreateOutput))

	expectedResourceKey, err := client.ObjectKeyFromObject(expectedResource)
	g.Expect(err).NotTo(HaveOccurred())

	actualPostCreate := expectedResource.NewEmptyInstance()
	g.Expect(tool.WorkloadGet(ctx, clusterWithoutResource, expectedResourceKey, actualPostCreate)).To(Succeed())
	g.Expect(actualPostCreate).To(testutils.BeDerivativeOf(expectedResource))

	// Test Noop on unchanged
	preNoopOutput := tool.GetOutputFor(clusterWithResource)
	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, clusterWithResource,
		expectedResource.DeepCopy())).To(Succeed())

	postNoopOutput := tool.GetOutputFor(clusterWithResource)
	testutils.VerifySuccessOutputUnchanged(t, strings.TrimPrefix(postNoopOutput, preNoopOutput))

	actualNoop := expectedResource.NewEmptyInstance()
	g.Expect(tool.WorkloadGet(ctx, clusterWithResource, expectedResourceKey, actualNoop)).To(Succeed())
	g.Expect(actualNoop).To(testutils.BeDerivativeOf(expectedResource))

	// Test Modify
	preMutateOutput := tool.GetOutputFor(clusterWithResourceDiff)
	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, clusterWithResourceDiff,
		expectedResource.DeepCopy())).To(Succeed())

	postMutateOutput := tool.GetOutputFor(clusterWithResourceDiff)
	testutils.VerifySuccessOutputChanged(t, strings.TrimPrefix(postMutateOutput, preMutateOutput))

	actualMutate := expectedResource.NewEmptyInstance()
	g.Expect(tool.WorkloadGet(ctx, clusterWithResourceDiff, expectedResourceKey, actualMutate)).To(Succeed())
	g.Expect(actualMutate).To(testutils.BeDerivativeOf(expectedResource))
	g.Expect(actualMutate).NotTo(testutils.BeDerivativeOf(preMutatedResource))
}

func TestTool_WorkloadPatchOrCreateUnstructuredDry(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	expectedData := map[string]interface{}{
		"color": base64.StdEncoding.EncodeToString([]byte("red")),
	}
	expectedResource := new(unstructured.Unstructured)
	expectedResource.SetUnstructuredContent(map[string]interface{}{"data": expectedData})
	expectedResource.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	expectedResource.SetNamespace(fmt.Sprintf("test-%s", util.RandomString(6)))
	expectedResource.SetName(fmt.Sprintf("test-secret-%s", util.RandomString(6)))

	preMutatedData := map[string]string{
		"color": base64.StdEncoding.EncodeToString([]byte("purple")),
	}
	preMutatedResource := expectedResource.DeepCopy()
	g.Expect(unstructured.SetNestedStringMap(preMutatedResource.UnstructuredContent(), preMutatedData, "data")).
		To(Succeed())

	clusterWithoutResource := testutils.GenerateCluster("", "withoutresource")
	clusterWithResourceDiff := testutils.GenerateCluster("", "withdiff")

	workloadResources := map[client.ObjectKey][]runtime.Object{
		{Namespace: clusterWithResourceDiff.Namespace, Name: clusterWithResourceDiff.Name}: {
			preMutatedResource.DeepCopy(),
		},
	}

	testEnv := testutils.NewTestEnv(ctx, t, workloadResources,
		clusterWithResourceDiff, clusterWithoutResource)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		DryRun:               true,
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}

	tool := &base.Tool{}
	tool.Configure(toolConfig)

	// Test Dry Run Create
	preDryRunOutput := tool.GetOutputFor(clusterWithoutResource)
	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, clusterWithoutResource,
		expectedResource.DeepCopy())).To(Succeed())

	postDryRunOutput := tool.GetOutputFor(clusterWithoutResource)
	testutils.VerifySuccessOutputDryRun(t, strings.TrimPrefix(postDryRunOutput, preDryRunOutput))

	expectedResourceKey, err := client.ObjectKeyFromObject(expectedResource)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(tool.WorkloadGet(ctx, clusterWithoutResource, expectedResourceKey, expectedResource.NewEmptyInstance())).
		To(MatchError(ContainSubstring("not found")))

	// Test Dry Run Modify
	preDryRunMutateOutput := tool.GetOutputFor(clusterWithResourceDiff)
	g.Expect(tool.WorkloadPatchOrCreateUnstructured(ctx, toolConfig.Logger, clusterWithResourceDiff,
		expectedResource.DeepCopy())).To(Succeed())

	postDryRunMutateOutput := tool.GetOutputFor(clusterWithResourceDiff)
	testutils.VerifySuccessOutputDryRun(t, strings.TrimPrefix(postDryRunMutateOutput, preDryRunMutateOutput))

	actualDryRunMutate := expectedResource.NewEmptyInstance()
	g.Expect(tool.WorkloadGet(ctx, clusterWithResourceDiff, expectedResourceKey, actualDryRunMutate)).To(Succeed())
	g.Expect(actualDryRunMutate).To(testutils.BeDerivativeOf(preMutatedResource))
	g.Expect(actualDryRunMutate).NotTo(testutils.BeDerivativeOf(expectedResource))
}

func TestTool_TestGetClustersNone(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	fakeEnv := testutils.NewFakeEnv(ctx, t, nil)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	res, err := tool.GetClusters(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(res).To(BeEmpty())
}

func TestTool_TestGetClustersAll(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	// generate a random number of namespaces between 3 and 10
	numNamespaces := 3 + rand.Intn(8) //nolint:gosec

	var testClusters []runtime.Object

	for i := 0; i < numNamespaces; i++ {
		namespace := util.RandomString(6)

		// generate a random number of clusters between 1 and 10
		for j := 0; j < rand.Intn(10)+1; j++ { //nolint:gosec
			testClusters = append(testClusters, testutils.GenerateCluster(namespace, ""))
		}
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, nil, testClusters...)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	allClusters, err := tool.GetClusters(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(allClusters).To(HaveLen(len(testClusters)))
}

func TestTool_TestGetClustersFiltered(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	// generate a random number of namespaces between 3 and 10
	numNamespaces := 3 + rand.Intn(8) //nolint:gosec
	namespaces := make([]string, 0, numNamespaces)

	var testClusters []runtime.Object

	for i := 0; i < numNamespaces; i++ {
		namespace := util.RandomString(6)
		namespaces = append(namespaces, namespace)

		// generate a random number of clusters between 1 and 10
		for j := 0; j < rand.Intn(10)+1; j++ { //nolint:gosec
			testClusters = append(testClusters, testutils.GenerateCluster(namespace, ""))
		}
	}

	namespaceToFilterOn := namespaces[rand.Intn(len(namespaces))] //nolint:gosec

	var expectedClusterNames []string

	for _, c := range testClusters {
		cluster, _ := c.(controllerutil.Object)
		if cluster.GetNamespace() == namespaceToFilterOn {
			expectedClusterNames = append(expectedClusterNames, cluster.GetName())
		}
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, nil, testClusters...)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		WatchingNamespace:    namespaceToFilterOn,
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	filteredClusters, err := tool.GetClusters(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(filteredClusters).To(HaveLen(len(expectedClusterNames)))

	for _, c := range filteredClusters {
		g.Expect(c.Namespace).To(BeEquivalentTo(namespaceToFilterOn))
		g.Expect(c.Name).To(BeElementOf(expectedClusterNames))
	}
}

func TestTool_ManagementGet(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := context.TODO()

	testNamespace := fmt.Sprintf("test-%s", util.RandomString(6))
	typedNamespacedResourceName := fmt.Sprintf("test-deployment-%s", util.RandomString(6))
	typedNamespacedResource := testutils.GenerateDeployment(testNamespace, typedNamespacedResourceName, "test")

	typedResourceName := fmt.Sprintf("test-node-%s", util.RandomString(6))
	typedResource := testutils.GenerateNode(typedResourceName, "")

	managementResources := []runtime.Object{
		typedNamespacedResource,
		typedResource,
	}

	fakeEnv := testutils.NewFakeEnv(ctx, t, nil, managementResources...)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		MgmtClient:           fakeEnv.MgmtClient,
		WorkloadClientGetter: fakeEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	typedNamespacedResourceKey, err := client.ObjectKeyFromObject(typedNamespacedResource)
	g.Expect(err).NotTo(HaveOccurred())

	// Ensure that the resource is returned
	typedNamespacedRes := new(appsv1.Deployment)
	g.Expect(tool.ManagementGet(ctx, typedNamespacedResourceKey, typedNamespacedRes)).To(Succeed())
	g.Expect(typedNamespacedRes).To(testutils.BeDerivativeOf(typedNamespacedResource))

	typedResourceKey, err := client.ObjectKeyFromObject(typedResource)
	g.Expect(err).NotTo(HaveOccurred())

	// Ensure that the resource is returned
	typedRes := new(corev1.Node)
	g.Expect(tool.ManagementGet(ctx, typedResourceKey, typedRes)).To(Succeed())
	g.Expect(typedRes).To(testutils.BeDerivativeOf(typedResource))
}

func TestTool_TestTypedNamespacedWorkloadLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	testNamespace := fmt.Sprintf("test-%s", util.RandomString(6))
	testName := fmt.Sprintf("test-secret-%s", util.RandomString(6))
	initial := &corev1.Secret{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Namespace: testNamespace,
			Name:      testName,
		},
		Data: map[string][]byte{
			"color": []byte("yellow"),
			"shape": []byte("square"),
		},
	}
	patchInput := initial.DeepCopy()
	patchInput.Data["size"] = []byte("large")

	testLifecycle(ctx, t, initial, patchInput)
	testLifecycleDry(ctx, t, initial, patchInput)
}

func TestTool_TestTypedNonNamespacedWorkloadLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	testName := fmt.Sprintf("test-node-%s", util.RandomString(6))
	initial := &corev1.Node{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Name: testName,
		},
		Spec: corev1.NodeSpec{}, // nolint:exhaustivestruct
	}
	patchInput := initial.DeepCopy()
	patchInput.Spec.Unschedulable = true

	testLifecycle(ctx, t, initial, patchInput)
	testLifecycleDry(ctx, t, initial, patchInput)
}

func testLifecycle(ctx context.Context, t *testing.T, initial, patchInput controllerutil.Object) {
	g := NewWithT(t)
	resourceType := reflect.TypeOf(initial).Elem()
	cluster := testutils.GenerateCluster("", "")
	resourceKey, err := client.ObjectKeyFromObject(initial)
	g.Expect(err).NotTo(HaveOccurred())

	testEnv := testutils.NewTestEnv(ctx, t, nil, cluster)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	// Ensure that the resource doesn't already exist
	g.Expect(tool.WorkloadGet(
		ctx,
		cluster,
		resourceKey,
		reflect.New(resourceType).Interface().(controllerutil.Object),
	)).To(MatchError(ContainSubstring("not found")))

	preCreate, _ := initial.DeepCopyObject().(controllerutil.Object)
	preCreateOutput := tool.GetOutputFor(cluster)

	// verify deletion of non-existing resource acts as expected
	g.Expect(tool.WorkloadDelete(ctx, toolConfig.Logger, cluster, preCreate.DeepCopyObject().(controllerutil.Object))).
		To(MatchError(ContainSubstring("not found")))

	// verify real create
	postCreate, _ := preCreate.DeepCopyObject().(controllerutil.Object)
	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, cluster, postCreate)).To(Succeed())
	g.Expect(postCreate).To(testutils.BeDerivativeOf(preCreate))

	postCreateOutput := tool.GetOutputFor(cluster)
	testutils.VerifySuccessOutputChanged(t, strings.TrimPrefix(postCreateOutput, preCreateOutput))

	// Ensure that the resource now exists
	actualPostCreate, _ := reflect.New(resourceType).Interface().(controllerutil.Object)
	g.Expect(tool.WorkloadGet(ctx, cluster, resourceKey, actualPostCreate)).To(Succeed())
	g.Expect(actualPostCreate).To(testutils.BeDerivativeOf(preCreate))

	// verify create of an already existing resource fails
	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, cluster, preCreate.DeepCopyObject().(controllerutil.Object))).
		To(MatchError(ContainSubstring("already exists")))

	preDelete, _ := postCreate.DeepCopyObject().(controllerutil.Object)
	preDelete.SetCreationTimestamp(metav1.NewTime(time.Time{}))

	// verify real delete
	g.Expect(tool.WorkloadDelete(ctx, toolConfig.Logger, cluster,
		preDelete.DeepCopyObject().(controllerutil.Object))).To(Succeed())

	postDeleteOutput := tool.GetOutputFor(cluster)
	testutils.VerifySuccessOutputChanged(t, strings.TrimPrefix(postDeleteOutput, postCreateOutput))

	// ensure that the resource no longer exists
	g.Expect(tool.WorkloadGet(
		ctx,
		cluster,
		resourceKey,
		reflect.New(resourceType).Interface().(controllerutil.Object),
	)).To(MatchError(ContainSubstring("not found")))
}

func testLifecycleDry(ctx context.Context, t *testing.T, initial, patchInput controllerutil.Object) {
	g := NewWithT(t)
	resourceType := reflect.TypeOf(initial).Elem()
	clusterWith := testutils.GenerateCluster("", "with")
	clusterWithKey, err := client.ObjectKeyFromObject(clusterWith)
	g.Expect(err).NotTo(HaveOccurred())

	clusterWithout := testutils.GenerateCluster("", "without")
	resourceKey, err := client.ObjectKeyFromObject(initial)
	g.Expect(err).NotTo(HaveOccurred())

	workloadResources := map[client.ObjectKey][]runtime.Object{
		clusterWithKey: {initial},
	}

	testEnv := testutils.NewTestEnv(ctx, t, workloadResources, clusterWith, clusterWithout)
	toolConfig := &base.ToolConfig{ //nolint:exhaustivestruct
		DryRun:               true,
		RestConfig:           testEnv.RestConfig,
		WorkloadClientGetter: testEnv.WorkloadClientGetter,
		Logger:               klogr.New(),
	}
	tool := &base.Tool{}
	tool.Configure(toolConfig)

	// verify dry-run deletion of non-existing resource acts as expected
	g.Expect(tool.WorkloadDelete(ctx, toolConfig.Logger, clusterWithout,
		initial.DeepCopyObject().(controllerutil.Object))).To(MatchError(ContainSubstring("not found")))

	// verify dry-run create
	preCreateOutput := tool.GetOutputFor(clusterWithout)
	postDryCreate, _ := initial.DeepCopyObject().(controllerutil.Object)
	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, clusterWithout, postDryCreate)).To(Succeed())
	postCreateOutput := tool.GetOutputFor(clusterWithout)

	g.Expect(postDryCreate).To(testutils.BeDerivativeOf(initial))
	testutils.VerifySuccessOutputDryRun(t, strings.TrimPrefix(postCreateOutput, preCreateOutput))

	// Ensure that the resource still doesn't exist
	g.Expect(tool.WorkloadGet(
		ctx,
		clusterWithout,
		resourceKey,
		reflect.New(resourceType).Interface().(controllerutil.Object),
	)).To(MatchError(ContainSubstring("not found")))

	// verify dry run create of an already existing resource fails
	g.Expect(tool.WorkloadCreate(ctx, toolConfig.Logger, clusterWith, initial.DeepCopyObject().(controllerutil.Object))).
		To(MatchError(ContainSubstring("already exists")))

	// verify dry-run delete
	preDryDeleteOutput := tool.GetOutputFor(clusterWith)
	g.Expect(tool.WorkloadDelete(ctx, toolConfig.Logger, clusterWith,
		initial.DeepCopyObject().(controllerutil.Object))).To(Succeed())

	postDryDeleteOutput := tool.GetOutputFor(clusterWith)
	testutils.VerifySuccessOutputDryRun(t, strings.TrimPrefix(postDryDeleteOutput, preDryDeleteOutput))

	// ensure that the resource is the same as when we started
	actualPostDryDelete, _ := reflect.New(resourceType).Interface().(controllerutil.Object)
	g.Expect(tool.WorkloadGet(ctx, clusterWith, resourceKey, actualPostDryDelete)).To(Succeed())
	g.Expect(actualPostDryDelete).To(testutils.BeDerivativeOf(initial))
}
