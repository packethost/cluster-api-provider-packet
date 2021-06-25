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

package testutils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	gomegatypes "github.com/onsi/gomega/types"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/remote"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake" // nolint:staticcheck
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const generatedNameLength = 6

func RegisterSchemes() {
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme.Scheme))
	utilruntime.Must(clusterv1.AddToScheme(scheme.Scheme))
}

type FakeEnv struct {
	workloadClients      map[client.ObjectKey]client.Client
	WorkloadClientGetter remote.ClusterClientGetter
	MgmtClient           client.Client
}

func NewFakeEnv(
	ctx context.Context,
	t *testing.T,
	clusterObjs map[client.ObjectKey][]runtime.Object,
	initObjs ...runtime.Object,
) *FakeEnv {
	t.Helper()

	workloadClients := make(map[client.ObjectKey]client.Client, len(clusterObjs))
	for key, objs := range clusterObjs {
		workloadClients[key] = fake.NewFakeClient(objs...)
	}

	fakeEnv := &FakeEnv{ //nolint:exhaustivestruct
		workloadClients: workloadClients,
		MgmtClient:      fake.NewFakeClient(initObjs...),
	}

	fakeEnv.WorkloadClientGetter = fakeEnv.newWorkloadClusterGetter(t)

	return fakeEnv
}

func (e *FakeEnv) newWorkloadClusterGetter(
	t *testing.T,
) func(context.Context, client.Client, types.NamespacedName, *runtime.Scheme) (client.Client, error) {
	t.Helper()

	return func(
		ctx context.Context,
		_ client.Client,
		cluster types.NamespacedName,
		scheme *runtime.Scheme,
	) (client.Client, error) {
		if e.workloadClients[cluster] == nil {
			if scheme == nil {
				e.workloadClients[cluster] = fake.NewFakeClient()
			} else {
				e.workloadClients[cluster] = fake.NewFakeClientWithScheme(scheme)
			}
		}

		return e.workloadClients[cluster], nil
	}
}

type TestEnv struct {
	env                  *envtest.Environment
	workloadEnvs         map[types.NamespacedName]*envtest.Environment
	WorkloadClientGetter remote.ClusterClientGetter
	RestConfig           *rest.Config
	Client               client.Client
}

func NewTestEnv(
	ctx context.Context,
	t *testing.T,
	clusterObjs map[client.ObjectKey][]runtime.Object,
	initObjs ...runtime.Object,
) *TestEnv {
	t.Helper()
	g := gomega.NewWithT(t)

	testEnv := &TestEnv{ //nolint:exhaustivestruct
		env: &envtest.Environment{ //nolint:exhaustivestruct
			CRDs: getClusterAPICRDs(ctx, t, "release-0.3"),
		},
		workloadEnvs: make(map[types.NamespacedName]*envtest.Environment),
	}

	testEnv.WorkloadClientGetter = testEnv.newWorkloadClusterGetter(t)

	restConfig, err := testEnv.env.Start()
	g.Expect(err).NotTo(gomega.HaveOccurred())

	t.Cleanup(func() {
		g.Expect(testEnv.env.Stop()).To(gomega.Succeed())
	})

	testEnv.RestConfig = restConfig

	c, err := client.New(testEnv.RestConfig, client.Options{}) //nolint:exhaustivestruct
	g.Expect(err).NotTo(gomega.HaveOccurred())

	testEnv.Client = c

	for _, obj := range initObjs {
		g.Expect(c.Create(ctx, obj.DeepCopyObject())).To(gomega.Succeed())
	}

	for key, objs := range clusterObjs {
		wc, err := testEnv.WorkloadClientGetter(ctx, c, key, nil)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		for _, obj := range objs {
			g.Expect(wc.Create(ctx, obj.DeepCopyObject())).To(gomega.Succeed())
		}
	}

	return testEnv
}

func (e *TestEnv) AddWorkloadResources(
	ctx context.Context,
	t *testing.T,
	cluster *clusterv1.Cluster,
	objs ...runtime.Object,
) {
	t.Helper()
	g := gomega.NewWithT(t)

	clusterKey, err := client.ObjectKeyFromObject(cluster)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	wc, err := e.WorkloadClientGetter(ctx, e.Client, clusterKey, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	for _, obj := range objs {
		g.Expect(wc.Create(ctx, obj.DeepCopyObject())).To(gomega.Succeed())
	}
}

func (e *TestEnv) newWorkloadClusterGetter(
	t *testing.T,
) func(context.Context, client.Client, types.NamespacedName, *runtime.Scheme) (client.Client, error) {
	t.Helper()
	g := gomega.NewWithT(t)

	return func(
		ctx context.Context,
		_ client.Client,
		cluster types.NamespacedName,
		scheme *runtime.Scheme,
	) (client.Client, error) {
		if e.workloadEnvs[cluster] == nil {
			env := new(envtest.Environment)
			if _, err := env.Start(); err != nil {
				return nil, err //nolint:wrapcheck
			}

			t.Cleanup(func() {
				g.Expect(env.Stop()).To(gomega.Succeed())
			})

			e.workloadEnvs[cluster] = env
		}

		return client.New(e.workloadEnvs[cluster].Config, client.Options{Scheme: scheme}) //nolint:exhaustivestruct,wrapcheck
	}
}

func getClusterAPICRDs(ctx context.Context, t *testing.T, branch string) []runtime.Object {
	t.Helper()
	g := gomega.NewWithT(t)
	httpClient := new(http.Client)

	files := []string{
		"cluster.x-k8s.io_clusters.yaml",
	}

	var resources []runtime.Object

	for _, file := range files {
		url := fmt.Sprintf(
			"https://github.com/kubernetes-sigs/cluster-api/raw/%s/config/crd/bases/%s",
			branch,
			file,
		)

		artifactsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		artifactsResp, err := httpClient.Do(artifactsReq)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		defer artifactsResp.Body.Close()

		decoder := yaml.NewYAMLDecoder(artifactsResp.Body)
		defer decoder.Close()

		for {
			obj, _, err := decoder.Decode(nil, nil)
			if errors.Is(err, io.EOF) {
				break
			}

			g.Expect(err).NotTo(gomega.HaveOccurred())

			resources = append(resources, obj)
		}
	}

	return resources
}

func BeDerivativeOf(expected interface{}) gomegatypes.GomegaMatcher {
	return derivativeMatcher{
		expected: expected,
	}
}

var errDerivativeType = errors.New("BeDerivativeOf matcher expects a controllerutil.Object")

type derivativeMatcher struct {
	expected interface{}
}

func (m *derivativeMatcher) copy(obj controllerutil.Object) controllerutil.Object {
	c, _ := obj.DeepCopyObject().(controllerutil.Object)
	c.SetCreationTimestamp(metav1.NewTime(time.Time{}))
	c.SetGeneration(0)

	return c
}

func (m derivativeMatcher) Match(actual interface{}) (bool, error) {
	a, ok := actual.(controllerutil.Object)
	if !ok {
		return false, errDerivativeType
	}

	expected, ok := m.expected.(controllerutil.Object)
	if !ok {
		return false, errDerivativeType
	}

	return equality.Semantic.DeepDerivative(expected, m.copy(a)), nil
}

func (m derivativeMatcher) FailureMessage(actual interface{}) string {
	return fmt.Sprintf("Expected no diff\ndiff: %s",
		cmp.Diff(m.copy(actual.(controllerutil.Object)), m.expected.(controllerutil.Object), diff.IgnoreUnset()))
}

func (m derivativeMatcher) NegatedFailureMessage(actual interface{}) string {
	return "Expected diff but found none"
}

func GenerateCluster(namespace, name string) *clusterv1.Cluster {
	if namespace == "" {
		namespace = util.RandomString(generatedNameLength)
	}

	if name == "" {
		name = util.RandomString(generatedNameLength)
	}

	return &clusterv1.Cluster{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Namespace: namespace,
			Name:      name,
		},
	}
}

func GenerateNode(name, providerID string) *corev1.Node {
	if name == "" {
		name = util.RandomString(generatedNameLength)
	}

	return &corev1.Node{ // nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ // nolint:exhaustivestruct
			Name: name,
		},
		Spec: corev1.NodeSpec{ // nolint:exhaustivestruct
			ProviderID: providerID,
		},
	}
}

func GenerateDeployment(namespace, name, containerImage string) *appsv1.Deployment {
	labels := map[string]string{"app": name}

	return &appsv1.Deployment{ //nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ //nolint:exhaustivestruct
			Namespace: namespace,
			Name:      name,
		},
		Spec: appsv1.DeploymentSpec{ //nolint:exhaustivestruct
			Selector: &metav1.LabelSelector{MatchLabels: labels}, //nolint:exhaustivestruct
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels}, //nolint:exhaustivestruct
				Spec: corev1.PodSpec{ //nolint:exhaustivestruct
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: containerImage,
						},
					},
				},
			},
		},
	}
}

func GenerateStatefulSet(namespace, name, containerImage string) *appsv1.StatefulSet {
	labels := map[string]string{"app": name}

	return &appsv1.StatefulSet{ //nolint:exhaustivestruct
		ObjectMeta: metav1.ObjectMeta{ //nolint:exhaustivestruct
			Namespace: namespace,
			Name:      name,
		},
		Spec: appsv1.StatefulSetSpec{ //nolint:exhaustivestruct
			Selector: &metav1.LabelSelector{MatchLabels: labels}, //nolint:exhaustivestruct
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels}, //nolint:exhaustivestruct
				Spec: corev1.PodSpec{ //nolint:exhaustivestruct
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: containerImage,
						},
					},
				},
			},
		},
	}
}

func VerifySuccessOutputDryRun(t *testing.T, output string) {
	t.Helper()
	g := gomega.NewWithT(t)
	// TODO: update prefix strings to be variables instead of hardcoded
	g.Expect(output).To(gomega.HavePrefix(base.DryRunPrefix))
}

func VerifySuccessOutputChanged(t *testing.T, output string) {
	t.Helper()
	g := gomega.NewWithT(t)
	// TODO: update prefix strings to be variables instead of hardcoded
	g.Expect(output).To(gomega.HavePrefix(base.SuccessPrefix))
}

func VerifySuccessOutputUnchanged(t *testing.T, output string) {
	t.Helper()
	g := gomega.NewWithT(t)
	// TODO: update prefix strings to be variables instead of hardcoded
	g.Expect(output).To(gomega.HavePrefix(base.NoOpPrefix))
}

func VerifySuccessOutputSkipped(t *testing.T, output string) {
	t.Helper()
	g := gomega.NewWithT(t)
	// TODO: update prefix strings to be variables instead of hardcoded
	g.Expect(output).To(gomega.HavePrefix(base.SkipPrefix))
}
