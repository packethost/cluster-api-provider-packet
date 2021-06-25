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

package upgrader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/cluster-api-provider-packet/cmd/helper/base"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	oldSecretName      = "packet-cloud-config"
	newSecretName      = "metal-cloud-config" //nolint: gosec
	oldDeploymentName  = "packet-cloud-controller-manager"
	csiStatefulSetName = "csi-packet-controller"
)

type Upgrader struct {
	*base.Tool
	upgradeMutex  sync.Mutex
	steps         []step
	clusters      []*clusterv1.Cluster
	clusterStatus map[string]map[string]bool
	logger        logr.Logger
}

type step struct {
	name   string
	method func(context.Context, logr.Logger, *base.Tool, *clusterv1.Cluster) error
}

func New(ctx context.Context, config *base.ToolConfig) (*Upgrader, error) {
	u := new(Upgrader)
	u.upgradeMutex.Lock()
	defer u.upgradeMutex.Unlock()

	u.logger = config.Logger.WithName("UpgradeCloudProvider")
	u.Tool = new(base.Tool)
	u.Configure(config)

	u.steps = []step{
		{
			name:   "MigrateCredentialsSecret",
			method: migrateSecret,
		},
		{
			name:   "RemoveDeprecatedManager",
			method: removeCCMDeployment,
		},
		{
			name:   "RemoveOldCredentialsSecret",
			method: removeOldCCMSecret,
		},
		{
			name:   "InstallNewManager",
			method: installCPEM,
		},
	}

	clusters, err := u.GetClusters(ctx)
	if err != nil {
		u.logger.Error(err, "Failed to get clusters")

		return u, err
	}

	u.clusters = clusters
	u.clusterStatus = make(map[string]map[string]bool, len(clusters))

	for _, c := range clusters {
		clusterKey := base.ObjectToName(c)

		if u.clusterStatus[clusterKey] == nil {
			u.clusterStatus[clusterKey] = make(map[string]bool, len(u.steps))
		}
	}

	return u, nil
}

func (u *Upgrader) CalculatePercentage() float64 {
	u.upgradeMutex.Lock()
	defer u.upgradeMutex.Unlock()

	if u.clusterStatus == nil {
		u.clusterStatus = make(map[string]map[string]bool)
	}

	totalClusters := len(u.clusterStatus)
	totalSteps := totalClusters * len(u.steps)
	doneSteps := 0

	for cKey := range u.clusterStatus {
		for _, sDone := range u.clusterStatus[cKey] {
			if sDone {
				doneSteps++
			}
		}
	}

	if totalSteps == 0 {
		return float64(0)
	}

	return float64(doneSteps) / float64(totalSteps)
}

func (u *Upgrader) CheckPrerequisites(ctx context.Context) error {
	u.logger.Info("Checking Prerequisites")

	return nil
}

func (u *Upgrader) Run(ctx context.Context) {
	wg := new(sync.WaitGroup)

	for i := range u.clusters {
		c := u.clusters[i]

		wg.Add(1)

		go func() {
			defer wg.Done()

			u.upgradeCloudProviderForCluster(ctx, c)
		}()
	}

	wg.Wait()
}

func (u *Upgrader) upgradeCloudProviderForCluster(
	ctx context.Context,
	c *clusterv1.Cluster,
) {
	logger := u.logger.WithValues("cluster", base.ObjectToName(c))
	logger.Info("Started upgrade for cluster")

	// Return early if cluster has already hit an error
	if u.HasError(c) {
		logger.Info("Cluster previously ran into an error, skipping further processing")

		return
	}

	for _, s := range u.steps {
		stepLogger := logger.WithValues("step", s.name)
		stepLogger.Info("Started running step")

		if err := s.method(ctx, stepLogger, u.Tool, c); err != nil {
			stepLogger.Error(err, "Failure running step")
			u.AddErrorFor(c, err)

			return
		}

		u.updateStepStatus(c, s, true)
		stepLogger.Info("Finished running step")
	}

	logger.Info("Finished upgrade for cluster")
}

func getLatestCPEMVersion(ctx context.Context) (string, error) {
	url := "https://github.com/equinix/cloud-provider-equinix-metal/releases/latest"
	httpClient := new(http.Client)

	versionReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	versionReq.Header.Set("Accept", "application/json")

	versionResp, err := httpClient.Do(versionReq)
	if err != nil {
		return "", err
	}

	defer versionResp.Body.Close()

	versionBody, err := ioutil.ReadAll(versionResp.Body)
	if err != nil {
		return "", err
	}

	releaseInfo := make(map[string]interface{})
	if err := json.Unmarshal(versionBody, &releaseInfo); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s", releaseInfo["tag_name"]), nil
}

func getCPEMArtifacts(ctx context.Context, version string) ([]*unstructured.Unstructured, error) {
	httpClient := new(http.Client)
	url := fmt.Sprintf(
		"https://github.com/equinix/cloud-provider-equinix-metal/releases/download/%s/deployment.yaml",
		version,
	)

	artifactsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	artifactsResp, err := httpClient.Do(artifactsReq)
	if err != nil {
		return nil, err
	}

	defer artifactsResp.Body.Close()

	decoder := yaml.NewYAMLDecoder(artifactsResp.Body)
	defer decoder.Close()

	var resources []*unstructured.Unstructured

	for {
		obj, _, err := decoder.Decode(nil, nil)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		u := new(unstructured.Unstructured)
		u.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())

		un, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, err
		}

		u.SetUnstructuredContent(un)

		resources = append(resources, u)
	}

	return resources, nil
}

func installCPEM(ctx context.Context, logger logr.Logger, u *base.Tool, c *clusterv1.Cluster) error {
	cpemVersion, err := getLatestCPEMVersion(ctx)
	if err != nil {
		return err
	}

	resources, err := getCPEMArtifacts(ctx, cpemVersion)
	if err != nil {
		return err
	}

	for _, r := range resources {
		if err := u.WorkloadPatchOrCreateUnstructured(ctx, logger, c, r); err != nil {
			return err
		}
	}

	return nil
}

func removeOldCCMSecret(ctx context.Context, logger logr.Logger, u *base.Tool, c *clusterv1.Cluster) error {
	stdout := u.GetBufferFor(c)
	ccmSecretKey := client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: oldSecretName}
	csiStatefulSet := new(appsv1.StatefulSet)
	csiKey := client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: csiStatefulSetName}
	secretName := fmt.Sprintf("%s/%s", ccmSecretKey.Namespace, ccmSecretKey.Name)

	err := u.WorkloadGet(ctx, c, csiKey, csiStatefulSet)

	switch {
	case err != nil && !apierrors.IsNotFound(err):
		return err
	case err == nil:
		logger.Info(base.SkipPrefix+"removal of secret because Packet CSI is deployed", "name", secretName)
		fmt.Fprintf(stdout, "%sremoval of Secret %s because Packet CSI is deployed", base.SkipPrefix, secretName)

		return nil
	}

	ccmSecret := new(corev1.Secret)
	if err := u.WorkloadGet(ctx, c, ccmSecretKey, ccmSecret); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Secret already removed", "name", secretName)
			fmt.Fprintf(stdout, "%sSecret %s already deleted\n", base.NoOpPrefix, secretName)

			return nil
		}

		return err
	}

	return u.WorkloadDelete(ctx, logger, c, ccmSecret)
}

func removeCCMDeployment(ctx context.Context, logger logr.Logger, u *base.Tool, c *clusterv1.Cluster) error {
	stdout := u.GetBufferFor(c)
	ccmDeployment := new(appsv1.Deployment)
	ccmKey := client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: oldDeploymentName}
	deploymentName := fmt.Sprintf("%s/%s", ccmDeployment.Namespace, ccmDeployment.Name)

	if err := u.WorkloadGet(ctx, c, ccmKey, ccmDeployment); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Deployment already removed", "name", deploymentName)
			fmt.Fprintf(stdout, "%sDeployment %s already deleted\n", base.NoOpPrefix, deploymentName)

			return nil
		}

		return err
	}

	return u.WorkloadDelete(ctx, logger, c, ccmDeployment)
}

func migrateSecret(ctx context.Context, logger logr.Logger, u *base.Tool, c *clusterv1.Cluster) error {
	stdout := u.GetBufferFor(c)
	// Check to see if the CPEM secret already exists
	cpemSecret := new(corev1.Secret)
	cpemSecretKey := client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: newSecretName}
	cpemSecretName := fmt.Sprintf("%s/%s", cpemSecretKey.Namespace, cpemSecretKey.Name)
	err := u.WorkloadGet(ctx, c, cpemSecretKey, cpemSecret)

	switch {
	case err != nil && !apierrors.IsNotFound(err):
		return err
	case err == nil:
		// If there was no error, then the secret already exists and there is no need to proceed

		logger.Info("Secret already exists", "name", cpemSecretName)
		fmt.Fprintf(stdout, "%sSecret %s/%s already exists\n", base.NoOpPrefix, cpemSecret.Namespace, cpemSecret.Name)

		return nil
	}

	// Fetch the old CCM secret
	ccmSecret := new(corev1.Secret)
	ccmSecretKey := client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: oldSecretName}

	if err := u.WorkloadGet(ctx, c, ccmSecretKey, ccmSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("secret %s/%s not found", ccmSecretKey.Namespace, ccmSecretKey.Name)
		}

		return err
	}

	newSecret := new(corev1.Secret)
	newSecret.SetNamespace(cpemSecretKey.Namespace)
	newSecret.SetName(cpemSecretKey.Name)
	newSecret.Data = ccmSecret.Data

	return u.WorkloadCreate(ctx, logger, c, newSecret)
}

func (u *Upgrader) updateStepStatus(c *clusterv1.Cluster, step step, done bool) {
	u.upgradeMutex.Lock()
	defer u.upgradeMutex.Unlock()

	if u.clusterStatus == nil {
		u.clusterStatus = make(map[string]map[string]bool)
	}

	clusterKey := base.ObjectToName(c)

	if u.clusterStatus[clusterKey] == nil {
		u.clusterStatus[clusterKey] = make(map[string]bool, len(u.steps))
	}

	u.clusterStatus[clusterKey][step.name] = done
}
