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

package packet

import (
	"fmt"
)

const (
	MachineUIDTag  = "cluster-api-provider-packet:machine-uid"
	machineNameTag = "cluster-api-provider-packet:machine-name"
	clusterIDTag   = "cluster-api-provider-packet:cluster-id"
	namespaceTag   = "cluster-api-provider-packet:namespace"
)

func GenerateMachineTag(id string) string {
	return fmt.Sprintf("%s:%s", MachineUIDTag, id)
}

func GenerateMachineNameTag(name string) string {
	return fmt.Sprintf("%s:%s", machineNameTag, name)
}

func GenerateClusterTag(clusterName string) string {
	return fmt.Sprintf("%s:%s", clusterIDTag, clusterName)
}

func GenerateNamespaceTag(namespace string) string {
	return fmt.Sprintf("%s:%s", namespaceTag, namespace)
}

// ItemsInList checks if all items are in the list
func ItemsInList(list []string, items []string) bool {
	// convert the items against which we are mapping into a map
	itemMap := map[string]bool{}
	for _, elm := range items {
		itemMap[elm] = false
	}
	// every one that is matched goes from false to true in the map
	for _, elm := range list {
		if _, ok := itemMap[elm]; ok {
			itemMap[elm] = true
		}
	}
	// go through the map; if any is false, return false, else all matched so return true
	for _, v := range itemMap {
		if !v {
			return false
		}
	}
	return true
}
