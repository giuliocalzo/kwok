/*
Copyright 2026 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
)

// Once a node lease is marked, the node's InternalIP points at one of these two annotations whenever it is a kwok address.
const (
	// nodeIPAnnotation records the kwok address of the current lease holder.
	nodeIPAnnotation = "kwok.x-k8s.io/node-ip"
	// previousNodeIPAnnotation records the kwok address the node pointed at before the current holder took over.
	previousNodeIPAnnotation = "kwok.x-k8s.io/previous-node-ip"
)

// internalIPIndex returns the index of the node's first InternalIP address, or -1.
func internalIPIndex(node *corev1.Node) int {
	for i, address := range node.Status.Addresses {
		if address.Type == corev1.NodeInternalIP {
			return i
		}
	}
	return -1
}

// recordNodeIP updates the lease annotations before the lease is written, so the takeover
// of a node pointing at a kwok address is recorded before the node is repointed.
func recordNodeIP(lease *coordinationv1.Lease, node *corev1.Node, ownIP string) {
	if ownIP == "" || lease == nil || node == nil {
		return
	}
	i := internalIPIndex(node)
	if i < 0 {
		return
	}
	current := node.Status.Addresses[i].Address
	switch current {
	case "":
		return
	case ownIP:
		setLeaseAnnotation(lease, nodeIPAnnotation, ownIP)
		delete(lease.Annotations, previousNodeIPAnnotation)
	case lease.Annotations[nodeIPAnnotation], lease.Annotations[previousNodeIPAnnotation]:
		setLeaseAnnotation(lease, previousNodeIPAnnotation, current)
		setLeaseAnnotation(lease, nodeIPAnnotation, ownIP)
	}
}

func setLeaseAnnotation(lease *coordinationv1.Lease, key, value string) {
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[key] = value
}

// repointNodeIP moves the node's InternalIP from the previous kwok address recorded on the lease to ownIP.
// It reports whether the node was patched.
func repointNodeIP(ctx context.Context, client clientset.Interface, lease *coordinationv1.Lease, node *corev1.Node, ownIP string) (bool, error) {
	if ownIP == "" || lease == nil || node == nil {
		return false, nil
	}
	if lease.Annotations[nodeIPAnnotation] != ownIP {
		return false, nil
	}
	previous := lease.Annotations[previousNodeIPAnnotation]
	if previous == "" || previous == ownIP {
		return false, nil
	}
	i := internalIPIndex(node)
	if i < 0 || node.Status.Addresses[i].Address != previous {
		return false, nil
	}

	path := "/status/addresses/" + strconv.Itoa(i)
	patch, err := json.Marshal([]map[string]string{
		{"op": "test", "path": path + "/type", "value": string(corev1.NodeInternalIP)},
		{"op": "test", "path": path + "/address", "value": previous},
		{"op": "replace", "path": path + "/address", "value": ownIP},
	})
	if err != nil {
		return false, err
	}
	_, err = client.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		return false, fmt.Errorf("failed to repoint node %s from %s to %s: %w", node.Name, previous, ownIP, err)
	}
	return true, nil
}
