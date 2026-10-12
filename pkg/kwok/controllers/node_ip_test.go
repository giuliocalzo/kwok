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
	"reflect"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func nodeWithAddresses(addresses ...corev1.NodeAddress) *corev1.Node {
	return &corev1.Node{
		Name:   "node0",
		Status: corev1.NodeStatus{Addresses: addresses},
	}
}

// leaseWithMarkers returns a node lease with the given current and previous node IP annotations; empty means unset.
func leaseWithMarkers(current, previous string) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		Name:      "node0",
		Namespace: corev1.NamespaceNodeLease,
	}
	if current != "" {
		setLeaseAnnotation(lease, nodeIPAnnotation, current)
	}
	if previous != "" {
		setLeaseAnnotation(lease, previousNodeIPAnnotation, previous)
	}
	return lease
}

func internalIP(ip string) corev1.NodeAddress {
	return corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: ip}
}

func externalIP(ip string) corev1.NodeAddress {
	return corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: ip}
}

func hostname(name string) corev1.NodeAddress {
	return corev1.NodeAddress{Type: corev1.NodeHostName, Address: name}
}

func TestRecordNodeIP(t *testing.T) {
	tests := []struct {
		name         string
		lease        *coordinationv1.Lease
		node         *corev1.Node
		ownIP        string
		wantCurrent  string
		wantPrevious string
	}{
		{
			name:        "marks a node pointing at us",
			lease:       leaseWithMarkers("", ""),
			node:        nodeWithAddresses(hostname("node0"), internalIP("10.0.0.2")),
			ownIP:       "10.0.0.2",
			wantCurrent: "10.0.0.2",
		},
		{
			name:        "drops the previous IP once the node points at us",
			lease:       leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:        nodeWithAddresses(internalIP("10.0.0.2")),
			ownIP:       "10.0.0.2",
			wantCurrent: "10.0.0.2",
		},
		{
			name:         "records a takeover from the marked holder",
			lease:        leaseWithMarkers("10.0.0.1", ""),
			node:         nodeWithAddresses(internalIP("10.0.0.1")),
			ownIP:        "10.0.0.2",
			wantCurrent:  "10.0.0.2",
			wantPrevious: "10.0.0.1",
		},
		{
			name:         "records a takeover after a holder died before repointing",
			lease:        leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:         nodeWithAddresses(internalIP("10.0.0.1")),
			ownIP:        "10.0.0.3",
			wantCurrent:  "10.0.0.3",
			wantPrevious: "10.0.0.1",
		},
		{
			name:         "records a takeover after a holder died right after repointing",
			lease:        leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:         nodeWithAddresses(internalIP("10.0.0.2")),
			ownIP:        "10.0.0.3",
			wantCurrent:  "10.0.0.3",
			wantPrevious: "10.0.0.2",
		},
		{
			name:  "leaves an unmarked user-set address alone",
			lease: leaseWithMarkers("", ""),
			node:  nodeWithAddresses(internalIP("192.168.0.9")),
			ownIP: "10.0.0.2",
		},
		{
			name:        "leaves the markers alone when the address is not a kwok one",
			lease:       leaseWithMarkers("10.0.0.1", ""),
			node:        nodeWithAddresses(internalIP("192.168.0.9")),
			ownIP:       "10.0.0.2",
			wantCurrent: "10.0.0.1",
		},
		{
			name:        "does nothing without an own IP",
			lease:       leaseWithMarkers("10.0.0.1", ""),
			node:        nodeWithAddresses(internalIP("10.0.0.1")),
			ownIP:       "",
			wantCurrent: "10.0.0.1",
		},
		{
			name:  "does nothing for a node without an InternalIP",
			lease: leaseWithMarkers("", ""),
			node:  nodeWithAddresses(hostname("node0")),
			ownIP: "10.0.0.2",
		},
		{
			name:  "only considers the first InternalIP",
			lease: leaseWithMarkers("", ""),
			node:  nodeWithAddresses(internalIP("10.0.0.1"), internalIP("10.0.0.2")),
			ownIP: "10.0.0.2",
		},
		{
			name:  "ignores an InternalIP without an address",
			lease: leaseWithMarkers("", ""),
			node:  nodeWithAddresses(internalIP("")),
			ownIP: "10.0.0.2",
		},
		{
			name:         "keeps the takeover recorded until the node is repointed",
			lease:        leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:         nodeWithAddresses(internalIP("10.0.0.1")),
			ownIP:        "10.0.0.2",
			wantCurrent:  "10.0.0.2",
			wantPrevious: "10.0.0.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recordNodeIP(tt.lease, tt.node, tt.ownIP)
			if got := tt.lease.Annotations[nodeIPAnnotation]; got != tt.wantCurrent {
				t.Errorf("%s = %q, want %q", nodeIPAnnotation, got, tt.wantCurrent)
			}
			if got := tt.lease.Annotations[previousNodeIPAnnotation]; got != tt.wantPrevious {
				t.Errorf("%s = %q, want %q", previousNodeIPAnnotation, got, tt.wantPrevious)
			}
		})
	}
}

func TestRecordNodeIPIgnoresANilLease(t *testing.T) {
	recordNodeIP(nil, nodeWithAddresses(internalIP("10.0.0.2")), "10.0.0.2")
}

func TestRepointNodeIP(t *testing.T) {
	tests := []struct {
		name      string
		lease     *coordinationv1.Lease
		node      *corev1.Node
		ownIP     string
		repointed bool
		want      []corev1.NodeAddress
	}{
		{
			name:      "repoints a node once the takeover is recorded",
			lease:     leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:      nodeWithAddresses(hostname("node0"), internalIP("10.0.0.1")),
			ownIP:     "10.0.0.2",
			repointed: true,
			want:      []corev1.NodeAddress{hostname("node0"), internalIP("10.0.0.2")},
		},
		{
			name:  "waits until the takeover is recorded on the lease",
			lease: leaseWithMarkers("10.0.0.1", "10.0.0.9"),
			node:  nodeWithAddresses(internalIP("10.0.0.9")),
			ownIP: "10.0.0.2",
			want:  []corev1.NodeAddress{internalIP("10.0.0.9")},
		},
		{
			name:  "skips a lease without a previous IP",
			lease: leaseWithMarkers("10.0.0.2", ""),
			node:  nodeWithAddresses(internalIP("")),
			ownIP: "10.0.0.2",
			want:  []corev1.NodeAddress{internalIP("")},
		},
		{
			name:  "skips a node whose address no longer matches the previous IP",
			lease: leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:  nodeWithAddresses(internalIP("192.168.0.9")),
			ownIP: "10.0.0.2",
			want:  []corev1.NodeAddress{internalIP("192.168.0.9")},
		},
		{
			name:  "skips without an own IP",
			lease: leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:  nodeWithAddresses(internalIP("10.0.0.1")),
			ownIP: "",
			want:  []corev1.NodeAddress{internalIP("10.0.0.1")},
		},
		{
			name:  "skips a node without an InternalIP",
			lease: leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:  nodeWithAddresses(hostname("node0")),
			ownIP: "10.0.0.2",
			want:  []corev1.NodeAddress{hostname("node0")},
		},
		{
			name:  "only considers the first InternalIP",
			lease: leaseWithMarkers("10.0.0.2", "10.0.0.1"),
			node:  nodeWithAddresses(internalIP("10.9.9.9"), internalIP("10.0.0.1")),
			ownIP: "10.0.0.2",
			want:  []corev1.NodeAddress{internalIP("10.9.9.9"), internalIP("10.0.0.1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewClientset(tt.node.DeepCopy())

			repointed, err := repointNodeIP(ctx, client, tt.lease, tt.node, tt.ownIP)
			if err != nil {
				t.Fatalf("repointNodeIP() error = %v", err)
			}
			if repointed != tt.repointed {
				t.Errorf("repointNodeIP() = %v, want %v", repointed, tt.repointed)
			}

			node, err := client.CoreV1().Nodes().Get(ctx, tt.node.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(node.Status.Addresses, tt.want) {
				t.Errorf("addresses = %v, want %v", node.Status.Addresses, tt.want)
			}
		})
	}
}

func TestRepointNodeIPRefusesAStaleCache(t *testing.T) {
	tests := []struct {
		name   string
		stored []corev1.NodeAddress
	}{
		{
			name:   "address changed",
			stored: []corev1.NodeAddress{internalIP("192.168.0.9")},
		},
		{
			name:   "same address under another type",
			stored: []corev1.NodeAddress{externalIP("10.0.0.1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewClientset(nodeWithAddresses(tt.stored...))

			cached := nodeWithAddresses(internalIP("10.0.0.1"))
			_, err := repointNodeIP(ctx, client, leaseWithMarkers("10.0.0.2", "10.0.0.1"), cached, "10.0.0.2")
			if err == nil {
				t.Fatal("repointNodeIP() succeeded against a node that changed since the cache was read")
			}

			node, err := client.CoreV1().Nodes().Get(ctx, "node0", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(node.Status.Addresses, tt.stored) {
				t.Errorf("addresses = %v, want them untouched %v", node.Status.Addresses, tt.stored)
			}
		})
	}
}
