/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/kwok/pkg/log"
	"sigs.k8s.io/kwok/pkg/utils/informer"
)

func TestNodeLeaseController(t *testing.T) {
	now := time.Now()
	clientset := fake.NewClientset(
		&coordinationv1.Lease{
			Name:      "lease1",
			Namespace: corev1.NamespaceNodeLease,
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new("lease1"),
				RenewTime:            new(metav1.NewMicroTime(now.Add(-61 * time.Second))),
				LeaseDurationSeconds: new(int32(60)),
			},
		},
		&coordinationv1.Lease{
			Name:      "lease2",
			Namespace: corev1.NamespaceNodeLease,
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new("lease2"),
				RenewTime:            new(metav1.NewMicroTime(now)),
				LeaseDurationSeconds: new(int32(60)),
			},
		},
		&coordinationv1.Lease{
			Name:      "lease3",
			Namespace: corev1.NamespaceNodeLease,
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new("lease3"),
				RenewTime:            new(metav1.NewMicroTime(now.Add(-61 * time.Second))),
				LeaseDurationSeconds: new(int32(60)),
			},
		},
	)

	ctx := context.Background()
	ctx = log.NewContext(ctx, log.NewLogger(os.Stderr, log.LevelDebug))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(func() {
		cancel()
		time.Sleep(time.Second)
	})

	nodeLeasesCli := clientset.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nodesInformer := informer.NewInformer[*coordinationv1.Lease, *coordinationv1.LeaseList](nodeLeasesCli)
	cache, err := nodesInformer.WatchWithCache(ctx, informer.Option{}, nil)
	if err != nil {
		t.Fatal(fmt.Errorf("watch node leases error: %w", err))
	}

	nodeLeases, err := NewNodeLeaseController(NodeLeaseControllerConfig{
		TypedClient: clientset,
		GetLease: func(nodeName string) (*coordinationv1.Lease, bool) {
			return cache.GetWithNamespace(nodeName, corev1.NamespaceNodeLease)
		},
		HolderIdentity:       "test",
		LeaseDurationSeconds: 40,
		LeaseParallelism:     2,
		RenewInterval:        10 * time.Second,
		RenewIntervalJitter:  0.04,
	})
	if err != nil {
		t.Fatal(fmt.Errorf("new node leases controller error: %w", err))
	}

	time.Sleep(1 * time.Second)

	err = nodeLeases.Start(ctx)
	if err != nil {
		t.Fatal(fmt.Errorf("start node leases controller error: %w", err))
	}

	nodeLeases.TryHold("lease0")
	nodeLeases.TryHold("lease1")
	nodeLeases.TryHold("lease2")

	time.Sleep(1 * time.Second)

	if !nodeLeases.Held("lease0") {
		t.Error("lease0 not held")
	}

	if !nodeLeases.Held("lease1") {
		t.Error("lease1 not held")
	}

	if nodeLeases.Held("lease2") {
		t.Error("lease2 held")
	}

	if nodeLeases.Held("lease3") {
		t.Error("lease3 held")
	}

	if nodeLeases.Held("lease4") {
		t.Error("lease4 held")
	}

	_ = clientset.CoordinationV1().Leases(corev1.NamespaceNodeLease).Delete(ctx, "lease1", metav1.DeleteOptions{})
	time.Sleep(2 * time.Second)

	if !nodeLeases.Held("lease0") {
		t.Error("lease0 not held")
	}

	if nodeLeases.Held("lease1") {
		t.Error("lease1 held")
	}

	if nodeLeases.Held("lease3") {
		t.Error("lease3 held")
	}

	if nodeLeases.Held("lease4") {
		t.Error("lease4 held")
	}
}

func TestNodeLeaseControllerOnLeaseRenewed(t *testing.T) {
	now := time.Now()
	clientset := fake.NewClientset(
		&coordinationv1.Lease{
			Name:        "node0",
			Namespace:   corev1.NamespaceNodeLease,
			Annotations: map[string]string{nodeIPAnnotation: "10.0.0.1"},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new("previous"),
				RenewTime:            new(metav1.NewMicroTime(now.Add(-61 * time.Second))),
				LeaseDurationSeconds: new(int32(60)),
			},
		},
	)

	ctx := context.Background()
	ctx = log.NewContext(ctx, log.NewLogger(os.Stderr, log.LevelDebug))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)

	nodeLeasesCli := clientset.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	leasesInformer := informer.NewInformer[*coordinationv1.Lease, *coordinationv1.LeaseList](nodeLeasesCli)
	cache, err := leasesInformer.WatchWithCache(ctx, informer.Option{}, nil)
	if err != nil {
		t.Fatal(fmt.Errorf("watch node leases error: %w", err))
	}

	renewed := make(chan *coordinationv1.Lease, 10)
	nodeLeases, err := NewNodeLeaseController(NodeLeaseControllerConfig{
		TypedClient: clientset,
		GetLease: func(nodeName string) (*coordinationv1.Lease, bool) {
			return cache.GetWithNamespace(nodeName, corev1.NamespaceNodeLease)
		},
		HolderIdentity:       "test",
		LeaseDurationSeconds: 40,
		LeaseParallelism:     1,
		RenewInterval:        10 * time.Second,
		RenewIntervalJitter:  0.04,
		OnLeaseRenewedFunc: func(_ context.Context, lease *coordinationv1.Lease) {
			renewed <- lease
		},
	})
	if err != nil {
		t.Fatal(fmt.Errorf("new node leases controller error: %w", err))
	}

	time.Sleep(1 * time.Second)

	err = nodeLeases.Start(ctx)
	if err != nil {
		t.Fatal(fmt.Errorf("start node leases controller error: %w", err))
	}
	nodeLeases.TryHold("node0")

	select {
	case lease := <-renewed:
		if got := *lease.Spec.HolderIdentity; got != "test" {
			t.Errorf("holder = %q, want %q", got, "test")
		}
		if got := lease.Annotations[nodeIPAnnotation]; got != "10.0.0.1" {
			t.Errorf("marker = %q, want the previous holder's %q", got, "10.0.0.1")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnLeaseRenewedFunc was not called after taking over the lease")
	}
}

func Test_tryAcquireOrRenew(t *testing.T) {
	now := time.Now()
	type args struct {
		lease          *coordinationv1.Lease
		holderIdentity string
		now            time.Time
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "holder self",
			args: args{
				lease: &coordinationv1.Lease{
					Spec: coordinationv1.LeaseSpec{
						HolderIdentity: new("test"),
					},
				},
				holderIdentity: "test",
				now:            now,
			},
			want: true,
		},
		{
			name: "holder not self",
			args: args{
				lease: &coordinationv1.Lease{
					Spec: coordinationv1.LeaseSpec{
						HolderIdentity:       new("test"),
						LeaseDurationSeconds: new(int32(10)),
						RenewTime:            new(metav1.NewMicroTime(now.Add(-5 * time.Second))),
					},
				},
				holderIdentity: "test-new",
				now:            now,
			},
			want: false,
		},
		{
			name: "holder not self but expired",
			args: args{
				lease: &coordinationv1.Lease{
					Spec: coordinationv1.LeaseSpec{
						HolderIdentity:       new("test"),
						LeaseDurationSeconds: new(int32(10)),
						RenewTime:            new(metav1.NewMicroTime(now.Add(-11 * time.Second))),
					},
				},
				holderIdentity: "test-new",
				now:            now,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tryAcquireOrRenew(tt.args.lease, tt.args.holderIdentity, tt.args.now); got != tt.want {
				t.Errorf("tryAcquireOrRenew() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNextTryDuration(t *testing.T) {
	tests := []struct {
		name           string
		renewInterval  time.Duration
		expire         time.Duration
		hold           bool
		expectedResult time.Duration
	}{
		{
			name:           "Lease not held",
			renewInterval:  10 * time.Second,
			expire:         5 * time.Second,
			hold:           false,
			expectedResult: 10 * time.Second,
		},
		{
			name:           "Lease held, renew interval less than expire time",
			renewInterval:  5 * time.Second,
			expire:         10 * time.Second,
			hold:           true,
			expectedResult: 5 * time.Second,
		},
		{
			name:           "Lease held, renew interval greater than expire time",
			renewInterval:  10 * time.Second,
			expire:         5 * time.Second,
			hold:           true,
			expectedResult: 5 * time.Second,
		},
		{
			name:           "Lease held, expire time less than 1 second",
			renewInterval:  10 * time.Second,
			expire:         500 * time.Millisecond,
			hold:           true,
			expectedResult: 1 * time.Second,
		},
		{
			name:           "Lease held, renew interval equals expire time",
			renewInterval:  5 * time.Second,
			expire:         5 * time.Second,
			hold:           true,
			expectedResult: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nextTryDuration(tt.renewInterval, tt.expire, tt.hold)
			if result != tt.expectedResult {
				t.Errorf("nextTryDuration() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestExpireTime(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		lease    *coordinationv1.Lease
		wantTime time.Time
		wantBool bool
	}{
		{
			name:     "Lease with missing fields",
			lease:    &coordinationv1.Lease{},
			wantTime: time.Time{},
			wantBool: false,
		},
		{
			name: "Lease with all fields",
			lease: &coordinationv1.Lease{
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       new("test"),
					LeaseDurationSeconds: new(int32(10)),
					RenewTime:            new(metav1.NewMicroTime(now)),
				},
			},
			wantTime: now.Add(10 * time.Second),
			wantBool: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTime, gotBool := expireTime(tt.lease)
			if !reflect.DeepEqual(gotTime, tt.wantTime) {
				t.Errorf("expireTime() gotTime = %v, want %v", gotTime, tt.wantTime)
			}
			if gotBool != tt.wantBool {
				t.Errorf("expireTime() gotBool = %v, want %v", gotBool, tt.wantBool)
			}
		})
	}
}

func TestNodeLeaseControllerTakeOverLeases(t *testing.T) {
	const group = "kube-system/kwok-controller"
	now := time.Now()
	leaseRenewedAt := func(name, holder string, renewTime time.Time, annotations map[string]string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			Name:        name,
			Namespace:   corev1.NamespaceNodeLease,
			Annotations: annotations,
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new(holder),
				RenewTime:            new(metav1.NewMicroTime(renewTime)),
				LeaseDurationSeconds: new(int32(60)),
			},
		}
	}
	clientset := fake.NewClientset(
		leaseRenewedAt("same-group", "previous-leader", now, map[string]string{takeOverGroupAnnotation: group}),
		leaseRenewedAt("no-group", "kubelet", now, nil),
		leaseRenewedAt("other-group", "other-leader", now, map[string]string{takeOverGroupAnnotation: "kube-system/other"}),
		leaseRenewedAt("expired", "old-kwok", now.Add(-61*time.Second), nil),
	)

	ctx := context.Background()
	ctx = log.NewContext(ctx, log.NewLogger(os.Stderr, log.LevelDebug))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)

	nodeLeasesCli := clientset.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	leasesInformer := informer.NewInformer[*coordinationv1.Lease, *coordinationv1.LeaseList](nodeLeasesCli)
	cache, err := leasesInformer.WatchWithCache(ctx, informer.Option{}, nil)
	if err != nil {
		t.Fatal(fmt.Errorf("watch node leases error: %w", err))
	}

	nodeLeases, err := NewNodeLeaseController(NodeLeaseControllerConfig{
		TypedClient: clientset,
		GetLease: func(nodeName string) (*coordinationv1.Lease, bool) {
			return cache.GetWithNamespace(nodeName, corev1.NamespaceNodeLease)
		},
		HolderIdentity:       "test",
		LeaseDurationSeconds: 40,
		LeaseParallelism:     1,
		RenewInterval:        10 * time.Second,
		RenewIntervalJitter:  0.04,
		TakeOverGroup:        group,
	})
	if err != nil {
		t.Fatal(fmt.Errorf("new node leases controller error: %w", err))
	}

	time.Sleep(1 * time.Second)

	err = nodeLeases.Start(ctx)
	if err != nil {
		t.Fatal(fmt.Errorf("start node leases controller error: %w", err))
	}
	for _, name := range []string{"same-group", "no-group", "other-group", "expired", "created"} {
		nodeLeases.TryHold(name)
	}

	time.Sleep(1 * time.Second)

	for name, want := range map[string]bool{"same-group": true, "expired": true, "created": true, "no-group": false, "other-group": false} {
		if got := nodeLeases.Held(name); got != want {
			t.Errorf("Held(%q) = %v, want %v", name, got, want)
		}
	}
	for _, name := range []string{"same-group", "expired", "created"} {
		lease, ok := cache.GetWithNamespace(name, corev1.NamespaceNodeLease)
		if !ok {
			t.Errorf("lease %q not found", name)
			continue
		}
		if got := lease.Annotations[takeOverGroupAnnotation]; got != group {
			t.Errorf("lease %q group = %q, want %q", name, got, group)
		}
	}
}

func TestNodeLeaseControllerSetTakeOverGroup(t *testing.T) {
	stamped := func() *coordinationv1.Lease {
		return &coordinationv1.Lease{
			Annotations: map[string]string{takeOverGroupAnnotation: "kube-system/old"},
		}
	}

	lease := stamped()
	(&NodeLeaseController{takeOverGroup: "kube-system/kwok-controller"}).setTakeOverGroup(lease)
	if got := lease.Annotations[takeOverGroupAnnotation]; got != "kube-system/kwok-controller" {
		t.Errorf("group = %q, want %q", got, "kube-system/kwok-controller")
	}

	lease = stamped()
	(&NodeLeaseController{}).setTakeOverGroup(lease)
	if got, ok := lease.Annotations[takeOverGroupAnnotation]; ok {
		t.Errorf("group = %q, want it removed without a takeover group", got)
	}
}
