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

package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func testLeaderElectionConfig(client *fake.Clientset) leaderElectionConfig {
	return leaderElectionConfig{
		Client:        client,
		Name:          "kwok-controller",
		Namespace:     "kube-system",
		Identity:      "test",
		LeaseDuration: time.Second,
		RenewDeadline: 500 * time.Millisecond,
		RetryPeriod:   100 * time.Millisecond,
	}
}

func TestRunWithLeaderElectionRunsOnceElected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := make(chan struct{})
	err := runWithLeaderElection(ctx, testLeaderElectionConfig(fake.NewClientset()), func(runCtx context.Context) error {
		close(started)
		cancel()
		<-runCtx.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("runWithLeaderElection() error = %v, want nil on shutdown", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("run was not called after acquiring the lease")
	}
}

func TestRunWithLeaderElectionWaitsWhileAnotherHolds(t *testing.T) {
	client := fake.NewClientset(&coordinationv1.Lease{
		Name:      "kwok-controller",
		Namespace: "kube-system",
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       new("other"),
			RenewTime:            new(metav1.NewMicroTime(time.Now())),
			AcquireTime:          new(metav1.NewMicroTime(time.Now())),
			LeaseDurationSeconds: new(int32(60)),
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	called := false
	err := runWithLeaderElection(ctx, testLeaderElectionConfig(client), func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("runWithLeaderElection() error = %v, want nil on shutdown", err)
	}
	if called {
		t.Fatal("run was called while another instance holds the lease")
	}
}

func TestRunWithLeaderElectionReturnsRunError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	boom := errors.New("boom")
	err := runWithLeaderElection(ctx, testLeaderElectionConfig(fake.NewClientset()), func(context.Context) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("runWithLeaderElection() error = %v, want %v", err, boom)
	}
}

// newLeaseClientWithOutage returns a client whose lease updates fail once the returned flag is set.
func newLeaseClientWithOutage() (*fake.Clientset, *atomic.Bool) {
	apiserverDown := &atomic.Bool{}
	client := fake.NewClientset()
	client.PrependReactor("update", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !apiserverDown.Load() {
			return false, nil, nil
		}
		return true, nil, errors.New("apiserver unavailable")
	})
	return client, apiserverDown
}

func TestRunWithLeaderElectionReportsLostLeadership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, apiserverDown := newLeaseClientWithOutage()
	var runReturned atomic.Bool
	err := runWithLeaderElection(ctx, testLeaderElectionConfig(client), func(runCtx context.Context) error {
		apiserverDown.Store(true)
		<-runCtx.Done()
		time.Sleep(200 * time.Millisecond)
		runReturned.Store(true)
		return nil
	})
	if !errors.Is(err, errLeaderElectionLost) {
		t.Fatalf("runWithLeaderElection() error = %v, want %v", err, errLeaderElectionLost)
	}
	if !runReturned.Load() {
		t.Fatal("runWithLeaderElection() returned while run was still executing")
	}
}

func TestRunWithLeaderElectionReportsLostLeadershipWhenRunReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, apiserverDown := newLeaseClientWithOutage()
	err := runWithLeaderElection(ctx, testLeaderElectionConfig(client), func(runCtx context.Context) error {
		apiserverDown.Store(true)
		<-runCtx.Done()
		return nil
	})
	if !errors.Is(err, errLeaderElectionLost) {
		t.Fatalf("runWithLeaderElection() error = %v, want %v", err, errLeaderElectionLost)
	}
}

func TestRunWithLeaderElectionReleasesAfterRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := fake.NewClientset()
	holder := func() string {
		lease, err := client.CoordinationV1().Leases("kube-system").Get(context.Background(), "kwok-controller", metav1.GetOptions{})
		if err != nil {
			t.Errorf("failed to get lease: %v", err)
			return ""
		}
		if lease.Spec.HolderIdentity == nil {
			return "<nil>"
		}
		return *lease.Spec.HolderIdentity
	}

	err := runWithLeaderElection(ctx, testLeaderElectionConfig(client), func(runCtx context.Context) error {
		cancel()
		<-runCtx.Done()
		if got := holder(); got != "test" {
			t.Errorf("lease holder once run is canceled = %q, want %q", got, "test")
		}
		time.Sleep(600 * time.Millisecond)
		if got := holder(); got != "test" {
			t.Errorf("lease holder while run winds down = %q, want %q", got, "test")
		}
		return runCtx.Err()
	})
	if err != nil {
		t.Fatalf("runWithLeaderElection() error = %v, want nil on shutdown", err)
	}
	if got := holder(); got != "" {
		t.Errorf("lease holder after return = %q, want released (empty)", got)
	}
}

func TestLeaderElectionNamespace(t *testing.T) {
	dir := t.TempDir()
	namespaceFile := filepath.Join(dir, "namespace")
	err := os.WriteFile(namespaceFile, []byte("kwok-system\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	blankFile := filepath.Join(dir, "blank")
	err = os.WriteFile(blankFile, []byte(" \n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		configured    string
		namespaceFile string
		want          string
	}{
		{name: "configured wins", configured: "custom", namespaceFile: namespaceFile, want: "custom"},
		{name: "in-cluster namespace", namespaceFile: namespaceFile, want: "kwok-system"},
		{name: "outside a cluster", namespaceFile: filepath.Join(dir, "missing"), want: "kube-system"},
		{name: "blank namespace file", namespaceFile: blankFile, want: "kube-system"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := leaderElectionNamespace(tt.configured, tt.namespaceFile); got != tt.want {
				t.Errorf("leaderElectionNamespace() = %q, want %q", got, tt.want)
			}
		})
	}
}
