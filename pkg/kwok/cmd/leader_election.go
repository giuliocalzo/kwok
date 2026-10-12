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
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"sigs.k8s.io/kwok/pkg/log"
)

const (
	leaderElectionLeaseDuration = 15 * time.Second
	leaderElectionRenewDeadline = 10 * time.Second
	leaderElectionRetryPeriod   = 2 * time.Second

	inClusterNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

var errLeaderElectionLost = errors.New("leader election lost")

type leaderElectionConfig struct {
	Client        kubernetes.Interface
	Name          string
	Namespace     string
	Identity      string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// runWithLeaderElection blocks until this instance is elected, then calls run with a context
// that is canceled when ctx is canceled or leadership is lost.
// It never returns while run is executing, and run is not started once ctx is canceled or leadership is lost.
// On shutdown, or when run returns while still leading, the lease is released only after run has returned.
// It returns errLeaderElectionLost if leadership is lost, nil if ctx is canceled, and run's result otherwise.
func runWithLeaderElection(ctx context.Context, conf leaderElectionConfig, run func(ctx context.Context) error) error {
	logger := log.FromContext(ctx)

	// The elector outlives ctx so that ReleaseOnCancel releases the lease only after run has returned.
	electionCtx, stopElection := context.WithCancel(context.WithoutCancel(ctx))
	defer stopElection()

	type runResult struct {
		err  error
		lost bool
	}

	var (
		mut      sync.Mutex
		started  bool
		finished bool
	)
	runDone := make(chan runResult, 1)

	stopWaiting := context.AfterFunc(ctx, func() {
		mut.Lock()
		defer mut.Unlock()
		if !started {
			stopElection()
		}
	})
	defer stopWaiting()

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      conf.Name,
				Namespace: conf.Namespace,
			},
			Client: conf.Client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: conf.Identity,
			},
		},
		LeaseDuration:   conf.LeaseDuration,
		RenewDeadline:   conf.RenewDeadline,
		RetryPeriod:     conf.RetryPeriod,
		ReleaseOnCancel: true,
		Name:            conf.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadingCtx context.Context) {
				mut.Lock()
				if finished || ctx.Err() != nil || leadingCtx.Err() != nil {
					mut.Unlock()
					return
				}
				started = true
				mut.Unlock()

				runCtx, cancel := context.WithCancel(leadingCtx)
				defer cancel()
				stopOnShutdown := context.AfterFunc(ctx, cancel)
				defer stopOnShutdown()

				err := run(runCtx)
				// Until stopElection below, leadingCtx is canceled only by Run when leadership is lost.
				runDone <- runResult{err: err, lost: leadingCtx.Err() != nil}
				stopElection()
			},
			OnStoppedLeading: func() {
				mut.Lock()
				defer mut.Unlock()
				if started {
					logger.Info("Stopped leading")
				}
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	elector.Run(electionCtx)
	// Until run starts, only the parent stops the elector, so a live electionCtx means leadership was lost.
	result := runResult{lost: electionCtx.Err() == nil}

	mut.Lock()
	finished = true
	wasStarted := started
	mut.Unlock()

	if wasStarted {
		result = <-runDone
	}
	switch {
	case result.lost:
		return errLeaderElectionLost
	case ctx.Err() != nil:
		return nil
	default:
		return result.err
	}
}

// leaderElectionNamespace returns the configured namespace, else the namespace kwok runs in, else kube-system.
func leaderElectionNamespace(configured, namespaceFile string) string {
	if configured != "" {
		return configured
	}
	data, err := os.ReadFile(namespaceFile)
	if err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace
		}
	}
	return metav1.NamespaceSystem
}
