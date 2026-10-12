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
	"net"
	"net/http"
	"sync"
	"time"

	"sigs.k8s.io/kwok/pkg/log"
)

// serveStandbyHealthz serves only the health endpoints on address while this instance waits for leadership.
// The returned stop function shuts the server down and frees the address.
func serveStandbyHealthz(ctx context.Context, address string) (stop func(), err error) {
	logger := log.FromContext(ctx)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	ok := func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write([]byte("ok"))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)
	mux.HandleFunc("/livez", ok)

	svc := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           mux,
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		err := svc.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to serve standby healthz",
				"err", err,
			)
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = svc.Shutdown(shutdownCtx)
			// Shutdown leaves the listener open if Serve has not started tracking it yet.
			_ = listener.Close()
			// Serve may be the one closing the listener, so the address is free only once it returns.
			<-served
		})
	}, nil
}
