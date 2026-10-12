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

package v1alpha1

import (
	"testing"
)

func TestKwokConfigurationLeaderElectionDefaults(t *testing.T) {
	conf := &KwokConfiguration{}
	SetObjectDefaults_KwokConfiguration(conf)

	le := conf.Options.LeaderElection
	if le.LeaderElect == nil || *le.LeaderElect {
		t.Errorf("leaderElect = %v, want false", le.LeaderElect)
	}
	if le.ResourceName != "kwok-controller" {
		t.Errorf("resourceName = %q, want %q", le.ResourceName, "kwok-controller")
	}
	if le.ResourceNamespace != "" {
		t.Errorf("resourceNamespace = %q, want empty", le.ResourceNamespace)
	}
}
