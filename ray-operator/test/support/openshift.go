package support

import (
	"testing"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// RequireOpenShift skips the test when the cluster is not OpenShift (e.g. Kind / upstream CI).
func RequireOpenShift(t *testing.T, test Test) {
	t.Helper()
	cfg := test.Client().Config()
	isOCP, err := utils.IsOpenShiftCluster(&cfg)
	if err != nil {
		t.Fatalf("detect OpenShift platform: %v", err)
	}
	if !isOCP {
		t.Skip("OpenShift API groups not found; skipping managed NetworkPolicy e2e")
	}
}
