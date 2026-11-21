package driver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	spotv1 "github.com/rackspace-spot/spot-go-sdk/api/v1"
	"github.com/rancher/kontainer-engine/types"
)

// TestCreateSendsSpotAPIPayload drives the same calls Create() makes after
// authentication, against the real Spot SDK and a local API. Rancher passes
// camelCase engine-config keys, and number inputs arrive as strings.
func TestCreateSendsSpotAPIPayload(t *testing.T) {
	var mu sync.Mutex
	type call struct {
		method string
		path   string
		body   []byte
	}
	var calls []call
	created := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls = append(calls, call{method: r.Method, path: r.URL.Path, body: body})
		isCreated := created
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cloudspaces") {
			created = true
		}
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/apis/auth.ngpc.rxt.io/v1/organizations":
			_, _ = w.Write([]byte(`{"organizations":[{"name":"acme","id":"acme"}]}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cloudspaces/"):
			if !isCreated {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"cloudspace not found"}`))
				return
			}
			_, _ = w.Write([]byte(`{"metadata":{"name":"my-cluster","namespace":"acme"},"spec":{"region":"us-east-iad-1","kubernetesVersion":"1.35.9","cni":"calico","deploymentType":"gen2"},"status":{"phase":"Pending"}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cloudspaces"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/spotnodepools"):
			_, _ = w.Write([]byte(`{"items":[]}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/ondemandnodepools"):
			_, _ = w.Write([]byte(`{"items":[]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/ngpc.rxt.io/v1/serverclasses/"):
			_, _ = w.Write([]byte(`{"metadata":{"name":"gp.vs1.medium-iad"},"spec":{"availability":"available","region":"us-east-iad-1","minBidPricePerHour":"0.001","resources":{"cpu":"2","memory":"4GB"}},"status":{"spotPricing":{"marketPricePerHour":"0.008"}}}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/spotnodepools"):
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.RequestURI())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// NewSpotClient applies envconfig defaults even when BaseURL is already set,
	// so the test server has to be selected through SPOT_BASE_URL.
	t.Setenv("SPOT_BASE_URL", server.URL)
	api, err := spotv1.NewSpotClient(&spotv1.Config{
		BaseURL:     server.URL,
		AccessToken: testAccessToken(),
		HTTPClient:  server.Client(),
		RetryConfig: spotv1.RetryConfig{MaxRetries: 1, InitialInterval: time.Millisecond, RetryWaitMax: time.Millisecond, Multiplier: 1},
	})
	if err != nil {
		t.Fatalf("NewSpotClient() error = %v", err)
	}

	state, err := stateFromOptions(&types.DriverOptions{
		StringOptions: map[string]string{
			"name":                      "My Cluster",
			"rackspaceSpotRefreshToken": "refresh-token",
			"rackspaceSpotOrganization": "acme",
			"rackspaceSpotRegion":       "us-east-iad-1",
			"kubernetesVersion":         "1.35.9",
			"cni":                       "calico",
			"spotServerClass":           "gp.vs1.medium-iad",
			"spotBidPrice":              "0.02",
			"spotNodeCount":             "5",
		},
	})
	if err != nil {
		t.Fatalf("stateFromOptions() error = %v", err)
	}
	state.CloudspaceName, err = sanitizeResourceName("My Cluster")
	if err != nil {
		t.Fatalf("sanitizeResourceName() error = %v", err)
	}

	client := &spotClient{api: api, org: state.Organization}
	ctx := context.Background()
	if _, err := client.ensureCloudspace(ctx, state); err != nil {
		t.Fatalf("ensureCloudspace() error = %v", err)
	}
	if err := client.reconcileSpotNodePools(ctx, state); err != nil {
		t.Fatalf("reconcileSpotNodePools() error = %v", err)
	}
	if err := client.reconcileOnDemandNodePool(ctx, state); err != nil {
		t.Fatalf("reconcileOnDemandNodePool() error = %v", err)
	}
	if err := client.reconcileAutopilotNodePools(ctx, state); err != nil {
		t.Fatalf("reconcileAutopilotNodePools() error = %v", err)
	}

	var cloudspace, pool map[string]any
	for _, recorded := range calls {
		if recorded.method != http.MethodPost || len(recorded.body) == 0 {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(recorded.body, &payload); err != nil {
			t.Fatalf("decode %s body: %v", recorded.path, err)
		}
		switch payload["kind"] {
		case "CloudSpace":
			cloudspace = payload
		case "SpotNodePool":
			pool = payload
		default:
			t.Fatalf("unexpected POST %s kind %v", recorded.path, payload["kind"])
		}
	}
	if cloudspace == nil || pool == nil {
		t.Fatalf("missing create requests, calls=%d", len(calls))
	}

	if cloudspace["apiVersion"] != "ngpc.rxt.io/v1" || pool["apiVersion"] != "ngpc.rxt.io/v1" {
		t.Fatalf("apiVersion cloudspace=%v pool=%v", cloudspace["apiVersion"], pool["apiVersion"])
	}
	meta := cloudspace["metadata"].(map[string]any)
	if meta["name"] != "my-cluster" || meta["namespace"] != "acme" {
		t.Fatalf("cloudspace metadata = %+v", meta)
	}
	spec := cloudspace["spec"].(map[string]any)
	if spec["region"] != "us-east-iad-1" || spec["kubernetesVersion"] != "1.35.9" || spec["cni"] != "calico" || spec["deploymentType"] != "gen2" {
		t.Fatalf("cloudspace spec = %+v", spec)
	}
	poolSpec := pool["spec"].(map[string]any)
	if poolSpec["serverClass"] != "gp.vs1.medium-iad" || poolSpec["bidPrice"] != "0.02" || poolSpec["desired"] != float64(5) || poolSpec["cloudSpace"] != "my-cluster" {
		t.Fatalf("spot pool spec = %+v", poolSpec)
	}
	if strings.HasPrefix(poolSpec["bidPrice"].(string), "$") {
		t.Fatalf("bid price %q includes the read-only $ prefix", poolSpec["bidPrice"])
	}
	poolMeta := pool["metadata"].(map[string]any)
	labels := poolMeta["labels"].(map[string]any)
	if labels["ngpc.rxt.io/cloudspace"] != "my-cluster" {
		t.Fatalf("spot pool labels = %+v", labels)
	}
}

func testAccessToken() string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())))
	return header + "." + payload + ".x"
}
