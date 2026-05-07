package graphcontroller_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ---------------------------------------------------------------------------
// Gauge nodes — prometheus gauge driven by CEL evaluation
// ---------------------------------------------------------------------------

// TestGaugeBasic proves that a gauge node with no labels emits a single
// prometheus gauge whose value equals len(source list).
func TestGaugeBasic(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create some ConfigMaps that the watch will observe.
	for i := 0; i < 3; i++ {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-test-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-basic"},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-basic",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-basic",
							},
						},
					},
					map[string]any{
						"id": "cmCount",
						"gauge": map[string]any{
							"name": "kro_test_gauge_basic_total",
							"expr": "${cms}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-basic", Namespace: ns}))

	// Verify the gauge is exposed on the metrics endpoint.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_basic_total", "3"))
}

// TestGaugeWithLabels proves that a gauge node with labels slices the source
// list by per-item CEL label expressions and emits one gauge series per
// unique label combination, each with value = count of items in that group.
func TestGaugeWithLabels(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps with different "tier" labels to test grouping.
	tiers := []string{"frontend", "frontend", "backend", "backend", "backend"}
	for i, tier := range tiers {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-labels-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-labels", "tier": tier},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-labels",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-labels",
							},
						},
					},
					map[string]any{
						"id": "cmByTier",
						"gauge": map[string]any{
							"name": "kro_test_gauge_by_tier",
							"expr": "${cms}",
							"labels": map[string]any{
								"tier": "${item.metadata.labels.tier}",
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-labels", Namespace: ns}))

	// Verify the gauge is exposed with correct label dimensions.
	require.NoError(t, waitForMetric(t, `kro_test_gauge_by_tier{tier="frontend"}`, "2"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_by_tier{tier="backend"}`, "3"))
}

// TestGaugeWithFilter proves that filtering the source list with CEL works.
func TestGaugeWithFilter(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps — some with "active" label, some without.
	for i := 0; i < 5; i++ {
		labels := map[string]any{"test": "gauge-filter"}
		if i < 3 {
			labels["active"] = "true"
		}
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-filter-cm-%d", i),
					"namespace": ns,
					"labels":    labels,
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-filter",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-filter",
							},
						},
					},
					map[string]any{
						"id": "activeCms",
						"gauge": map[string]any{
							"name": "kro_test_gauge_filter_active",
							"expr": "${cms.filter(c, has(c.metadata.labels.active))}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-filter", Namespace: ns}))

	// Only the 3 active ConfigMaps should be counted.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_filter_active", "3"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// waitForMetric polls the controller's /metrics endpoint until the given
// metric line appears with the expected value. The metricMatch is a
// substring that must appear on the same line as the value.
func waitForMetric(t *testing.T, metricMatch string, expectedValue string) error {
	t.Helper()
	if metricsAddr == "" {
		t.Skip("metrics endpoint not available")
	}

	return wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx2 context.Context) (bool, error) {
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			return false, nil // retry
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, nil
		}

		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.Contains(line, metricMatch) {
				// Line format: metric_name{labels} value
				// or: metric_name value
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					actual := parts[len(parts)-1]
					if actual == expectedValue {
						return true, nil
					}
					t.Logf("metric %q found but value=%s, want %s", metricMatch, actual, expectedValue)
				}
				return false, nil
			}
		}
		return false, nil // metric not found yet
	})
}

// TestGaugeReactive proves that gauge values update when watched resources change.
// Exercises add → add → delete lifecycle.
func TestGaugeReactive(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-reactive",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-reactive",
							},
						},
					},
					map[string]any{
						"id": "cmCount",
						"gauge": map[string]any{
							"name": "kro_test_gauge_reactive_total",
							"expr": "${cms}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-reactive", Namespace: ns}))

	// Initially 0 items.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_reactive_total", "0"))

	// Add first ConfigMap → gauge should go to 1.
	cm1 := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-reactive-cm-1",
				"namespace": ns,
				"labels":    map[string]any{"test": "gauge-reactive"},
			},
			"data": map[string]any{"key": "value"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, cm1))
	require.NoError(t, waitForMetric(t, "kro_test_gauge_reactive_total", "1"))

	// Add second ConfigMap → gauge should go to 2.
	cm2 := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-reactive-cm-2",
				"namespace": ns,
				"labels":    map[string]any{"test": "gauge-reactive"},
			},
			"data": map[string]any{"key": "value"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, cm2))
	require.NoError(t, waitForMetric(t, "kro_test_gauge_reactive_total", "2"))

	// Delete first ConfigMap → gauge should drop back to 1.
	require.NoError(t, k8sClient.Delete(ctx, cm1))
	require.NoError(t, waitForMetric(t, "kro_test_gauge_reactive_total", "1"))
}

// TestGaugeStaleDimensionCleanup proves that when a label combination disappears
// (e.g., all items with tier=frontend are deleted), the corresponding gauge
// series is removed from the metrics endpoint.
func TestGaugeStaleDimensionCleanup(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps with two tiers.
	for i, tier := range []string{"alpha", "beta"} {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-stale-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-stale", "tier": tier},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-stale",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-stale",
							},
						},
					},
					map[string]any{
						"id": "byTier",
						"gauge": map[string]any{
							"name": "kro_test_gauge_stale_by_tier",
							"expr": "${cms}",
							"labels": map[string]any{
								"tier": "${item.metadata.labels.tier}",
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-stale", Namespace: ns}))

	// Both dimensions should exist.
	require.NoError(t, waitForMetric(t, `kro_test_gauge_stale_by_tier{tier="alpha"}`, "1"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_stale_by_tier{tier="beta"}`, "1"))

	// Delete the "beta" ConfigMap.
	betaCM := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-stale-cm-1",
				"namespace": ns,
			},
		},
	}
	require.NoError(t, k8sClient.Delete(ctx, betaCM))

	// The "beta" series should disappear, "alpha" should remain.
	require.NoError(t, waitForMetricAbsence(t, `kro_test_gauge_stale_by_tier{tier="beta"}`))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_stale_by_tier{tier="alpha"}`, "1"))
}

// waitForMetricAbsence polls until the given metric line is no longer present
// on the /metrics endpoint.
func waitForMetricAbsence(t *testing.T, metricMatch string) error {
	t.Helper()
	if metricsAddr == "" {
		t.Skip("metrics endpoint not available")
	}

	return wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx2 context.Context) (bool, error) {
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, nil
		}

		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.Contains(line, metricMatch) {
				return false, nil // still present, keep polling
			}
		}
		return true, nil // absent
	})
}

// TestGaugeForEach proves that gauge nodes work inside forEach — each
// forEach iteration evaluates the gauge with its own scope, and all
// children contribute to the same GaugeVec with distinct label values.
func TestGaugeForEach(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps in two "environments".
	for i, env := range []string{"staging", "staging", "prod", "prod", "prod"} {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-foreach-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-foreach", "env": env},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-foreach",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-foreach",
							},
						},
					},
					// forEach over environments, count CMs per env
					map[string]any{
						"id": "countPerEnv",
						"forEach": map[string]any{
							"env": "${['staging', 'prod']}",
						},
						"gauge": map[string]any{
							"name": "kro_test_gauge_foreach_count",
							"expr": "${cms.filter(c, c.metadata.labels.env == env)}",
							"labels": map[string]any{
								"environment": "${item.metadata.labels.env}",
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-foreach", Namespace: ns}))

	// Verify: staging=2, prod=3
	require.NoError(t, waitForMetric(t, `kro_test_gauge_foreach_count{environment="staging"}`, "2"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_foreach_count{environment="prod"}`, "3"))
}

// TestGaugeIncludeWhen proves that a gauge node with includeWhen=false is
// excluded and emits no metrics. When the condition flips to true, the
// gauge activates.
func TestGaugeIncludeWhen(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create a "gate" ConfigMap that controls inclusion.
	gate := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-gate",
				"namespace": ns,
				"labels":    map[string]any{"test": "gauge-includewhen"},
			},
			"data": map[string]any{"enabled": "false"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, gate))

	// Create some items to count.
	for i := 0; i < 2; i++ {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-iw-item-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-includewhen-item"},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-includewhen",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "gateRef",
						"ref": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"name":      "gauge-gate",
								"namespace": ns,
							},
						},
					},
					map[string]any{
						"id": "items",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-includewhen-item",
							},
						},
					},
					map[string]any{
						"id": "conditionalGauge",
						"includeWhen": []any{
							"${gateRef.data.enabled == 'true'}",
						},
						"gauge": map[string]any{
							"name": "kro_test_gauge_includewhen",
							"expr": "${items}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-includewhen", Namespace: ns}))

	// Gauge should NOT be emitted (node is excluded).
	require.NoError(t, waitForMetricAbsence(t, "kro_test_gauge_includewhen"))

	// Flip the gate to true.
	require.NoError(t, updateWithRetry(ctx, k8sClient,
		schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		types.NamespacedName{Name: "gauge-gate", Namespace: ns},
		func(obj *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(obj.Object, "true", "data", "enabled")
		}))

	// Now the gauge should appear with value = 2.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_includewhen", "2"))
}

// TestGaugePropagateWhen proves that a gauge node with propagateWhen
// unsatisfied retains its previous state (no evaluation) until the
// condition is met.
func TestGaugePropagateWhen(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create a "gate" ConfigMap.
	gate := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-pw-gate",
				"namespace": ns,
				"labels":    map[string]any{"test": "gauge-propagatewhen"},
			},
			"data": map[string]any{"ready": "false"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, gate))

	// Create items to count.
	for i := 0; i < 3; i++ {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-pw-item-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-propagatewhen-item"},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-propagatewhen",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "gateRef",
						"ref": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"name":      "gauge-pw-gate",
								"namespace": ns,
							},
						},
					},
					map[string]any{
						"id": "items",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-propagatewhen-item",
							},
						},
					},
					map[string]any{
						"id": "gatedGauge",
						"propagateWhen": []any{
							"${gateRef.data.ready == 'true'}",
						},
						"gauge": map[string]any{
							"name": "kro_test_gauge_propagatewhen",
							"expr": "${items}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	// Graph won't be Ready because the gauge is Pending (propagateWhen unsatisfied).
	// Wait for compiled, then verify the metric is absent.
	require.NoError(t, waitForGraphCompiledStatus(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-propagatewhen", Namespace: ns}, "True"))
	require.NoError(t, waitForMetricAbsence(t, "kro_test_gauge_propagatewhen"))

	// Flip the gate — gauge should now evaluate.
	require.NoError(t, updateWithRetry(ctx, k8sClient,
		schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		types.NamespacedName{Name: "gauge-pw-gate", Namespace: ns},
		func(obj *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(obj.Object, "true", "data", "ready")
		}))

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-propagatewhen", Namespace: ns}))
	require.NoError(t, waitForMetric(t, "kro_test_gauge_propagatewhen", "3"))
}

// TestGaugeDeletion proves that when a Graph is deleted, its gauges are
// removed from the /metrics endpoint (no stale metrics left behind).
func TestGaugeDeletion(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create items to count.
	for i := 0; i < 2; i++ {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-del-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-deletion"},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-deletion",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-deletion",
							},
						},
					},
					map[string]any{
						"id": "count",
						"gauge": map[string]any{
							"name": "kro_test_gauge_deletion_total",
							"expr": "${cms}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-deletion", Namespace: ns}))

	// Gauge should be present.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_deletion_total", "2"))

	// Delete the Graph.
	require.NoError(t, k8sClient.Delete(ctx, graph))

	// Wait for the graph to be gone.
	require.NoError(t, waitForDeletion(ctx, k8sClient,
		schema.GroupVersionKind{Group: "experimental.kro.run", Version: "v1alpha1", Kind: "Graph"},
		types.NamespacedName{Name: "test-gauge-deletion", Namespace: ns}))

	// The metric should disappear from /metrics.
	require.NoError(t, waitForMetricAbsence(t, "kro_test_gauge_deletion_total"))
}

// TestGaugeMultiple proves that multiple gauge nodes in the same Graph
// coexist without interfering — each emits its own metric independently.
func TestGaugeMultiple(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps with different labels.
	for i, color := range []string{"red", "red", "blue"} {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-multi-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-multiple", "color": color},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-multiple",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-multiple",
							},
						},
					},
					// First gauge: total count, no labels.
					map[string]any{
						"id": "totalCount",
						"gauge": map[string]any{
							"name": "kro_test_gauge_multi_total",
							"expr": "${cms}",
						},
					},
					// Second gauge: count by color label.
					map[string]any{
						"id": "byColor",
						"gauge": map[string]any{
							"name": "kro_test_gauge_multi_by_color",
							"expr": "${cms}",
							"labels": map[string]any{
								"color": "${item.metadata.labels.color}",
							},
						},
					},
					// Third gauge: count filtered to red only.
					map[string]any{
						"id": "redOnly",
						"gauge": map[string]any{
							"name": "kro_test_gauge_multi_red",
							"expr": "${cms.filter(c, c.metadata.labels.color == 'red')}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-multiple", Namespace: ns}))

	// All three gauges should emit independently.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_multi_total", "3"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multi_by_color{color="red"}`, "2"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multi_by_color{color="blue"}`, "1"))
	require.NoError(t, waitForMetric(t, "kro_test_gauge_multi_red", "2"))
}

// TestGaugeWithDef proves that a gauge node can reference a def node's
// computed values — the gauge expr operates on scope data from upstream
// computations, not just raw watch outputs.
func TestGaugeWithDef(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps.
	for i := 0; i < 4; i++ {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-def-cm-%d", i),
					"namespace": ns,
					"labels":    map[string]any{"test": "gauge-def", "active": fmt.Sprintf("%t", i < 3)},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-def",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-def",
							},
						},
					},
					// Def node computes a filtered list inside a map.
					map[string]any{
						"id": "computed",
						"def": map[string]any{
							"active": "${cms.filter(c, c.metadata.labels.active == 'true')}",
						},
					},
					// Gauge references the def node's computed list.
					map[string]any{
						"id": "activeCount",
						"gauge": map[string]any{
							"name": "kro_test_gauge_def_active",
							"expr": "${computed.active}",
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-def", Namespace: ns}))

	// Only 3 of the 4 CMs have active=true.
	require.NoError(t, waitForMetric(t, "kro_test_gauge_def_active", "3"))
}

// TestGaugeMultipleLabels proves that a gauge can slice data across multiple
// dimensions simultaneously, producing the cross-product of label values.
// Verifies add/delete transitions update the correct dimension intersections.
func TestGaugeMultipleLabels(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	// Create ConfigMaps spanning a 2D matrix: env × tier
	// staging/frontend: 1
	// staging/backend:  2
	// prod/frontend:    1
	// prod/backend:     1
	items := []struct {
		env, tier string
	}{
		{"staging", "frontend"},
		{"staging", "backend"},
		{"staging", "backend"},
		{"prod", "frontend"},
		{"prod", "backend"},
	}
	for i, item := range items {
		cm := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("gauge-ml-cm-%d", i),
					"namespace": ns,
					"labels": map[string]any{
						"test": "gauge-multilabel",
						"env":  item.env,
						"tier": item.tier,
					},
				},
				"data": map[string]any{"key": "value"},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, cm))
	}

	graph := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata": map[string]any{
				"name":      "test-gauge-multilabel",
				"namespace": ns,
			},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id": "cms",
						"watch": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"namespace": ns,
							},
							"selector": map[string]any{
								"test": "gauge-multilabel",
							},
						},
					},
					map[string]any{
						"id": "byEnvTier",
						"gauge": map[string]any{
							"name": "kro_test_gauge_multilabel",
							"expr": "${cms}",
							"labels": map[string]any{
								"env":  "${item.metadata.labels.env}",
								"tier": "${item.metadata.labels.tier}",
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, graph) })

	require.NoError(t, waitForGraphReady(ctx, k8sClient,
		types.NamespacedName{Name: "test-gauge-multilabel", Namespace: ns}))

	// Verify all 4 dimension intersections.
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="staging",tier="frontend"}`, "1"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="staging",tier="backend"}`, "2"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="prod",tier="frontend"}`, "1"))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="prod",tier="backend"}`, "1"))

	// Add another prod/frontend item — that cell should go from 1 → 2.
	cm := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-ml-cm-new",
				"namespace": ns,
				"labels": map[string]any{
					"test": "gauge-multilabel",
					"env":  "prod",
					"tier": "frontend",
				},
			},
			"data": map[string]any{"key": "value"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, cm))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="prod",tier="frontend"}`, "2"))

	// Delete one staging/backend — that cell goes from 2 → 1.
	toDelete := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "gauge-ml-cm-1",
				"namespace": ns,
			},
		},
	}
	require.NoError(t, k8sClient.Delete(ctx, toDelete))
	require.NoError(t, waitForMetric(t, `kro_test_gauge_multilabel{env="staging",tier="backend"}`, "1"))
}
