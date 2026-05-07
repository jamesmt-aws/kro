// gauge.go implements the gauge: node type — a prometheus gauge driven by
// CEL evaluation. The gauge value is always len(group) after slicing the
// source list by label dimensions. Propagation-driven: re-evaluates when
// upstream dependencies change.
package graphcontroller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ellistarn/kro/experimental/controller/graph"
)

// ---------------------------------------------------------------------------
// GaugeStore — per-controller lifecycle for prometheus gauges
// ---------------------------------------------------------------------------

// GaugeStore manages prometheus GaugeVec registrations across Graph instances.
// Gauges persist across reconcile cycles (stateless evaluator, stateful gauge).
// Thread-safe — multiple reconcile workers may update concurrently.
//
// Registers with a provided prometheus.Registerer (typically the default
// registry used by controller-runtime's /metrics endpoint) so gauges are
// automatically exposed without additional HTTP plumbing.
type GaugeStore struct {
	mu         sync.Mutex
	registerer prometheus.Registerer

	// gauges maps (graphKey, metricName) → registered gauge state.
	// graphKey is "namespace/name" of the Graph object.
	gauges map[gaugeKey]*gaugeState
}

type gaugeKey struct {
	GraphKey   string // "namespace/name"
	MetricName string // prometheus metric name
}

type gaugeState struct {
	gauge       *prometheus.GaugeVec
	labelNames  []string            // sorted label names (registration order)
	knownLabels []prometheus.Labels // previous label combinations (for stale cleanup)
}

// NewGaugeStore creates a GaugeStore that registers gauges with the given
// prometheus.Registerer. Pass prometheus.DefaultRegisterer to expose gauges
// on the controller-runtime /metrics endpoint.
func NewGaugeStore(registerer prometheus.Registerer) *GaugeStore {
	return &GaugeStore{
		registerer: registerer,
		gauges:     make(map[gaugeKey]*gaugeState),
	}
}

// Cleanup removes all gauges registered for a given Graph key.
// Called when a Graph is deleted.
func (s *GaugeStore) Cleanup(graphKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, state := range s.gauges {
		if key.GraphKey == graphKey {
			s.registerer.Unregister(state.gauge)
			delete(s.gauges, key)
		}
	}
}

// getOrCreate returns the GaugeVec for the given key, creating and registering
// it if necessary. If the label set has changed (gauge definition mutated),
// the old gauge is unregistered and a new one is created. Returns an error
// if registration fails (e.g., metric name collision with another Graph).
func (s *GaugeStore) getOrCreate(key gaugeKey, labelNames []string) (*gaugeState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.gauges[key]; ok {
		// Check if label names match — if not, re-register
		if labelsMatch(existing.labelNames, labelNames) {
			return existing, nil
		}
		// Label set changed — unregister old, create new
		s.registerer.Unregister(existing.gauge)
		delete(s.gauges, key)
	}

	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: key.MetricName,
			Help: fmt.Sprintf("Graph gauge: %s (graph: %s)", key.MetricName, key.GraphKey),
		},
		labelNames,
	)
	if err := s.registerer.Register(gauge); err != nil {
		return nil, fmt.Errorf("registering metric %q: %w", key.MetricName, err)
	}

	state := &gaugeState{
		gauge:      gauge,
		labelNames: labelNames,
	}
	s.gauges[key] = state
	return state, nil
}

func labelsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// reconcileGauge — node handler
// ---------------------------------------------------------------------------

// reconcileGauge evaluates a gauge node: evaluates the source expression to
// get a list, groups items by label dimensions, and sets the gauge value per
// group to len(group). Stale label combinations are cleaned up.
func reconcileGauge(ctx context.Context, node graph.Node, eval *evaluator, store *GaugeStore, graphKey string) error {
	logger := log.FromContext(ctx)
	gauge := node.Gauge
	if gauge == nil {
		return fmt.Errorf("gauge node %s: missing GaugeBody", node.ID)
	}

	// Evaluate the source expression — must return a list.
	sourceVal, err := eval.evalString(gauge.Expr)
	if err != nil {
		return fmt.Errorf("gauge %s: evaluating expr: %w", node.ID, err)
	}
	items, ok := sourceVal.([]any)
	if !ok {
		return fmt.Errorf("gauge %s: expr must return a list, got %T", node.ID, sourceVal)
	}

	// Sorted label names for consistent registration and emission order.
	labelNames := sortedKeys(gauge.Labels)

	// Get or create the prometheus GaugeVec.
	key := gaugeKey{GraphKey: graphKey, MetricName: gauge.Name}
	state, err := store.getOrCreate(key, labelNames)
	if err != nil {
		return fmt.Errorf("gauge %s: %w", node.ID, err)
	}

	if len(labelNames) == 0 {
		// No labels — single gauge value = len(items).
		state.gauge.WithLabelValues().Set(float64(len(items)))
		eval.markUpdated(node.ID, true)
		logger.V(1).Info("gauge evaluated", "node", node.ID, "name", gauge.Name, "value", len(items))
		return nil
	}

	// Group items by label dimensions.
	groups, err := groupByLabels(items, gauge.Labels, labelNames, eval)
	if err != nil {
		return fmt.Errorf("gauge %s: grouping by labels: %w", node.ID, err)
	}

	// Collect current label combinations for stale cleanup.
	var currentLabels []prometheus.Labels
	for _, g := range groups {
		currentLabels = append(currentLabels, g.labels)
	}

	// Clean up stale dimensions BEFORE setting new ones.
	cleanStaleDimensions(state, currentLabels, labelNames)

	// Set gauge values per group.
	for _, g := range groups {
		state.gauge.With(g.labels).Set(float64(g.count))
	}
	state.knownLabels = currentLabels

	logger.V(1).Info("gauge evaluated", "node", node.ID, "name", gauge.Name, "groups", len(groups), "totalItems", len(items))
	// Gauges are always re-evaluated — vacuously updated (like def nodes).
	eval.markUpdated(node.ID, true)
	return nil
}

// reconcileForEachGaugeChild evaluates a gauge for a single forEach child.
// Unlike reconcileGauge, this does NOT perform stale dimension cleanup —
// multiple forEach children contribute to the same GaugeVec, so cleanup
// must wait until all children have been processed. The forEach orchestrator
// accumulates labels across children; prometheus automatically merges
// label combinations from different calls.
func reconcileForEachGaugeChild(ctx context.Context, node graph.Node, eval *evaluator, store *GaugeStore, graphKey string) error {
	logger := log.FromContext(ctx)
	gauge := node.Gauge
	if gauge == nil {
		return fmt.Errorf("gauge node %s: missing GaugeBody", node.ID)
	}

	sourceVal, err := eval.evalString(gauge.Expr)
	if err != nil {
		return fmt.Errorf("gauge %s: evaluating expr: %w", node.ID, err)
	}
	items, ok := sourceVal.([]any)
	if !ok {
		return fmt.Errorf("gauge %s: expr must return a list, got %T", node.ID, sourceVal)
	}

	labelNames := sortedKeys(gauge.Labels)
	key := gaugeKey{GraphKey: graphKey, MetricName: gauge.Name}
	state, err := store.getOrCreate(key, labelNames)
	if err != nil {
		return fmt.Errorf("gauge %s: %w", node.ID, err)
	}

	if len(labelNames) == 0 {
		// No labels — accumulate: add this child's count to the gauge.
		// For forEach without labels, each child's items are additive.
		// Use Add instead of Set so multiple children contribute.
		state.gauge.WithLabelValues().Add(float64(len(items)))
		logger.V(1).Info("forEach gauge child evaluated", "node", node.ID, "name", gauge.Name, "childItems", len(items))
		return nil
	}

	groups, err := groupByLabels(items, gauge.Labels, labelNames, eval)
	if err != nil {
		return fmt.Errorf("gauge %s: grouping by labels: %w", node.ID, err)
	}

	// Set gauge values per group — no stale cleanup.
	for _, g := range groups {
		state.gauge.With(g.labels).Set(float64(g.count))
	}

	logger.V(1).Info("forEach gauge child evaluated", "node", node.ID, "name", gauge.Name, "groups", len(groups), "childItems", len(items))
	return nil
}

// gaugeGroup represents a group of items sharing the same label values.
type gaugeGroup struct {
	labels prometheus.Labels
	count  int
}

// groupByLabels evaluates label expressions per-item and groups items by
// unique label value combinations. The "item" variable is bound in scope
// for each element. Saves and restores any previous "item" binding to
// support gauge nodes nested inside forEach (where the parent may also
// use "item" as its iterator variable).
func groupByLabels(items []any, labelExprs map[string]string, labelNames []string, eval *evaluator) ([]gaugeGroup, error) {
	type dimension struct {
		labels prometheus.Labels
		count  int
	}
	dimensions := make(map[string]*dimension)

	// Save previous "item" binding (if any) for restore after iteration.
	prevItem, hadPrevItem := eval.scope[graph.GaugeItemVar]

	for _, item := range items {
		// Bind "item" in scope for label expression evaluation.
		eval.scope[graph.GaugeItemVar] = item

		// Evaluate each label expression.
		labelValues := make(prometheus.Labels, len(labelNames))
		for _, labelName := range labelNames {
			expr := labelExprs[labelName]
			val, err := eval.evalString(expr)
			if err != nil {
				// If field doesn't exist, use empty string (graceful degradation).
				labelValues[labelName] = ""
				continue
			}
			labelValues[labelName] = convertLabelValue(val)
		}

		// Group by serialized label values.
		key := serializeLabels(labelValues, labelNames)
		if d, exists := dimensions[key]; exists {
			d.count++
		} else {
			dimensions[key] = &dimension{labels: labelValues, count: 1}
		}
	}

	// Restore previous "item" binding or remove.
	if hadPrevItem {
		eval.scope[graph.GaugeItemVar] = prevItem
	} else {
		delete(eval.scope, graph.GaugeItemVar)
	}

	// Convert to slice.
	groups := make([]gaugeGroup, 0, len(dimensions))
	for _, d := range dimensions {
		groups = append(groups, gaugeGroup{labels: d.labels, count: d.count})
	}
	return groups, nil
}

// cleanStaleDimensions removes gauge series for label combinations that no
// longer appear in the current evaluation.
func cleanStaleDimensions(state *gaugeState, currentLabels []prometheus.Labels, labelNames []string) {
	if len(state.knownLabels) == 0 {
		return
	}
	currentSet := make(map[string]bool, len(currentLabels))
	for _, labels := range currentLabels {
		currentSet[serializeLabels(labels, labelNames)] = true
	}
	for _, labels := range state.knownLabels {
		if !currentSet[serializeLabels(labels, labelNames)] {
			state.gauge.Delete(labels)
		}
	}
}

// convertLabelValue converts a CEL evaluation result to a string suitable
// for a prometheus label value.
func convertLabelValue(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case bool:
		return fmt.Sprintf("%t", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// serializeLabels creates a consistent string key from label values for grouping.
func serializeLabels(labels prometheus.Labels, sortedNames []string) string {
	parts := make([]string, len(sortedNames))
	for i, name := range sortedNames {
		parts[i] = name + "=" + labels[name]
	}
	return strings.Join(parts, ",")
}

// sortedKeys returns the sorted keys of a map.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
