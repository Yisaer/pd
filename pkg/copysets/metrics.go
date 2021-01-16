package copysets

import "github.com/prometheus/client_golang/prometheus"

var (
	csGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "copyset_manager",
			Name:      "node_count",
			Help:      "node count",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(csGauge)
}
