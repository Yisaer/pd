package copysets

import "github.com/prometheus/client_golang/prometheus"

var (
	csGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "copyset_manager",
			Name:      "count",
			Help:      "node count",
		}, []string{"type"})

	copySetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "copyset_manager",
			Name:      "sign",
			Help:      "sign of the scheduler.",
		}, []string{"sign"})
)

func init() {
	prometheus.MustRegister(csGauge)
	prometheus.MustRegister(copySetGauge)
}
