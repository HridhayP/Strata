// Package metrics exports Raft, KV and storage metrics to Prometheus.
package metrics

import (
	"strconv"
	"time"

	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/storage/lsm"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	commitLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "strata_raft_commit_latency_seconds",
		Help:    "Time from a leader accepting a proposal to the entry committing.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 100us .. ~3.3s
	}, []string{"group"})
	leaderChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "strata_raft_leader_changes_total",
		Help: "Number of times this replica observed a new leader.",
	}, []string{"group"})
	replicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "strata_raft_replication_lag_entries",
		Help: "Leader's view of how many log entries each follower is behind.",
	}, []string{"group", "peer"})
)

func init() {
	prometheus.MustRegister(commitLatency, leaderChanges, replicationLag)
}

// Observer implements raft.Observer for one group.
type Observer struct {
	group   string
	latency prometheus.Observer
	leaders prometheus.Counter
}

// NewObserver returns an observer labelled with group.
func NewObserver(group string) *Observer {
	return &Observer{
		group:   group,
		latency: commitLatency.WithLabelValues(group),
		leaders: leaderChanges.WithLabelValues(group),
	}
}

func (o *Observer) CommitLatency(d time.Duration) { o.latency.Observe(d.Seconds()) }

func (o *Observer) LeaderChange(uint64, int) { o.leaders.Inc() }

func (o *Observer) ReplicationLag(peer int, entries uint64) {
	replicationLag.WithLabelValues(o.group, strconv.Itoa(peer)).Set(float64(entries))
}

// Collector samples Raft and LSM state at scrape time.
type Collector struct {
	group string
	rf    *raft.Raft
	db    *lsm.DB // may be nil (controller)
}

var (
	termDesc     = prometheus.NewDesc("strata_raft_term", "Current Raft term.", []string{"group"}, nil)
	leaderDesc   = prometheus.NewDesc("strata_raft_is_leader", "1 if this replica is leader.", []string{"group"}, nil)
	commitDesc   = prometheus.NewDesc("strata_raft_commit_index", "Commit index.", []string{"group"}, nil)
	appliedDesc  = prometheus.NewDesc("strata_raft_applied_index", "Last applied index.", []string{"group"}, nil)
	logDesc      = prometheus.NewDesc("strata_raft_log_entries", "Entries in the uncompacted log.", []string{"group"}, nil)
	tablesDesc   = prometheus.NewDesc("strata_lsm_tables", "Live SSTables.", []string{"group"}, nil)
	tableBytes   = prometheus.NewDesc("strata_lsm_table_bytes", "Bytes in live SSTables.", []string{"group"}, nil)
	flushesDesc  = prometheus.NewDesc("strata_lsm_flushes_total", "Memtable flushes.", []string{"group"}, nil)
	compactsDesc = prometheus.NewDesc("strata_lsm_compactions_total", "Compactions.", []string{"group"}, nil)
)

// RegisterCollector exports state gauges for a replica.
func RegisterCollector(group string, rf *raft.Raft, db *lsm.DB) {
	prometheus.MustRegister(&Collector{group: group, rf: rf, db: db})
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{termDesc, leaderDesc, commitDesc, appliedDesc, logDesc, tablesDesc, tableBytes, flushesDesc, compactsDesc} {
		ch <- d
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.rf.Status()
	leader := 0.0
	if st.Role == "leader" {
		leader = 1
	}
	g := c.group
	ch <- prometheus.MustNewConstMetric(termDesc, prometheus.GaugeValue, float64(st.Term), g)
	ch <- prometheus.MustNewConstMetric(leaderDesc, prometheus.GaugeValue, leader, g)
	ch <- prometheus.MustNewConstMetric(commitDesc, prometheus.GaugeValue, float64(st.CommitIndex), g)
	ch <- prometheus.MustNewConstMetric(appliedDesc, prometheus.GaugeValue, float64(st.LastApplied), g)
	ch <- prometheus.MustNewConstMetric(logDesc, prometheus.GaugeValue, float64(st.LogEntries), g)
	if c.db != nil {
		s := c.db.Stats()
		ch <- prometheus.MustNewConstMetric(tablesDesc, prometheus.GaugeValue, float64(s.Tables), g)
		ch <- prometheus.MustNewConstMetric(tableBytes, prometheus.GaugeValue, float64(s.TableBytes), g)
		ch <- prometheus.MustNewConstMetric(flushesDesc, prometheus.CounterValue, float64(s.Flushes), g)
		ch <- prometheus.MustNewConstMetric(compactsDesc, prometheus.CounterValue, float64(s.Compactions), g)
	}
}
