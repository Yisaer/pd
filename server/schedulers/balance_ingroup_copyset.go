package schedulers

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"sort"
)

func init() {
	schedule.RegisterScheduler("balance-ingroup-copyset-scheduler", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return NewBalanceInGroupCopySetScheduler(opController), nil
	})
}

type balanceInGroupCopySetScheduler struct {
	*BaseScheduler
	opController *schedule.OperatorController
	filters      []filter.Filter
}

func NewBalanceInGroupCopySetScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceInGroupCopySetScheduler{
		BaseScheduler: base,
		opController:  opController,
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
	}
	return scheduler
}

func (s *balanceInGroupCopySetScheduler) GetName() string {
	return "balance-ingroup-copyset-scheduler"
}

func (s *balanceInGroupCopySetScheduler) GetType() string {
	return BalanceRegionType
}

func (s *balanceInGroupCopySetScheduler) EncodeConfig() ([]byte, error) {
	conf := &balanceCopySetSchedulerConfig{Name: "balance-ingroup-copyset-scheduler"}
	return schedule.EncodeConfig(conf)
}

func (s *balanceInGroupCopySetScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetRegionScheduleLimit()
}

func (s *balanceInGroupCopySetScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	if len(cluster.GetCopySets(toid(cluster.GetStores()))) < 1 {
		return nil
	}
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, s.filters, opts)
	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	storesScore := make(map[uint64]float64, 0)
	for _, store := range stores {
		op := opInfluence.GetStoreInfluence(store.GetID()).ResourceProperty(kind)
		score := store.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), op, -1)
		storesScore[store.GetID()] = score
	}
	//groupCSScore := make(map[string][]copysetScore, 0)
	ops := make([]*operator.Operator, 0)
	m := cluster.GetCopySetsByGroups(toid(cluster.GetStores()))
	for sign, css := range m {
		delta := float64(0)
		var deltaCS copysets.CopySet
		var selectRegion *core.RegionInfo
		detalList := make([]cssDetltScore, 0)
		for _, cs := range css {
			n1, n2, n3 := cs.GetNodesID()
			min, max := minAndMax(storesScore[n1], storesScore[n2], storesScore[n3])
			delta = max - min
			detalList = append(detalList, cssDetltScore{
				cs:    cs,
				sign:  cs.Sign(),
				delta: delta,
			})
		}
		sort.Slice(detalList, func(i, j int) bool {
			return detalList[i].delta > detalList[j].delta
		})
		for _, ds := range detalList {
			selectRegion = selectRandRegionInCopySet(cluster, ds.cs)
			if selectRegion == nil {
				log.Info("balanceInGroupCopySetScheduler no select region", zap.String("sign", deltaCS.Sign()))
				continue
			}
			break
		}
		if selectRegion == nil {
			log.Info("balanceInGroupCopySetScheduler no select region", zap.String("sign", sign))

		}
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		tolerantResource := getTolerantResource(cluster, selectRegion, kind)
		if delta <= float64(tolerantResource) {
			n1, n2, n3 := deltaCS.GetNodesID()
			s1 := storesScore[n1]
			s2 := storesScore[n2]
			s3 := storesScore[n3]
			log.Info("balanceInGroupCopySetScheduler",
				zap.Int64("tolerantResource", tolerantResource),
				zap.String("deltaCS", deltaCS.Sign()),
				zap.Float64("s1", s1),
				zap.Float64("s2", s2),
				zap.Float64("s3", s3),
				zap.Float64("delta", delta))
			continue
		}

		n1, n2, n3 := deltaCS.GetNodesID()
		minStoreID := n1
		if storesScore[n2] < storesScore[minStoreID] {
			minStoreID = n2
		}
		if storesScore[n3] < storesScore[minStoreID] {
			minStoreID = n3
		}
		find := false
		var tarCs copysets.CopySet
		for _, cs := range css {
			if cs.Sign() == cssSign {
				continue
			}
			minD := math.MaxFloat64
			if cs.IsStoreInCopySet(minStoreID) {
				n1, n2, n3 := cs.GetNodesID()
				d := caldelta(storesScore, n1, n2, n3)
				if d < minD {
					minD = d
					find = true
					tarCs = cs
				}
			}
		}
		if find {
			op, err := operator.CreateMoveCopySetOperator(s.GetName(), cluster, selectRegion, operator.OpRegion, tarCs)
			if err != nil {
				log.Error("balanceInGroupCopySetScheduler schedule failed",
					zap.Error(err),
					zap.String("deltaCS", deltaCS.Sign()))
				continue
			}
			if op != nil {
				ops = append(ops, op)
			} else {
				selectRegion.GetPeers()
				log.Info("balanceInGroupCopySetScheduler no operator",
					zap.String("tarCs", tarCs.Sign()),
					zap.Uint64("regionID", selectRegion.GetID()))
			}
		} else {
			log.Info("balanceInGroupCopySetScheduler no delta target found", zap.String("sign", sign))
			continue
		}
	}
	if len(ops) > 0 {
		log.Info("balanceInGroupCopySetScheduler", zap.Int("operatorCount", len(ops)))
		return ops
	}
	log.Info("balanceInGroupCopySetScheduler no schedule")
	return nil
}

func selectRandRegionInCopySet(cluster opt.Cluster, cs copysets.CopySet) *core.RegionInfo {
	n1, n2, n3 := cs.GetNodesID()
	cnt := make(map[uint64]int, 0)
	r1 := cluster.GetStoreRegions(n1)
	r2 := cluster.GetStoreRegions(n2)
	r3 := cluster.GetStoreRegions(n3)
	for _, region := range r1 {
		if _, ok := cnt[region.GetID()]; !ok {
			cnt[region.GetID()] = 0
		}
		cnt[region.GetID()] = cnt[region.GetID()] + 1
	}
	for _, region := range r2 {
		if _, ok := cnt[region.GetID()]; !ok {
			cnt[region.GetID()] = 0
		}
		cnt[region.GetID()] = cnt[region.GetID()] + 1
	}

	for _, region := range r3 {
		if _, ok := cnt[region.GetID()]; !ok {
			cnt[region.GetID()] = 0
		}
		cnt[region.GetID()] = cnt[region.GetID()] + 1
	}
	rids := make([]uint64, 0)
	for id, v := range cnt {
		if v >= 3 {
			rids = append(rids, id)
		}
	}

	if len(rids) < 1 {
		return nil
	}
	regionID := rids[rand.Intn(len(rids))]
	return cluster.GetRegion(regionID)
}

func minAndMax(n ...float64) (float64, float64) {
	min := math.MaxFloat64
	max := float64(0)
	for _, x := range n {
		if x > max {
			max = x
		} else if x < min {
			min = x
		}
	}
	return min, max
}

func caldelta(score map[uint64]float64, s1, s2, s3 uint64) float64 {
	min, max := minAndMax(score[s1], score[s2], score[s3])
	return max - min
}

type cssDetltScore struct {
	cs    copysets.CopySet
	sign  string
	delta float64
}
