package schedulers

import (
	log "github.com/sirupsen/logrus"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
	"math"
	"math/rand"
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
	return nil, nil
}

func (s *balanceInGroupCopySetScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetRegionScheduleLimit()
}

func (s *balanceInGroupCopySetScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
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
	for _, css := range cluster.GetCopySetsByGroups() {
		cssID := -1
		delta := float64(0)
		var deltaCS copysets.CopySet
		for id, cs := range css {
			n1, n2, n3 := cs.GetNodesID()
			min, max := minAndMax(storesScore[n1], storesScore[n2], storesScore[n3])
			if delta > max-min {
				cssID = id
				delta = max - min
				deltaCS = cs
			}
		}
		if cssID == -1 {
			continue
		}
		selectRegion := selectRandRegionInCopySet(cluster, deltaCS)
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		tolerantResource := getTolerantResource(cluster, selectRegion, kind)
		log.Info("balanceInGroupCopySetScheduler", zap.Int64("tolerantResource", tolerantResource))
		if delta <= float64(tolerantResource) {
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
		for id, cs := range css {
			if id == cssID {
				continue
			}
			if cs.IsStoreInCopySet(minStoreID) {
				op, err := operator.CreateMoveCopySetOperator(s.GetType(), cluster, selectRegion, operator.OpRegion, cs)
				if err != nil {
					log.Error("balanceInGroupCopySetScheduler schedule failed")
					continue
				}
				if op != nil {
					ops = append(ops, op)
				}
			}
		}
	}
	if len(ops) > 0 {
		return ops
	}
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
