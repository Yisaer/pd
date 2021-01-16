package schedulers

import (
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

func init() {
	//schedule.RegisterScheduler("balance-ingroup-copyset-scheduler", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
	//	return NewBalanceInGroupCopySetScheduler(opController), nil
	//})
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
	return nil
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
	groupCSScore := make(map[string][]copysetScore, 0)
	for group, css := range cluster.GetCopySetsByGroups() {
		cssScore := make([]copysetScore, 0)
		for _, cs := range css {
			n1, n2, n3 := cs.GetNodesID()
			csScore := buildCopySetScore(storesScore[n1], storesScore[n2], storesScore[n3], cs)
			cssScore = append(cssScore, csScore)
		}
		groupCSScore[group] = cssScore
	}
	//ops := make([]*operator.Operator, 0)
	//for group, csScores := range groupCSScore {
	//	for _, csScore := range csScores {
	//
	//	}
	//}

	return nil
}
