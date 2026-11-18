package versions

import "sort"

// Work counts version chains that still hold reclaimable versions, together
// with the number of versions those chains can release.
type Work struct {
	Keys     int `json:"keys"`
	Versions int `json:"versions"`
}

// VacuumOptions describes one deterministic maintenance pass. Horizon is the
// oldest snapshot that stays readable after the pass. MaxKeys and MaxRelations
// bound release work only: zero means unlimited.
//
// Only keys that actually hold removable versions consume key budget, and only
// relations that hold eligible keys consume relation budget. A key is released
// as a whole, so a budget stops between version chains rather than inside one.
// Processed work drops out of eligibility, so the next bounded pass continues
// with the remaining work. No cursor is stored, which makes every pass
// restart-safe and keeps the readable horizon from moving backwards after a
// restart.
//
// Budgets are counted in version chains and relations rather than wall-clock
// time because the pass is journaled and replayed. A time-based cutoff would
// reclaim different versions on replay and diverge from the durable record.
type VacuumOptions struct {
	Horizon      uint64
	MaxKeys      int
	MaxRelations int
}

// RelationVacuum is the per-relation accounting of one pass. Eligible is what
// the horizon makes reclaimable before the pass; Removed is what this pass
// actually released; Projected is what it intended to release (equal to
// Removed for an applied pass, an estimate for a dry run); Pending is what a
// later pass can still release at the same horizon. Planning is not
// reclamation, so a projection never fills Removed.
type RelationVacuum struct {
	Relation      string `json:"relation"`
	Eligible      Work   `json:"eligible"`
	Removed       int    `json:"removed"`
	ReclaimedKeys int    `json:"reclaimed_keys"`
	Projected     int    `json:"projected"`
	Pending       Work   `json:"pending"`
	Complete      bool   `json:"complete"`
}

// VacuumResult is a physical result. The caller owns the logical horizon and
// its durability; this type only describes what the store released or would
// release, and what remains.
type VacuumResult struct {
	Horizon            uint64           `json:"horizon"`
	Removed            int              `json:"removed"`
	Retained           int              `json:"retained"`
	ReclaimedKeys      int              `json:"reclaimed_keys"`
	Projected          int              `json:"projected"`
	ProcessedKeys      int              `json:"processed_keys"`
	ProcessedRelations int              `json:"processed_relations"`
	Eligible           Work             `json:"eligible"`
	Pending            Work             `json:"pending"`
	BudgetExhausted    bool             `json:"budget_exhausted"`
	Complete           bool             `json:"complete"`
	Relations          []RelationVacuum `json:"relations"`
	// TouchedRelations lists relations whose chains this pass changed. Index
	// maintenance can rebuild only these instead of the whole catalog.
	TouchedRelations []string `json:"touched_relations,omitempty"`
}

// removable reports how many leading versions are unreachable at horizon and
// whether the chain is a final tombstone that can be dropped entirely. The
// last version at or before the horizon is always kept so a snapshot exactly
// at the horizon can still reconstruct its view.
func removable(chain []Version, horizon uint64) (int, bool) {
	if len(chain) == 0 {
		return 0, false
	}
	last := chain[len(chain)-1]
	if last.Sequence <= horizon && last.Deleted {
		return len(chain), true
	}
	keep := 0
	for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
		keep++
	}
	return keep, false
}

type keyPlan struct {
	key       string
	removable int
	reclaim   bool
}

type relationPlan struct {
	name     string
	eligible Work
	keys     []keyPlan
}

// plan collects reclaimable work in a deterministic order. Relations and keys
// are sorted so bounded passes are reproducible on replay and successive
// passes walk the whole store without a cursor.
func (s *Store) plan(horizon uint64) ([]relationPlan, int) {
	names := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	plans := make([]relationPlan, 0, len(names))
	total := 0
	for _, name := range names {
		plan := relationPlan{name: name}
		for key, chain := range s.Tables[name] {
			total += len(chain)
			n, reclaim := removable(chain, horizon)
			if n == 0 {
				continue
			}
			plan.eligible.Keys++
			plan.eligible.Versions += n
			plan.keys = append(plan.keys, keyPlan{key: key, removable: n, reclaim: reclaim})
		}
		if plan.eligible.Versions == 0 {
			continue
		}
		sort.Slice(plan.keys, func(i, j int) bool { return plan.keys[i].key < plan.keys[j].key })
		plans = append(plans, plan)
	}
	return plans, total
}

// applyPlans walks the deterministic plan under the budget. When mutate is
// true the selected chains are changed and Removed counts the versions this
// call released. When mutate is false the same deterministic selection is
// reported in Projected while Removed stays zero, so a dry run can never be
// mistaken for a release. Retained is the version count left after the
// selection is applied.
func (s *Store) applyPlans(opts VacuumOptions, plans []relationPlan, total int, mutate bool) VacuumResult {
	result := VacuumResult{Horizon: opts.Horizon, Relations: []RelationVacuum{}}
	for _, plan := range plans {
		result.Eligible.Keys += plan.eligible.Keys
		result.Eligible.Versions += plan.eligible.Versions
	}

	maxRelations := opts.MaxRelations
	if maxRelations <= 0 {
		maxRelations = len(plans)
	}
	keysUsed := 0
	relationsUsed := 0
	relationsTouched := 0
	keysLimited := false
	relationsLimited := false
	projected := 0
	reclaimed := 0
	pendingOnly := func(plan relationPlan) {
		// A relation the budget cannot admit remains fully pending; it must
		// contribute to the remaining cost, otherwise the pass would claim
		// completeness while work is still eligible.
		result.Relations = append(result.Relations, RelationVacuum{
			Relation: plan.name, Eligible: plan.eligible, Pending: plan.eligible,
		})
		result.Pending.Keys += plan.eligible.Keys
		result.Pending.Versions += plan.eligible.Versions
	}
	for _, plan := range plans {
		if opts.MaxKeys > 0 && keysUsed >= opts.MaxKeys {
			keysLimited = true
			pendingOnly(plan)
			continue
		}
		if relationsUsed >= maxRelations {
			relationsLimited = true
			pendingOnly(plan)
			continue
		}
		relationsUsed++
		row := RelationVacuum{Relation: plan.name, Eligible: plan.eligible}
		processed := 0
		planned := 0
		reclaim := 0
		for _, key := range plan.keys {
			if opts.MaxKeys > 0 && keysUsed >= opts.MaxKeys {
				keysLimited = true
				break
			}
			if mutate {
				chain := s.Tables[plan.name][key.key]
				if key.reclaim {
					delete(s.Tables[plan.name], key.key)
				} else {
					// Copy so the reclaimed prefix is not kept alive by a
					// slice that still references the old backing array.
					s.Tables[plan.name][key.key] = append([]Version(nil), chain[key.removable:]...)
				}
			}
			if key.reclaim {
				reclaim++
			}
			planned += key.removable
			keysUsed++
			processed++
		}
		row.Projected += planned
		projected += planned
		if planned > 0 {
			relationsTouched++
		}
		row.Pending.Keys = plan.eligible.Keys - processed
		row.Pending.Versions = plan.eligible.Versions - planned
		row.Complete = row.Pending.Versions == 0
		if mutate {
			row.Removed = planned
			row.ReclaimedKeys = reclaim
			reclaimed += reclaim
			if planned > 0 {
				result.TouchedRelations = append(result.TouchedRelations, plan.name)
			}
		}
		result.Removed += row.Removed
		result.ReclaimedKeys += row.ReclaimedKeys
		result.Projected += row.Projected
		result.Pending.Keys += row.Pending.Keys
		result.Pending.Versions += row.Pending.Versions
		result.Relations = append(result.Relations, row)
	}
	result.ProcessedKeys = keysUsed
	// ProcessedRelations counts relations that actually selected release work,
	// not relations merely admitted by the relation budget.
	result.ProcessedRelations = relationsTouched
	result.Retained = total - projected
	if mutate {
		result.ReclaimedKeys = reclaimed
	}
	result.BudgetExhausted = (keysLimited || relationsLimited) && result.Pending.Versions > 0
	result.Complete = result.Pending.Versions == 0
	return result
}

// Vacuum releases eligible versions within the budget and mutates the store.
func (s *Store) Vacuum(opts VacuumOptions) VacuumResult {
	plans, total := s.plan(opts.Horizon)
	return s.applyPlans(opts, plans, total, true)
}

// Plan reports exactly what Vacuum would release with the same options without
// changing the store. Because the selection is deterministic, the plan is the
// release set an applied pass will produce.
func (s *Store) Plan(opts VacuumOptions) VacuumResult {
	plans, total := s.plan(opts.Horizon)
	return s.applyPlans(opts, plans, total, false)
}

// Project is Plan with every release counter zeroed. It describes cost and
// pending work for a dry run and never claims that anything was processed or
// released. Retained therefore stays at the current physical version count:
// nothing has been freed yet, and Pending says how much a real pass could free.
func (s *Store) Project(opts VacuumOptions) VacuumResult {
	result := s.Plan(opts)
	result.Removed = 0
	result.ReclaimedKeys = 0
	result.ProcessedKeys = 0
	result.ProcessedRelations = 0
	// Plan reported Retained as "what would be left after applying"; add the
	// projected release back so Retained describes the untouched store.
	result.Retained += result.Projected
	for i := range result.Relations {
		result.Relations[i].Removed = 0
		result.Relations[i].ReclaimedKeys = 0
	}
	return result
}

// AdditionalWork reports work that only becomes reclaimable when the readable
// horizon moves from the first sequence to the second. It describes what
// closing active readers would unblock.
func (s *Store) AdditionalWork(from, to uint64) Work {
	var work Work
	if to <= from {
		return work
	}
	for _, rows := range s.Tables {
		for _, chain := range rows {
			before, _ := removable(chain, from)
			after, _ := removable(chain, to)
			if after > before {
				work.Keys++
				work.Versions += after - before
			}
		}
	}
	return work
}

// Facts returns the physical size, reclaimable work and reclaimable relation
// names in one traversal so observability endpoints do not walk every chain
// more than once.
func (s *Store) Facts(horizon uint64) (keys, versions int, work Work, relations []string) {
	for name, rows := range s.Tables {
		keys += len(rows)
		relationWork := 0
		for _, chain := range rows {
			versions += len(chain)
			n, _ := removable(chain, horizon)
			if n > 0 {
				work.Keys++
				work.Versions += n
				relationWork += n
			}
		}
		if relationWork > 0 {
			relations = append(relations, name)
		}
	}
	sort.Strings(relations)
	return
}
