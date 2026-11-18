package engine

import (
	"fmt"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sort"
)

func (d *DB) image() snapshot.Image {
	return snapshot.Image{Format: 1, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Clone()}
}
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), d.image()); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}

// Blocker is one active reader that keeps the readable floor from advancing.
// Every session at the lowest snapshot is reported, not only the minimum:
// after the lowest reader closes, the next one becomes the blocker, so the
// operator needs the whole set to estimate how long the floor will stay pinned.
type Blocker struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
}

// VacuumRequest is one maintenance decision: how far the readable floor may
// advance and how much release work this pass may do. MaxKeys and MaxRelations
// bound physical work; zero means unlimited. DryRun asks for the plan without
// advancing the floor or releasing anything.
type VacuumRequest struct {
	Horizon      uint64 `json:"horizon"`
	MaxKeys      int    `json:"max_keys,omitempty"`
	MaxRelations int    `json:"max_relations,omitempty"`
	DryRun       bool   `json:"dry_run,omitempty"`
}

// VacuumPlan answers the maintenance question before acting: who blocks, what
// is already safe to release, what the requested pass would release, and what
// closing readers would unblock.
type VacuumPlan struct {
	Requested          uint64                `json:"requested"`
	Horizon            uint64                `json:"horizon"`
	SafeThrough        uint64                `json:"safe_through"`
	Pinned             bool                  `json:"pinned"`
	Blockers           []Blocker             `json:"blockers"`
	Active             int                   `json:"active"`
	Keys               int                   `json:"keys"`
	Versions           int                   `json:"versions"`
	Eligible           versions.Work         `json:"eligible"`
	Projected          versions.VacuumResult `json:"projected"`
	Remaining          versions.Work         `json:"remaining"`
	UnblockedByClosing versions.Work         `json:"unblocked_by_closing"`
	// Complete is true when no release work remains at the achieved horizon.
	// ReachedRequested additionally says the requested horizon was reached:
	// without it a pinned pass could look finished while an older reader still
	// holds the floor back.
	Complete         bool `json:"complete"`
	ReachedRequested bool `json:"reached_requested"`
}

// VacuumReport separates durable floor movement, physical release and
// remaining work. Planning is not release: a dry run fills Projected and
// leaves Removed at zero, while an applied pass reports what it actually
// dropped in Removed. Released carries the per-relation breakdown.
type VacuumReport struct {
	Requested       uint64 `json:"requested"`
	Horizon         uint64 `json:"horizon"`
	PreviousHorizon uint64 `json:"previous_horizon"`
	SafeThrough     uint64 `json:"safe_through"`
	Pinned          bool   `json:"pinned"`
	// Removed and Retained mirror Released for the original public shape.
	Removed         int       `json:"removed"`
	Retained        int       `json:"retained"`
	Projected       int       `json:"projected"`
	BudgetExhausted bool      `json:"budget_exhausted"`
	Blockers        []Blocker `json:"blockers"`
	Active          int       `json:"active"`
	DryRun          bool      `json:"dry_run"`
	MaxKeys         int       `json:"max_keys"`
	MaxRelations    int       `json:"max_relations"`
	// Eligible is release work already allowed by the durable floor, and
	// UnblockedByClosing is extra work that waits for active readers.
	Eligible           versions.Work         `json:"eligible"`
	Released           versions.VacuumResult `json:"released"`
	Remaining          versions.Work         `json:"remaining"`
	UnblockedByClosing versions.Work         `json:"unblocked_by_closing"`
	// Complete means no release work remains at the achieved horizon.
	// ReachedRequested distinguishes "done at a lower safe floor" from "done
	// at the requested horizon".
	Complete         bool `json:"complete"`
	ReachedRequested bool `json:"reached_requested"`
}

// lowestBlockers returns every active transaction at the oldest active
// snapshot when that snapshot is behind the head. Those are exactly the
// readers that decide how far the floor may advance.
func (d *DB) lowestBlockers() []Blocker {
	out := []Blocker{}
	if len(d.transactions) == 0 {
		return out
	}
	lowest := d.sequence
	for _, tx := range d.transactions {
		if tx.Snapshot < lowest {
			lowest = tx.Snapshot
		}
	}
	if lowest >= d.sequence {
		return out
	}
	for _, tx := range d.transactions {
		if tx.Snapshot == lowest {
			out = append(out, Blocker{ID: tx.ID, Snapshot: tx.Snapshot, ReadOnly: tx.ReadOnly, Started: tx.Started})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// blockers reports active readers that would be invalidated by the candidate
// horizon. Sorted by snapshot then id so the response is reproducible.
func (d *DB) blockers(horizon uint64) []Blocker {
	out := []Blocker{}
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			out = append(out, Blocker{ID: tx.ID, Snapshot: tx.Snapshot, ReadOnly: tx.ReadOnly, Started: tx.Started})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Snapshot == out[j].Snapshot {
			return out[i].ID < out[j].ID
		}
		return out[i].Snapshot < out[j].Snapshot
	})
	return out
}

// safeHorizon returns the smallest active snapshot, or the head when no reader
// is active. It is the highest readable floor that invalidates no live view.
func (d *DB) safeHorizon() uint64 {
	horizon := d.sequence
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	return horizon
}

// effectiveHorizon clamps a request to the safe reader floor and the durable
// floor. The result never moves backwards and never cuts off a live reader.
func (d *DB) effectiveHorizon(requested uint64) uint64 {
	horizon := requested
	if safe := d.safeHorizon(); safe < horizon {
		horizon = safe
	}
	if horizon < d.horizon {
		horizon = d.horizon
	}
	return horizon
}

func (d *DB) checkVacuum(request VacuumRequest) error {
	if request.MaxKeys < 0 || request.MaxRelations < 0 {
		return fault("invalid", "vacuum budgets cannot be negative")
	}
	if request.Horizon > d.sequence {
		return fault("invalid", "vacuum horizon %d is ahead of sequence %d", request.Horizon, d.sequence)
	}
	if request.Horizon < d.horizon {
		return fault("invalid", "vacuum horizon %d is below durable horizon %d; horizons cannot move backwards", request.Horizon, d.horizon)
	}
	return nil
}

func (d *DB) vacuumOptions(request VacuumRequest, horizon uint64) versions.VacuumOptions {
	return versions.VacuumOptions{Horizon: horizon, MaxKeys: request.MaxKeys, MaxRelations: request.MaxRelations}
}

// unblockedWork is the extra work that becomes eligible if every active reader
// closes and the floor can reach the requested sequence.
func (d *DB) unblockedWork(horizon, requested uint64) versions.Work {
	if requested <= horizon {
		return versions.Work{}
	}
	return d.store.AdditionalWork(horizon, requested)
}

// PlanVacuum answers a maintenance request without changing the durable floor
// or releasing versions. Its Projected result carries intent only.
func (d *DB) PlanVacuum(request VacuumRequest) (VacuumPlan, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return VacuumPlan{}, err
	}
	if err := d.checkVacuum(request); err != nil {
		return VacuumPlan{}, err
	}
	horizon := d.effectiveHorizon(request.Horizon)
	keys, versionCount, eligible, _ := d.store.Facts(horizon)
	projected := d.store.Project(d.vacuumOptions(request, horizon))
	plan := VacuumPlan{
		Requested:          request.Horizon,
		Horizon:            horizon,
		SafeThrough:        d.safeHorizon(),
		Pinned:             horizon < request.Horizon,
		Blockers:           d.blockers(request.Horizon),
		Active:             len(d.transactions),
		Keys:               keys,
		Versions:           versionCount,
		Eligible:           eligible,
		Projected:          projected,
		Remaining:          versions.Work{Keys: projected.Pending.Keys, Versions: projected.Pending.Versions},
		UnblockedByClosing: d.unblockedWork(horizon, request.Horizon),
		Complete:           projected.Pending.Versions == 0,
		ReachedRequested:   horizon == request.Horizon,
	}
	return plan, nil
}

// Vacuum advances the durable readable floor and releases versions under an
// explicit work budget. A pass is one journaled record, so a restart either has
// the whole pass or none of it: the floor cannot move backwards and a partially
// released chain cannot be observed.
func (d *DB) Vacuum(request VacuumRequest) (VacuumReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return VacuumReport{}, err
	}
	if err := d.checkVacuum(request); err != nil {
		return VacuumReport{}, err
	}
	horizon := d.effectiveHorizon(request.Horizon)
	options := d.vacuumOptions(request, horizon)
	report := VacuumReport{
		Requested:          request.Horizon,
		Horizon:            horizon,
		PreviousHorizon:    d.horizon,
		SafeThrough:        d.safeHorizon(),
		Pinned:             horizon < request.Horizon,
		Blockers:           d.blockers(request.Horizon),
		Active:             len(d.transactions),
		DryRun:             request.DryRun,
		MaxKeys:            request.MaxKeys,
		MaxRelations:       request.MaxRelations,
		UnblockedByClosing: d.unblockedWork(horizon, request.Horizon),
		ReachedRequested:   horizon == request.Horizon,
	}
	if request.DryRun {
		// A dry run reports intent and must never claim a release. Retained is
		// the unchanged physical version count; Projected is what a real pass
		// would free.
		projected := d.store.Project(options)
		report.Released = projected
		report.Projected = projected.Projected
		report.Retained = projected.Retained
		report.Eligible = versions.Work{Keys: projected.Eligible.Keys, Versions: projected.Eligible.Versions}
		report.Remaining = versions.Work{Keys: projected.Pending.Keys, Versions: projected.Pending.Versions}
		report.Complete = projected.Pending.Versions == 0
		return report, nil
	}

	// When neither the floor nor any version changes, journaling another
	// record would only grow the log. Report the same facts a durable record
	// would have produced and leave the sequence untouched.
	if horizon == d.horizon {
		planned := d.store.Plan(options)
		if planned.Projected == 0 {
			report.Released = planned
			report.Removed = 0
			report.Retained = planned.Retained
			report.BudgetExhausted = false
			report.Eligible = versions.Work{Keys: planned.Eligible.Keys, Versions: planned.Eligible.Versions}
			report.Remaining = versions.Work{Keys: planned.Pending.Keys, Versions: planned.Pending.Versions}
			report.Complete = planned.Pending.Versions == 0
			return report, nil
		}
	}

	// Journal the floor movement and its work bounds. publish applies the
	// record through the same deterministic function replay uses, and
	// lastVacuum is the physical result of that application. Reporting it
	// directly avoids presenting a projection as an accomplished release.
	record := journal.Record{Kind: "vacuum", Horizon: horizon, MaxKeys: request.MaxKeys, MaxRelations: request.MaxRelations}
	if err := d.publish(record); err != nil {
		return VacuumReport{}, err
	}
	released := d.lastVacuum
	d.indexes.RebuildRelations(d.catalog, d.store, released.TouchedRelations)
	report.Released = released
	report.Removed = released.Removed
	report.Retained = released.Retained
	report.BudgetExhausted = released.BudgetExhausted
	report.Eligible = versions.Work{Keys: released.Eligible.Keys, Versions: released.Eligible.Versions}
	report.Remaining = versions.Work{Keys: released.Pending.Keys, Versions: released.Pending.Versions}
	report.Complete = released.Pending.Versions == 0
	return report, nil
}
