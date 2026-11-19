package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/api"
)

// FailUnplaceable fails every prepare task that has been pending for limit
// while its original is larger than any worker able to take it could hold:
// each online active worker that may prepare (kind and sandbox level, as
// FleetCapacity) and holds process on the job's queue reports in its gauges
// the largest input its disk could take when idle (PrepareCapacityBytes),
// and the original exceeds the largest of them. Claims already keep such a
// task from workers without room (Eligibility.PrepareMaxInput); this ends
// the wait instead of leaving it queued for ever (DESIGN.md §7.2).
//
// A task is never failed while no such worker is online (approving or
// starting one may fix that) or while one of them did not report its
// capacity (an older worker, or no free-space reading): it might fit there.
// Each failed task makes its job failing, as FailStranded does.
func (s *Store) FailUnplaceable(ctx context.Context, limit, seenWithin time.Duration) ([]StrandedTask, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: retry wait limit must be positive", ErrInvalid)
	}
	now := s.now()
	caps, err := s.prepareCapacities(ctx, now, seenWithin)
	if err != nil || len(caps) == 0 {
		return nil, err
	}
	var out []StrandedTask
	err = s.write(ctx, func(tx *sql.Tx) error {
		out = nil
		cands, err := waitingPrepares(ctx, tx, now.Add(-limit))
		if err != nil {
			return err
		}
		for _, c := range cands {
			best, ok := caps.forQueue(c.queue)
			if !ok || best >= c.bytes {
				continue
			}
			c.st.Reason = fmt.Sprintf("the original (%s) is larger than any worker can take: the most free disk "+
				"for preparing is %s (DESIGN.md §7.2); waited %s", byteSize(c.bytes), byteSize(best), shortDuration(limit))
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET state = 'failed', error = ? WHERE job_id = ? AND seq = ?
				AND state = 'pending'`, c.st.Reason, c.st.JobID, c.st.Seq); err != nil {
				return fmt.Errorf("fail unplaceable task: %w", err)
			}
			if err := doomJob(ctx, tx, c.st.JobID, c.st.Reason, now); err != nil {
				return err
			}
			if c.st.JobFailed, err = settleJob(ctx, tx, c.st.JobID, now); err != nil {
				return err
			}
			out = append(out, c.st)
		}
		return nil
	})
	return out, err
}

// waitingPrepare is a prepare task pending since before a cutoff.
type waitingPrepare struct {
	st    StrandedTask
	queue int64
	bytes int64
}

// waitingPrepares lists the prepare tasks pending since before cutoff, with
// their queues and original sizes.
func waitingPrepares(ctx context.Context, tx *sql.Tx, cutoff time.Time) ([]waitingPrepare, error) {
	rows, err := tx.QueryContext(ctx, `SELECT t.job_id, t.seq, j.queue_id, j.original_bytes FROM tasks t
		JOIN jobs j ON j.id = t.job_id
		WHERE t.kind = 'prepare' AND t.state = 'pending' AND `+pendingSinceSQL+` <= ?
		ORDER BY t.job_id`, toMS(cutoff))
	if err != nil {
		return nil, fmt.Errorf("unplaceable tasks: %w", err)
	}
	defer rows.Close()
	var out []waitingPrepare
	for rows.Next() {
		c := waitingPrepare{st: StrandedTask{Kind: api.KindPrepare}}
		if err := rows.Scan(&c.st.JobID, &c.st.Seq, &c.queue, &c.bytes); err != nil {
			return nil, fmt.Errorf("unplaceable tasks: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unplaceable tasks: %w", err)
	}
	return out, nil
}

// prepareCaps are the prepare capacities of the online workers that may
// prepare, by the queues they may process ("*": all).
type prepareCaps []prepareCap

type prepareCap struct {
	all    bool
	queues map[int64]bool
	bytes  int64 // math.MaxInt64 if not reported
}

// forQueue returns the largest capacity among the workers that may process
// queue, and whether there is any.
func (cs prepareCaps) forQueue(queue int64) (best int64, ok bool) {
	for _, c := range cs {
		if c.all || c.queues[queue] {
			best, ok = max(best, c.bytes), true
		}
	}
	return best, ok
}

func (s *Store) prepareCapacities(ctx context.Context, now time.Time, seenWithin time.Duration) (prepareCaps, error) {
	ws, err := s.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	var out prepareCaps
	for i := range ws {
		w := &ws[i]
		if w.State != api.WorkerActive || w.LastSeenAt == nil || now.Sub(*w.LastSeenAt) > seenWithin ||
			!CanPrepare(w.Facts, s.cfg.PrepareMinSandbox) {
			continue
		}
		c := prepareCap{queues: map[int64]bool{}, bytes: math.MaxInt64}
		var g api.Gauges
		if len(w.Gauges) > 0 && json.Unmarshal(w.Gauges, &g) == nil && g.PrepareCapacityBytes != nil {
			c.bytes = *g.PrepareCapacityBytes
		}
		gs, err := s.GrantsFor(ctx, w.Principal())
		if err != nil {
			return nil, err
		}
		for _, gr := range gs {
			if gr.Role != api.RoleProcess {
				continue
			}
			if gr.Scope == "*" {
				c.all = true
			} else if id, ok := queueScopeID(gr.Scope); ok {
				c.queues[id] = true
			}
		}
		out = append(out, c)
	}
	return out, nil
}

// byteSize formats n in binary units, one decimal: 12.0 GiB.
func byteSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	v, i := float64(n), -1
	for v >= unit && i < 3 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %ciB", v, "KMGT"[i])
}

// queueScopeID parses a stored queue scope (QueueScope).
func queueScopeID(scope string) (int64, bool) {
	s, ok := strings.CutPrefix(scope, "queue:")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil
}
