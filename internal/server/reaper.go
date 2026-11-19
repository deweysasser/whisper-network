package server

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"

	"github.com/deweysasser/whisper-network/internal/blobs"
	"github.com/deweysasser/whisper-network/internal/obs"
	"github.com/deweysasser/whisper-network/internal/store"
)

// ThroughputRetention is how long the reaper keeps fleet throughput buckets
// (DESIGN.md §8.5). The dashboard reads the last hour; the rest is headroom.
const ThroughputRetention = 24 * time.Hour

// reapOnce runs one reaper pass (DESIGN.md §8.3, §8.5): expire leases,
// fail tasks no other eligible worker took within the retry wait limit,
// finish jobs whose assembly was interrupted, purge media and jobs past
// retention, and delete stale leases, throughput buckets, pending workers and sessions. Each
// step logs its own failure and the pass continues; the next tick retries.
func (s *Server) reapOnce(ctx context.Context) {
	now := s.now()
	counts := map[string]int{}
	fail := func(step string, err error) {
		if err != nil && ctx.Err() == nil {
			s.log.Error().Err(err).Str("step", step).Msg("reaper step failed")
		}
	}

	ends, err := s.st.ExpireLeases(ctx)
	fail("expire leases", err)
	for i := range ends {
		e := &ends[i]
		obs.Warn(&s.log, obs.EventLeaseExpired).Str(obs.FieldLeaseID, e.LeaseID).Str(obs.FieldJobID, e.JobID).
			Int(obs.FieldTaskSeq, e.Seq).Str(obs.FieldTaskKind, string(e.Kind)).Str(obs.FieldWorkerID, e.WorkerID).
			Int(obs.FieldAttempt, e.Attempt).Bool("requeued", e.Requeued).Msg("lease expired")
		s.leaseEnded(ctx, e)
	}
	counts["leases_expired"] = len(ends)

	stranded, err := s.st.FailStranded(ctx, s.cfg.RetryWaitLimit)
	fail("fail stranded tasks", err)
	for i := range stranded {
		st := &stranded[i]
		obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldJobID, st.JobID).Int(obs.FieldTaskSeq, st.Seq).
			Str(obs.FieldTaskKind, string(st.Kind)).Strs("failed_on", st.Workers).Bool("retryable", false).
			Str(obs.FieldError, st.Reason).Msg("task failed: no other eligible worker took it")
		if st.JobFailed {
			s.jobFinished(ctx, st.JobID)
		}
	}
	counts["tasks_stranded"] = len(stranded)

	unplaceable, err := s.st.FailUnplaceable(ctx, s.cfg.RetryWaitLimit, eligibleWindow)
	fail("fail unplaceable tasks", err)
	for i := range unplaceable {
		st := &unplaceable[i]
		obs.Warn(&s.log, obs.EventTaskFailed).Str(obs.FieldJobID, st.JobID).Int(obs.FieldTaskSeq, st.Seq).
			Str(obs.FieldTaskKind, string(st.Kind)).Bool("retryable", false).
			Str(obs.FieldError, st.Reason).Msg("task failed: no worker has room for its input")
		if st.JobFailed {
			s.jobFinished(ctx, st.JobID)
		}
	}
	counts["tasks_unplaceable"] = len(unplaceable)

	// Lease ends settle a failing job themselves; this is the backstop.
	settled, err := s.st.SettleFailingJobs(ctx)
	fail("settle failing jobs", err)
	for _, id := range settled {
		s.jobFinished(ctx, id)
	}
	counts["jobs_failed"] = len(settled)

	ids, err := s.st.JobsAwaitingAssembly(ctx)
	fail("jobs awaiting assembly", err)
	for _, id := range ids {
		if err := s.finishJob(ctx, id); err != nil {
			fail("assemble "+id, err)
			continue
		}
		counts["jobs_assembled"]++
	}

	purges, err := s.st.MediaToPurge(ctx, now.Add(-s.cfg.AudioRetention), now.Add(-s.cfg.FailedRetention))
	fail("media to purge", err)
	for _, p := range purges {
		o, c, err := s.purgeTerminalMedia(ctx, p.JobID, p.Original, p.Canonical)
		fail("purge media", err)
		counts["originals_purged"] += o
		counts["canonicals_purged"] += c
	}
	n, err := s.purgeFailedForDisk(ctx)
	fail("purge failed jobs' media for disk", err)
	counts["failed_media_purged_for_disk"] = n

	expired, err := s.st.JobsToDelete(ctx, now.Add(-s.cfg.JobRetention))
	fail("jobs to delete", err)
	for _, id := range expired {
		if err := s.deleteJob(ctx, SystemActor(), id); err != nil && !errors.Is(err, store.ErrNotFound) {
			fail("delete job", err)
			continue
		}
		counts["jobs_deleted"]++
	}

	n64, err := s.st.DeleteEndedLeases(ctx, now.Add(-s.cfg.EndedLeaseRetention))
	fail("delete ended leases", err)
	counts["leases_deleted"] = int(n64)
	n64, err = s.st.PruneThroughput(ctx, now.Add(-ThroughputRetention))
	fail("prune throughput", err)
	counts["throughput_buckets_deleted"] = int(n64)
	n64, err = s.ops.PrunePendingWorkers(ctx, now.Add(-s.cfg.PendingWorkerTTL))
	fail("delete pending workers", err)
	counts["pending_workers_deleted"] = int(n64)
	n64, err = s.ops.PruneRejectedWorkers(ctx, now.Add(-s.cfg.RejectedWorkerRetention))
	fail("delete rejected workers", err)
	counts["rejected_workers_deleted"] = int(n64)
	n64, err = s.ops.DeleteExpiredSessions(ctx)
	fail("delete sessions", err)
	counts["sessions_deleted"] = int(n64)

	ev := obs.Debug(&s.log, obs.EventReaperRun)
	for k, v := range counts {
		ev = ev.Int(k, v)
	}
	ev.Msg("reaper pass")
}

// touchedSince reports whether any file in a job directory was modified at
// or after t (or the directory cannot be read, which is not a reason to
// delete it).
func (s *Server) touchedSince(jobID string, t time.Time) bool {
	ents, err := os.ReadDir(filepath.Join(s.blobs.Root(), jobID))
	if err != nil {
		return !errors.Is(err, fs.ErrNotExist)
	}
	for _, e := range ents {
		info, err := e.Info()
		if err == nil && !info.ModTime().Before(t) {
			return true
		}
	}
	return false
}

// purgeTerminalMedia deletes a job's original and/or canonical media under
// the job lock, after re-reading the job: media is only ever purged from a
// done or failed job, so a job retried since it was selected keeps its
// media. It returns how many originals and canonicals it purged.
func (s *Server) purgeTerminalMedia(ctx context.Context, jobID string, original, canonical bool) (int, int, error) {
	unlock := s.locks.Lock(jobID)
	defer unlock()
	j, err := s.st.Job(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if !j.Terminal() {
		return 0, 0, nil
	}
	var o, c int
	if original && j.OriginalPurgedAt == nil {
		if err := s.purgeOriginal(ctx, jobID); err != nil {
			return o, c, err
		}
		o++
	}
	if canonical && j.Canonical != nil && j.CanonicalPurgedAt == nil {
		if err := s.purgeCanonical(ctx, jobID); err != nil {
			return o, c, err
		}
		c++
	}
	return o, c, nil
}

// diskOverage returns how many bytes the reaper should free from failed
// jobs' media (DESIGN.md §8.5): what the data filesystem is above
// FailedMaxDisk, or what it lacks of twice MinFreeBytes (the upload guard
// is near), whichever is more. It uses the same free-space probe as the
// upload guard and /readyz.
func (s *Server) diskOverage() int64 {
	free, err := s.freeBytes(s.cfg.DataDir)
	if err != nil {
		return 0
	}
	var need int64
	if total, err := s.totalBytes(s.cfg.DataDir); err == nil && total > 0 {
		need = (total - free) - s.cfg.FailedMaxDisk.Limit(total)
	}
	if low := 2 * s.cfg.MinFreeBytes; free < low {
		need = max(need, low-free)
	}
	return need
}

// purgeFailedForDisk purges failed jobs' media, oldest failure first, until
// the disk overage is covered, whatever their failed retention. Live jobs'
// media is never touched. It returns how many jobs it purged.
func (s *Server) purgeFailedForDisk(ctx context.Context) (int, error) {
	need := s.diskOverage()
	if need <= 0 {
		return 0, nil
	}
	ms, err := s.st.FailedJobMedia(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range ms {
		if need <= 0 {
			break
		}
		o, c, err := s.purgeTerminalMedia(ctx, m.JobID, m.Original, m.Canonical)
		if err != nil {
			return n, err
		}
		if o+c > 0 {
			n++
			need -= m.Bytes
			s.log.Info().Str(obs.FieldJobID, m.JobID).Int64("bytes", m.Bytes).
				Msg("purged a failed job's media to free disk space")
		}
	}
	return n, nil
}

func (s *Server) purgeCanonical(ctx context.Context, jobID string) error {
	if err := s.blobs.Remove(jobID, blobs.Canonical, blobs.CanonicalIndex); err != nil {
		return err
	}
	if err := s.ops.MediaPurged(ctx, jobID, MediaCanonical); err != nil {
		return err
	}
	obs.Info(&s.log, obs.EventJobPurged).Str(obs.FieldJobID, jobID).Str(obs.FieldWhat, "canonical").Msg("media purged")
	return nil
}

// sweep removes the leftovers of interrupted writes: ".part" files and job
// directories with no job row (a crash between writing an upload and
// committing it, or a deletion whose blob removal failed). Only files older
// than Config.OrphanGrace are touched. A row-less directory is spared while
// its upload is still being received (however slowly: a directory's mtime
// does not change as its part file grows) and while anything in it is
// newer than the cutoff.
func (s *Server) sweep(ctx context.Context) {
	cutoff := s.now().Add(-s.cfg.OrphanGrace)
	parts, err := s.blobs.SweepParts(cutoff)
	if err != nil {
		s.log.Warn().Err(err).Msg("sweeping part files failed")
	}
	dirs, err := s.blobs.Jobs()
	if err != nil {
		s.log.Warn().Err(err).Msg("listing blob directories failed")
	}
	orphans := 0
	for _, d := range dirs {
		if !d.ModTime.Before(cutoff) || s.uploads.has(d.ID) || s.touchedSince(d.ID, cutoff) {
			continue
		}
		if _, err := s.st.Job(ctx, d.ID); !errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err := s.blobs.RemoveJob(d.ID); err == nil {
			orphans++
		}
	}
	lvl := zerolog.DebugLevel
	if parts > 0 || orphans > 0 {
		lvl = zerolog.InfoLevel
	}
	s.log.WithLevel(lvl).Int("part_files", parts).Int("orphan_dirs", orphans).Msg("blob sweep")
}
