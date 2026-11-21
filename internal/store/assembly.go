package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// TaskOutput is one finished task's output and provenance.
type TaskOutput struct {
	Seq                    int
	StartSample, EndSample int64
	// Result is the segments JSON (transcribe only).
	Result json.RawMessage
	// EngineVersion is whisper.cpp's version for transcribe, ffmpeg's for
	// prepare.
	EngineVersion string
	WorkerID      string
	ComputeMS     int64
}

// JobOutputs is everything needed to assemble a transcript.
type JobOutputs struct {
	// Prepare is nil when the upload took the fast path.
	Prepare *TaskOutput
	// Transcribe holds the done transcribe tasks in seq order.
	Transcribe []TaskOutput
	// Missing holds the transcribe tasks that are not done, in seq order,
	// with only Seq and the sample range set.
	Missing []TaskOutput
}

// JobOutputs returns the finished task outputs of a job.
func (s *Store) JobOutputs(ctx context.Context, jobID string) (*JobOutputs, error) {
	var n int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE id = ?`, jobID).Scan(&n); err != nil {
		return nil, fmt.Errorf("job outputs: %w", err)
	}
	if n == 0 {
		return nil, fmt.Errorf("job %s: %w", jobID, ErrNotFound)
	}
	rows, err := s.r.QueryContext(ctx, `SELECT seq, kind, state = 'done', COALESCE(start_sample, 0),
		COALESCE(end_sample, 0), result, COALESCE(engine_version, ''), COALESCE(worker_id, ''), COALESCE(compute_ms, 0)
		FROM tasks WHERE job_id = ? AND kind != 'fetch' AND (state = 'done' OR kind = 'transcribe') ORDER BY seq`, jobID)
	if err != nil {
		return nil, fmt.Errorf("job outputs: %w", err)
	}
	defer rows.Close()
	out := &JobOutputs{}
	for rows.Next() {
		var t TaskOutput
		var kind string
		var done bool
		var result sql.NullString
		if err := rows.Scan(&t.Seq, &kind, &done, &t.StartSample, &t.EndSample, &result, &t.EngineVersion,
			&t.WorkerID, &t.ComputeMS); err != nil {
			return nil, fmt.Errorf("job outputs: %w", err)
		}
		if !done {
			out.Missing = append(out.Missing, TaskOutput{Seq: t.Seq, StartSample: t.StartSample, EndSample: t.EndSample})
			continue
		}
		if result.Valid {
			t.Result = json.RawMessage(result.String)
		}
		if kind == "prepare" {
			out.Prepare = &t
			continue
		}
		out.Transcribe = append(out.Transcribe, t)
	}
	return out, rows.Err()
}

// MarkJobDone marks a job done with completed_at = now and compute_ms = the
// sum over its tasks. Every transcribe task must be done (else ErrConflict).
// Marking a done job again returns it unchanged.
func (s *Store) MarkJobDone(ctx context.Context, jobID string) (*Job, error) {
	var job *Job
	err := s.write(ctx, func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id = ?`, jobID).Scan(&state); err != nil {
			return notFound(err, "job "+jobID)
		}
		if state != "done" {
			complete, err := transcriptionComplete(ctx, tx, jobID)
			if err != nil {
				return err
			}
			if state == "failed" || !complete {
				return fmt.Errorf("mark job %s done from %s (transcription complete: %v): %w",
					jobID, state, complete, ErrConflict)
			}
			now := toMS(s.now())
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state = 'done', completed_at = ?, updated_at = ?,
				compute_ms = (SELECT COALESCE(SUM(compute_ms), 0) FROM tasks WHERE job_id = jobs.id)
				WHERE id = ?`, now, now, jobID); err != nil {
				return fmt.Errorf("mark job done: %w", err)
			}
		}
		var err error
		job, err = getJob(ctx, tx, jobID)
		return err
	})
	return job, err
}
