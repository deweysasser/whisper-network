-- Retry elsewhere (DESIGN.md §8.2): a claim skips a task the claiming worker
-- recently failed or let expire, and a failed job names the workers of its
-- failed attempts. Both look up a task's failed and expired leases.
CREATE INDEX IF NOT EXISTS leases_task_failures ON leases (job_id, seq, worker_id, ended_at)
  WHERE outcome IN ('failed','expired');
