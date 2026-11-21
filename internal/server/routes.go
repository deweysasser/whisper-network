package server

import (
	"net/http"
)

// routes registers every endpoint. The API is DESIGN.md §6; the admin
// endpoints are documented in admin.go.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Health (§10.3), unauthenticated.
	mux.Handle("GET /healthz", http.HandlerFunc(s.handleHealthz))
	mux.Handle("GET /readyz", http.HandlerFunc(s.handleReadyz))

	// Usage guide for LLM agents (llms.txt), unauthenticated.
	mux.Handle("GET /llms.txt", http.HandlerFunc(s.handleLLMs))

	// Client API (§6.1).
	mux.Handle("POST /queue/{q}", s.authed(anyHuman, s.handleUpload))
	mux.Handle("POST /queue/{q}/fetch", s.authed(anyHuman, s.handleFetchSubmit))
	mux.Handle("GET /queue/{q}", s.authed(anyHuman, s.handleListJobs))
	mux.Handle("PATCH /queue/{q}", s.authed(anyHuman, s.handlePatchQueue))
	mux.Handle("GET /job/{id}", s.authed(anyHuman, s.handleGetJob))
	mux.Handle("GET /job/{id}/transcript", s.authed(anyHuman, s.handleTranscript))
	mux.Handle("GET /job/{id}/result", s.authed(anyHuman, s.handleResult))
	mux.Handle("DELETE /job/{id}", s.authed(anyHuman, s.handleDeleteJob))
	mux.Handle("POST /job/{id}/retry", s.authed(anyHuman, s.handleRetryJob))
	mux.Handle("POST /queue/{q}/bulk", s.authed(anyHuman, s.handleBulkJobs))
	mux.Handle("POST /client", s.authed(anyHuman, s.handleCreateClient))
	mux.Handle("GET /client/{id}", s.authed(anyHuman, s.handleGetClient))
	mux.Handle("DELETE /client/{id}", s.authed(anyHuman, s.handleRevokeClient))

	// Worker API (§6.2).
	mux.Handle("POST /worker", s.public(s.handleRegister))
	mux.Handle("GET /worker/{id}", s.authed(anyKind, s.handleGetWorker))
	mux.Handle("PATCH /worker/{id}", s.authed(workerOnly, s.handlePatchWorker))
	mux.Handle("POST /worker/{id}/leases", s.authed(workerOnly, s.handleClaim))
	mux.Handle("POST /worker/{id}/gauges", s.authed(workerOnly, s.handleGauges))
	mux.Handle("GET /lease/{id}/input", s.authed(workerOnly, s.handleInput))
	mux.Handle("POST /lease/{id}/heartbeat", s.authed(workerOnly, s.handleHeartbeat))
	mux.Handle("PUT /lease/{id}/result", s.authed(workerOnly, s.handleLeaseResult))
	mux.Handle("POST /lease/{id}/failure", s.authed(workerOnly, s.handleFailure))
	mux.Handle("DELETE /lease/{id}", s.authed(workerOnly, s.handleRelease))
	mux.Handle("GET /models/{name}", s.authed(workerOnly, s.handleModel))

	// Admin API (admin.go).
	mux.Handle("GET /admin/workers", s.authed(anyHuman, s.handleAdminListWorkers))
	mux.Handle("GET /admin/version", s.authed(anyHuman, s.handleAdminVersion))
	mux.Handle("POST /admin/workers/{id}/approve", s.authed(anyHuman, s.handleAdminApprove))
	mux.Handle("POST /admin/workers/{id}/pause", s.authed(anyHuman, s.handleAdminWorkerState))
	mux.Handle("POST /admin/workers/{id}/resume", s.authed(anyHuman, s.handleAdminWorkerState))
	mux.Handle("POST /admin/workers/{id}/reject", s.authed(anyHuman, s.handleAdminWorkerState))
	mux.Handle("DELETE /admin/workers/{id}", s.authed(anyHuman, s.handleAdminDeleteWorker))
	mux.Handle("GET /admin/queues", s.authed(anyHuman, s.handleAdminListQueues))
	mux.Handle("POST /admin/queues", s.authed(anyHuman, s.handleAdminCreateQueue))
	mux.Handle("GET /admin/users", s.authed(anyHuman, s.handleAdminListUsers))
	mux.Handle("POST /admin/users", s.authed(anyHuman, s.handleAdminCreateUser))
	mux.Handle("GET /admin/clients", s.authed(anyHuman, s.handleAdminListClients))
	mux.Handle("GET /admin/grants", s.authed(anyHuman, s.handleAdminListGrants))
	mux.Handle("POST /admin/grants", s.authed(anyHuman, s.handleAdminGrant))
	mux.Handle("DELETE /admin/grants", s.authed(anyHuman, s.handleAdminRevoke))
	mux.Handle("GET /admin/fetch-policies", s.authed(anyHuman, s.handleAdminListFetchPolicies))
	mux.Handle("PUT /admin/fetch-policies/{key}", s.authed(anyHuman, s.handleAdminPutFetchPolicy))
	mux.Handle("DELETE /admin/fetch-policies/{key}", s.authed(anyHuman, s.handleAdminDeleteFetchPolicy))
	mux.Handle("GET /admin/fetch-cooldowns", s.authed(anyHuman, s.handleAdminListCooldowns))
	mux.Handle("POST /admin/fetch-cooldowns", s.authed(anyHuman, s.handleAdminSetCooldown))
	mux.Handle("DELETE /admin/fetch-cooldowns/{egress}/{site}", s.authed(anyHuman, s.handleAdminClearCooldown))
	mux.Handle("GET /admin/audit", s.authed(anyHuman, s.handleAdminAudit))

	// UI (§6.3). "/" redirects to it; the UI owns everything under /ui/.
	mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))
	if s.ui != nil {
		mux.Handle("/ui/", s.ui)
	} else {
		mux.Handle("GET /ui/", http.HandlerFunc(uiPlaceholder))
	}
	mux.Handle("/", s.public(func(http.ResponseWriter, *http.Request) error { return notFound() }))
	return mux
}

func uiPlaceholder(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("<!doctype html><title>whisper-network</title>" +
		"<p>whisper-network is running. The web UI is not part of this build.</p>\n"))
}
