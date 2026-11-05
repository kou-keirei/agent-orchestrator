package controllers

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	storagesvc "github.com/aoagents/agent-orchestrator/backend/internal/service/storage"
)

// StorageController exposes retention reports without filesystem or cleanup authority.
type StorageController struct {
	Source  storagesvc.WorkspaceSource
	DataDir string
}

// Register mounts only GET operations. Both views retain every reported object.
func (c *StorageController) Register(r chi.Router) {
	r.Get("/storage/status", c.read)
	r.Get("/storage/cleanup-plan", c.read)
}

func (c *StorageController) read(w http.ResponseWriter, r *http.Request) {
	limit := 100
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		envelope.WriteError(w, r, apierr.Invalid("INVALID_STORAGE_PAGE", "invalid storage page query", nil))
		return
	}
	for key, values := range query {
		if (key != "after" && key != "limit") || len(values) != 1 {
			envelope.WriteError(w, r, apierr.Invalid("INVALID_STORAGE_PAGE", "only single after and limit parameters are supported", nil))
			return
		}
	}
	if raw := query.Get("limit"); raw != "" {
		var err error
		limit, err = strconv.Atoi(raw)
		if err != nil {
			limit = 0
		}
	}
	after := query.Get("after")
	if limit < 1 || limit > storagesvc.MaxStatusPage || len(after) > 4096 || strings.ContainsRune(after, '\x00') {
		envelope.WriteError(w, r, apierr.Invalid("INVALID_STORAGE_PAGE", "limit must be 1..256; after must be a bounded cursor", nil))
		return
	}
	if c.Source == nil {
		apispec.NotImplemented(w, r, http.MethodGet, r.URL.Path)
		return
	}
	status, err := storagesvc.ReadStatus(r.Context(), c.Source, c.DataDir, after, limit)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, StorageStatusResponse(status))
}
