package store_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

func TestSessionSourceRefInsertUpdateListAndReopen(t *testing.T) {
	dir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dir)
	seedProject(t, s, "source")
	rec := sampleRecord("source")
	rec.Metadata.ConversationCheckpointState = domain.ConversationCheckpointEmpty
	rec.Metadata.ExecutionBaseSHA = strings.Repeat("a", 40)
	rec.Metadata.SourceRef = "refs/heads/source"
	rec.Metadata.DiffBaseSHA = strings.Repeat("b", 40)
	rec.Metadata.DiffBaseRef = "refs/heads/comparison"
	created, err := s.CreateSession(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
	check := func(st *sqlite.Store, want domain.SessionMetadata) {
		t.Helper()
		got, found, err := st.GetSession(t.Context(), created.ID)
		if err != nil || !found || !reflect.DeepEqual(got.Metadata, want) {
			t.Fatalf("source readback: %+v found=%v err=%v want=%+v", got.Metadata, found, err, want)
		}
		project, err := st.ListSessions(t.Context(), "source")
		if err != nil || len(project) != 1 || !reflect.DeepEqual(project[0].Metadata, want) {
			t.Fatalf("project source list: %+v err=%v", project, err)
		}
		all, err := st.ListAllSessions(t.Context())
		if err != nil || len(all) != 1 || !reflect.DeepEqual(all[0].Metadata, want) {
			t.Fatalf("all source list: %+v err=%v", all, err)
		}
	}
	check(s, rec.Metadata)
	created.Metadata.SourceRef = "refs/remotes/origin/source"
	if err := s.UpdateSession(t.Context(), created); err != nil {
		t.Fatal(err)
	}
	check(s, created.Metadata)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	check(reopened, created.Metadata)
}
