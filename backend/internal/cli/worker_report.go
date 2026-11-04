package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// workerReport mirrors the explicit v1 store envelope. No field is inferred
// from environment, runtime activity, or provider completion.
type workerReport struct {
	ID, Type string
	Version  int
	Identity struct {
		ID, AttemptID, ProjectID, Repository, ExecutionBaseSHA           string
		OriginalParentSessionID, OriginalWorkerSessionID, OwnerSessionID string
		OwnerGeneration                                                  int64
		WorktreePath, Branch                                             string
	}
	LeaseID                                                                 string
	LeaseRevision                                                           int64
	WorkerSessionID, NativeSessionID, NativeTurnID, OriginalNativeSessionID string
	Content, ArtifactReference, Digest                                      string
}

func newWorkerCommand(ctx *commandContext) *cobra.Command {
	worker := &cobra.Command{Use: "worker", Short: "Submit explicit worker assertions"}
	var file string
	report := &cobra.Command{
		Use:   "report --file <path|->",
		Short: "Accept a v1 report and enqueue it durably; does not prove parent ACK",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if file == "" {
				return usageError{fmt.Errorf("--file is required (use - for stdin)")}
			}
			input := ctx.deps.In
			if file != "-" {
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				defer f.Close()
				input = f
			}
			body, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
			if err != nil {
				return err
			}
			if len(body) > 1<<20 {
				return usageError{fmt.Errorf("report exceeds 1 MiB")}
			}
			var in workerReport
			dec := json.NewDecoder(bytes.NewReader(body))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&in); err != nil {
				return usageError{fmt.Errorf("report JSON: %w", err)}
			}
			if err := dec.Decode(new(any)); err != io.EOF {
				return usageError{fmt.Errorf("report must contain one JSON object")}
			}
			var receipt json.RawMessage
			if err := ctx.postJSON(cmd.Context(), "worker/reports", in, &receipt); err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), receipt)
		},
	}
	report.Flags().StringVar(&file, "file", "", "Explicit WorkerReport JSON file, or - for stdin")
	worker.AddCommand(report)
	return worker
}
