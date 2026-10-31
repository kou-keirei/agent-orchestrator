//go:build windows && ao_experimental_persistenthost_job

package persistenthost

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// These fixtures use no native handles or helper processes. They are source
// regression specifications, not evidence of Windows process containment.
type experimentalJobRecord struct {
	calls         []string
	fail          string
	foreignParent bool
	foreignChild  bool
	missingMember bool
	limits        windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	closed        []windows.Handle
	terminated    []windows.Handle
	rootClosed    chan struct{}
	onMembership  func()
}

func experimentalJobFixture() (experimentalJobLaunch, experimentalJobCalls, *experimentalJobRecord) {
	cfg := experimentalJobLaunch{
		Enabled: true, Owner: experimentalJobOwner{SessionID: "fixture", OwnerGeneration: 1, RuntimeGeneration: 2},
		Executable: `C:\fixture\provider.exe`, Workdir: `C:\fixture`, Environment: []string{},
		Stdio: [3]windows.Handle{10, 11, 12}, ProcessLimit: 3, CommitBytes: 64 << 20,
	}
	r := &experimentalJobRecord{rootClosed: make(chan struct{}, 1)}
	step := func(name string) error {
		r.calls = append(r.calls, name)
		if r.fail == name {
			return fmt.Errorf("injected %s failure", name)
		}
		return nil
	}
	api := experimentalJobCalls{
		inJob: func(process, job windows.Handle) (bool, error) {
			if process == windows.CurrentProcess() {
				return r.foreignParent, step("parent")
			}
			if job == 0 {
				return r.foreignChild, step("child")
			}
			err := step("membership")
			if r.onMembership != nil {
				r.onMembership()
			}
			return !r.missingMember, err
		},
		createJob: func() (windows.Handle, error) {
			if err := step("job"); err != nil {
				return 0, err
			}
			return 40, nil
		},
		setLimits: func(_ windows.Handle, limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error {
			r.limits = limits
			return step("limits")
		},
		duplicate: func(handle windows.Handle) (windows.Handle, error) {
			if err := step(fmt.Sprintf("dup%d", handle)); err != nil {
				return 0, err
			}
			return handle + 10, nil
		},
		create: func(_ experimentalJobLaunch, handles [3]windows.Handle) (windows.ProcessInformation, error) {
			if handles != [3]windows.Handle{20, 21, 22} {
				return windows.ProcessInformation{}, errors.New("stdio originals escaped duplication")
			}
			if err := step("create-suspended"); err != nil {
				return windows.ProcessInformation{}, err
			}
			return windows.ProcessInformation{Process: 50, Thread: 51}, nil
		},
		assign: func(job, process windows.Handle) error {
			if job != 40 || process != 50 {
				return errors.New("assignment targeted an unrelated handle")
			}
			return step("assign")
		},
		resume: func(thread windows.Handle) (uint32, error) {
			if thread != 51 {
				return 0, errors.New("resumed an unrelated thread")
			}
			return 1, step("resume")
		},
		terminateJob: func(job windows.Handle) error {
			r.terminated = append(r.terminated, job)
			return step("terminate-job")
		},
		terminate: func(process windows.Handle) error {
			r.terminated = append(r.terminated, process)
			return step("terminate-root")
		},
		wait: func(windows.Handle, uint32) (uint32, error) {
			return windows.WAIT_OBJECT_0, step("wait")
		},
		exitCode: func(_ windows.Handle, code *uint32) error {
			*code = 0
			return step("exit-code")
		},
		close: func(handle windows.Handle) error {
			r.closed = append(r.closed, handle)
			if handle == 50 {
				r.rootClosed <- struct{}{}
			}
			return step(fmt.Sprintf("close%d", handle))
		},
	}
	return cfg, api, r
}

func experimentalJobCalled(r *experimentalJobRecord, name string) bool {
	for _, call := range r.calls {
		if call == name {
			return true
		}
	}
	return false
}

func TestExperimentalWindowsJobRejectsInvalidConfigBeforeNativeCalls(t *testing.T) {
	cases := map[string]func(*experimentalJobLaunch){
		"disabled":           func(c *experimentalJobLaunch) { c.Enabled = false },
		"owner":              func(c *experimentalJobLaunch) { c.Owner.OwnerGeneration = 0 },
		"runtime":            func(c *experimentalJobLaunch) { c.Owner.RuntimeGeneration = 0 },
		"session":            func(c *experimentalJobLaunch) { c.Owner.SessionID = "" },
		"zero-processes":     func(c *experimentalJobLaunch) { c.ProcessLimit = 0 },
		"too-many-processes": func(c *experimentalJobLaunch) { c.ProcessLimit = 4 },
		"zero-memory":        func(c *experimentalJobLaunch) { c.CommitBytes = 0 },
		"too-much-memory":    func(c *experimentalJobLaunch) { c.CommitBytes = 1<<30 + 1 },
		"shim":               func(c *experimentalJobLaunch) { c.Executable = `C:\fixture\provider.cmd` },
		"relative-exe":       func(c *experimentalJobLaunch) { c.Executable = "provider.exe" },
		"relative-workdir":   func(c *experimentalJobLaunch) { c.Workdir = "." },
		"inherited-env":      func(c *experimentalJobLaunch) { c.Environment = nil },
		"invalid-stdio":      func(c *experimentalJobLaunch) { c.Stdio[0] = windows.InvalidHandle },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg, api, r := experimentalJobFixture()
			mutate(&cfg)
			p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
			var hold *experimentalJobHold
			if p != nil || !errors.As(err, &hold) || hold.Code != "SOURCE_CONTRACT_HOLD" || len(r.calls) != 0 {
				t.Fatalf("invalid configuration reached native calls: process=%v error=%v calls=%v", p, err, r.calls)
			}
		})
	}
}

func TestExperimentalWindowsJobConstructorFailuresNeverResume(t *testing.T) {
	for _, stage := range []string{"parent", "job", "limits", "dup10", "dup11", "dup12", "create-suspended", "child", "assign", "membership"} {
		t.Run(stage, func(t *testing.T) {
			cfg, api, r := experimentalJobFixture()
			r.fail = stage
			p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
			var hold *experimentalJobHold
			if p != nil || !errors.As(err, &hold) || hold.Code != "ENVIRONMENT_BLOCKED" || experimentalJobCalled(r, "resume") {
				t.Fatalf("constructor failure resumed provider: process=%v error=%v calls=%v", p, err, r.calls)
			}
			if stage == "job" || stage == "parent" {
				if experimentalJobCalled(r, "create-suspended") {
					t.Fatal("Job admission failure created a provider")
				}
			}
			if experimentalJobCalled(r, "child") {
				if !experimentalJobCalled(r, "terminate-root") || !experimentalJobCalled(r, "close50") || !experimentalJobCalled(r, "close51") {
					t.Fatalf("unadmitted suspended root leaked: %v", r.calls)
				}
			}
		})
	}
}

func TestExperimentalWindowsJobRejectsForeignOrMissingMembership(t *testing.T) {
	for _, condition := range []string{"parent", "child", "missing"} {
		t.Run(condition, func(t *testing.T) {
			cfg, api, r := experimentalJobFixture()
			r.foreignParent, r.foreignChild, r.missingMember = condition == "parent", condition == "child", condition == "missing"
			p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
			if p != nil || err == nil || experimentalJobCalled(r, "resume") {
				t.Fatalf("uncertain ownership admitted: process=%v error=%v calls=%v", p, err, r.calls)
			}
		})
	}
}

func TestExperimentalWindowsJobMembershipAndLimitsPrecedeResume(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	want := []string{"parent", "job", "limits", "dup10", "dup11", "dup12", "create-suspended", "close20", "close21", "close22", "child", "assign", "membership", "resume", "close51"}
	if !reflect.DeepEqual(r.calls, want) || p.owner != cfg.Owner {
		t.Fatalf("launch order/owner mismatch: %v owner=%+v", r.calls, p.owner)
	}
	wantFlags := uint32(windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
	if r.limits.BasicLimitInformation.LimitFlags != wantFlags || r.limits.BasicLimitInformation.ActiveProcessLimit != cfg.ProcessLimit || r.limits.JobMemoryLimit != cfg.CommitBytes {
		t.Fatalf("Job limits or no-breakaway contract changed: %+v", r.limits)
	}
}

func TestExperimentalWindowsJobCanceledBeforeResume(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onMembership = cancel
	p, err := launchExperimentalWindowsJobWithCalls(ctx, cfg, api)
	if p != nil || !errors.Is(err, context.Canceled) || experimentalJobCalled(r, "resume") || !experimentalJobCalled(r, "close50") {
		t.Fatalf("canceled admission ran or leaked provider: process=%v error=%v calls=%v", p, err, r.calls)
	}
}

func TestExperimentalWindowsJobUnexpectedResumeOrThreadCloseFailsLaunch(t *testing.T) {
	for _, failure := range []string{"resume-error", "resume-zero", "resume-two", "thread-close"} {
		t.Run(failure, func(t *testing.T) {
			cfg, api, r := experimentalJobFixture()
			switch failure {
			case "resume-error":
				r.fail = "resume"
			case "resume-zero":
				api.resume = func(windows.Handle) (uint32, error) { return 0, nil }
			case "resume-two":
				api.resume = func(windows.Handle) (uint32, error) { return 2, nil }
			case "thread-close":
				r.fail = "close51"
			}
			p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
			var hold *experimentalJobHold
			if p != nil || !errors.As(err, &hold) || !experimentalJobCalled(r, "terminate-job") || !experimentalJobCalled(r, "close50") {
				t.Fatalf("failed launch retained ownership: process=%v error=%v calls=%v", p, err, r.calls)
			}
		})
	}
}

func TestExperimentalWindowsJobCancellationClosesOnlyOwnedHandles(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := launchExperimentalWindowsJobWithCalls(ctx, cfg, api)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	cancel()
	select {
	case <-r.rootClosed:
	case <-time.After(time.Second):
		t.Fatal("launch-context cancellation did not close the owned process")
	}
	if err := p.Close(); err != nil { // Synchronize with callback and verify idempotence.
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.terminated, []windows.Handle{40}) || !reflect.DeepEqual(r.closed, []windows.Handle{20, 21, 22, 51, 40, 50}) {
		t.Fatalf("cleanup touched caller stdio, a sibling, or repeated a close: terminated=%v closed=%v", r.terminated, r.closed)
	}
}

func TestExperimentalWindowsJobNativeCreationRequiresSuspension(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "child_windows_job.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var flags []string
	createCalls := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "nativeExperimentalJobCreate" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "CreateProcess" {
					createCalls++
					if len(call.Args) != 10 {
						t.Fatal("unexpected CreateProcess API shape")
					}
					name, ok := call.Args[5].(*ast.Ident)
					if !ok || name.Name != "flags" {
						t.Fatal("native CreateProcess bypassed mandatory flags")
					}
				}
			}
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				return true
			}
			name, ok := assignment.Lhs[0].(*ast.Ident)
			if !ok || name.Name != "flags" {
				return true
			}
			ast.Inspect(assignment.Rhs[0], func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					flags = append(flags, selector.Sel.Name)
				}
				return true
			})
			return true
		})
	}
	want := []string{"CREATE_SUSPENDED", "CREATE_NO_WINDOW", "CREATE_UNICODE_ENVIRONMENT", "EXTENDED_STARTUPINFO_PRESENT"}
	if createCalls != 1 || !reflect.DeepEqual(flags, want) {
		t.Fatalf("native creation flags = %v, want mandatory suspension and no breakaway: %v", flags, want)
	}
}

func TestExperimentalWindowsJobExplicitEnvironment(t *testing.T) {
	for _, entries := range [][]string{{"A=1", "a=2"}, {"invalid"}, {"A=\x00"}} {
		if _, err := experimentalJobEnvironment(entries); err == nil {
			t.Fatalf("invalid environment accepted: %q", entries)
		}
	}
	block, err := experimentalJobEnvironment([]string{})
	if err != nil || !reflect.DeepEqual(block, []uint16{0, 0}) {
		t.Fatalf("empty environment = %v, %v; want explicit double-NUL", block, err)
	}
}
