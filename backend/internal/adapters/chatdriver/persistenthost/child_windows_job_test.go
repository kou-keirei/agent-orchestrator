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
	calls          []string
	fail           string
	foreignParent  bool
	foreignChild   bool
	missingMember  bool
	memberOnCreate bool
	createdJob     windows.Handle
	limits         windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	closed         []windows.Handle
	terminated     []windows.Handle
	rootClosed     chan struct{}
	onMembership   func()
	onCreate       func()
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
			if process != 50 || job != 40 {
				return false, errors.New("membership queried against an unrelated process/Job")
			}
			err := step("membership")
			if r.onMembership != nil {
				r.onMembership()
			}
			return r.memberOnCreate && !r.missingMember, err
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
		create: func(_ experimentalJobLaunch, handles [3]windows.Handle, job windows.Handle) (windows.ProcessInformation, error) {
			if handles != [3]windows.Handle{20, 21, 22} {
				return windows.ProcessInformation{}, errors.New("stdio originals escaped duplication")
			}
			if job != 40 || r.limits.BasicLimitInformation.ActiveProcessLimit == 0 || r.limits.JobMemoryLimit == 0 {
				return windows.ProcessInformation{}, errors.New("creation did not receive an already-limited owned Job")
			}
			if err := step("create-job-member"); err != nil {
				return windows.ProcessInformation{}, err
			}
			if r.foreignChild {
				return windows.ProcessInformation{}, errors.New("native creation rejected a foreign/nested Job")
			}
			r.memberOnCreate, r.createdJob = true, job
			if r.onCreate != nil {
				r.onCreate()
			}
			return windows.ProcessInformation{Process: 50, Thread: 51}, nil
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
	for _, stage := range []string{"parent", "job", "limits", "dup10", "dup11", "dup12", "create-job-member", "membership"} {
		t.Run(stage, func(t *testing.T) {
			cfg, api, r := experimentalJobFixture()
			r.fail = stage
			p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
			var hold *experimentalJobHold
			if p != nil || !errors.As(err, &hold) || hold.Code != "ENVIRONMENT_BLOCKED" || experimentalJobCalled(r, "resume") {
				t.Fatalf("constructor failure resumed provider: process=%v error=%v calls=%v", p, err, r.calls)
			}
			if stage == "job" || stage == "parent" {
				if experimentalJobCalled(r, "create-job-member") {
					t.Fatal("Job admission failure created a provider")
				}
			}
			if r.memberOnCreate {
				if !experimentalJobCalled(r, "terminate-job") || !experimentalJobCalled(r, "close50") || !experimentalJobCalled(r, "close51") || !reflect.DeepEqual(r.terminated, []windows.Handle{40}) {
					t.Fatalf("creation-time Job ownership was not cleaned up: %v", r.calls)
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

func TestExperimentalWindowsJobMembershipOnCreateUsesPriorLimits(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	want := []string{"parent", "job", "limits", "dup10", "dup11", "dup12", "create-job-member", "membership", "close20", "close21", "close22", "resume", "close51"}
	if !reflect.DeepEqual(r.calls, want) || p.owner != cfg.Owner || !r.memberOnCreate || r.createdJob != 40 {
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

func TestExperimentalWindowsJobCancellationImmediatelyAfterAtomicCreate(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onCreate = func() {
		if !r.memberOnCreate || r.createdJob != 40 {
			t.Fatal("creation returned a process outside the already-limited Job")
		}
		cancel()
	}
	p, err := launchExperimentalWindowsJobWithCalls(ctx, cfg, api)
	if p != nil || !errors.Is(err, context.Canceled) || !r.memberOnCreate || experimentalJobCalled(r, "resume") ||
		!reflect.DeepEqual(r.terminated, []windows.Handle{40}) || !experimentalJobCalled(r, "close50") || !experimentalJobCalled(r, "close51") {
		t.Fatalf("post-create cancellation lost atomic Job ownership: process=%v error=%v calls=%v", p, err, r.calls)
	}
}

func TestExperimentalWindowsJobUnsupportedAtomicCreateNeverFallsBack(t *testing.T) {
	cfg, api, r := experimentalJobFixture()
	api.create = func(experimentalJobLaunch, [3]windows.Handle, windows.Handle) (windows.ProcessInformation, error) {
		r.calls = append(r.calls, "unsupported-job-list")
		return windows.ProcessInformation{}, windows.ERROR_NOT_SUPPORTED
	}
	p, err := launchExperimentalWindowsJobWithCalls(context.Background(), cfg, api)
	var hold *experimentalJobHold
	if p != nil || !errors.As(err, &hold) || hold.Code != "ENVIRONMENT_BLOCKED" || !errors.Is(err, windows.ERROR_NOT_SUPPORTED) ||
		experimentalJobCalled(r, "resume") || experimentalJobCalled(r, "close50") || !experimentalJobCalled(r, "close40") {
		t.Fatalf("unsupported atomic creation selected a fallback: process=%v error=%v calls=%v", p, err, r.calls)
	}
}

func TestExperimentalWindowsJobNativeCreationRejectsMissingJob(t *testing.T) {
	cfg, _, _ := experimentalJobFixture()
	for _, job := range []windows.Handle{0, windows.InvalidHandle} {
		// The native entry must reject these before any Win32 call or process.
		pi, err := nativeExperimentalJobCreate(cfg, [3]windows.Handle{}, job)
		var hold *experimentalJobHold
		if pi != (windows.ProcessInformation{}) || !errors.As(err, &hold) || hold.Code != "ENVIRONMENT_BLOCKED" {
			t.Fatalf("missing Job reached native creation: info=%+v error=%v", pi, err)
		}
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

func TestExperimentalWindowsJobNativeCreationRequiresAtomicJobAndStdioAttributes(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "child_windows_job.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ident := func(expr ast.Expr, name string) bool {
		value, ok := expr.(*ast.Ident)
		return ok && value.Name == name
	}
	selector := func(expr ast.Expr, base, name string) bool {
		value, ok := expr.(*ast.SelectorExpr)
		return ok && ident(value.X, base) && value.Sel.Name == name
	}
	attributeStorage := func(args []ast.Expr, name string) bool {
		if len(args) != 3 {
			return false
		}
		pointer, ok := args[1].(*ast.CallExpr)
		if !ok || !selector(pointer.Fun, "unsafe", "Pointer") || len(pointer.Args) != 1 {
			return false
		}
		address, ok := pointer.Args[0].(*ast.UnaryExpr)
		if !ok || address.Op != token.AND {
			return false
		}
		index, ok := address.X.(*ast.IndexExpr)
		if !ok || !ident(index.X, name) {
			return false
		}
		zero, ok := index.Index.(*ast.BasicLit)
		if !ok || zero.Value != "0" {
			return false
		}
		size, ok := args[2].(*ast.CallExpr)
		return ok && selector(size.Fun, "unsafe", "Sizeof") && len(size.Args) == 1 && ident(size.Args[0], name)
	}
	var fn *ast.FuncDecl
	constantOK := false
	for _, decl := range file.Decls {
		if candidate, ok := decl.(*ast.FuncDecl); ok && candidate.Name.Name == "nativeExperimentalJobCreate" {
			fn = candidate
		}
		if values, ok := decl.(*ast.GenDecl); ok && values.Tok == token.CONST {
			for _, spec := range values.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || value.Names[0].Name != "experimentalJobListAttribute" || len(value.Values) != 1 {
					continue
				}
				literal, ok := value.Values[0].(*ast.BasicLit)
				constantOK = ok && literal.Value == "0x0002000d"
			}
		}
	}
	if fn == nil || !constantOK {
		t.Fatal("missing native constructor or WinSDK Job-list attribute")
	}
	var capacity, jobArray, jobAttribute, handleAttribute, startupList, create token.Pos
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
			if ident(assignment.Lhs[0], "jobs") {
				literal, ok := assignment.Rhs[0].(*ast.CompositeLit)
				if !ok || len(literal.Elts) != 1 || !ident(literal.Elts[0], "job") {
					t.Fatal("Job-list storage does not contain the supplied owned Job")
				}
				array, ok := literal.Type.(*ast.ArrayType)
				if !ok || !selector(array.Elt, "windows", "Handle") {
					t.Fatal("invalid Job-list array type")
				}
				length, ok := array.Len.(*ast.BasicLit)
				if !ok || length.Value != "1" {
					t.Fatal("creation must specify exactly the owned Job")
				}
				jobArray = assignment.Pos()
			}
			if selector(assignment.Lhs[0], "si", "ProcThreadAttributeList") {
				call, ok := assignment.Rhs[0].(*ast.CallExpr)
				if !ok || !selector(call.Fun, "attributes", "List") || len(call.Args) != 0 {
					t.Fatal("STARTUPINFOEX discarded its configured attribute list")
				}
				startupList = assignment.Pos()
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector(call.Fun, "windows", "NewProcThreadAttributeList") {
			if len(call.Args) != 1 {
				t.Fatal("invalid attribute allocation")
			}
			literal, ok := call.Args[0].(*ast.BasicLit)
			if !ok || literal.Value != "2" {
				t.Fatal("native creation must allocate both Job and stdio attributes")
			}
			capacity = call.Pos()
		}
		if selector(call.Fun, "attributes", "Update") {
			if len(call.Args) != 3 {
				t.Fatal("invalid native attribute update")
			}
			switch {
			case ident(call.Args[0], "experimentalJobListAttribute"):
				if !attributeStorage(call.Args, "jobs") {
					t.Fatal("Job-list attribute does not point to the owned Job array")
				}
				jobAttribute = call.Pos()
			case selector(call.Args[0], "windows", "PROC_THREAD_ATTRIBUTE_HANDLE_LIST"):
				if !attributeStorage(call.Args, "stdio") {
					t.Fatal("stdio handle-list attribute was discarded or replaced")
				}
				handleAttribute = call.Pos()
			default:
				t.Fatal("unexpected native creation attribute")
			}
		}
		if selector(call.Fun, "windows", "CreateProcess") {
			if create != token.NoPos || len(call.Args) != 10 || !ident(call.Args[4], "true") {
				t.Fatal("native creation must have one call with explicit stdio inheritance")
			}
			startup, ok := call.Args[8].(*ast.UnaryExpr)
			if !ok || startup.Op != token.AND || !selector(startup.X, "si", "StartupInfo") {
				t.Fatal("CreateProcess did not consume the configured STARTUPINFOEX")
			}
			create = call.Pos()
		}
		return true
	})
	if capacity == token.NoPos || jobArray == token.NoPos || jobAttribute == token.NoPos || handleAttribute == token.NoPos ||
		startupList == token.NoPos || create == token.NoPos || capacity >= jobAttribute || capacity >= handleAttribute ||
		jobArray >= jobAttribute || jobAttribute >= startupList || handleAttribute >= startupList || startupList >= create {
		t.Fatal("Job and stdio attributes must both reach CreateProcess before any child exists")
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if value, ok := node.(*ast.SelectorExpr); ok && value.Sel.Name == "AssignProcessToJobObject" {
			t.Fatal("post-creation Job assignment reintroduced an unowned root window")
		}
		return true
	})
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
