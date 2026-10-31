//go:build windows && ao_experimental_persistenthost_job && ao_job_native_fixture

package persistenthost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// SOURCE ONLY until separately compiled, independently reviewed, and explicitly
// run. No provider, shell, chat-host entry point, DB, or installed daemon is used.
// H is the test controller outside every Job. Each serial case owns P -> L -> G
// in one Job capped at three members/128 MiB. S is one independent child of H,
// outside every Job, with no child-spawn branch and a 45-second self deadline.
// There are exactly three tree cases (nine successful tree launches), one S,
// one overflow attempt, and one breakaway attempt; probes are NEVER resumed.
// The breakaway probe has an atomic guard Job and runs BEFORE G exists: even an
// unexpected successful probe keeps the combined live Job membership <= three.
// P now enters its limited Job inside CreateProcessW via JOB_LIST. Its helper
// watchdog is not a remedy for controller death before provider execution.
const (
	nativeJobCommit       = 128 << 20
	nativeJobProbeBytes   = 64 << 10
	nativeJobCaseDeadline = 10 * time.Second
	nativeJobHelperLife   = 20 * time.Second
	nativeJobSiblingLife  = 45 * time.Second
	nativeJobJobList      = experimentalJobListAttribute
)

type nativeJobReport struct {
	Token     string
	Role      string
	Event     string
	PID       uint32
	ChildRole string
	Error     string
	Errno     uint32
	Allocated bool
	Freed     bool
}

type nativeJobHandle struct {
	handle windows.Handle
	kind   string
	close  func() error
}

// This ledger covers only handles returned by this fixture's own constructors.
// It never opens/enumerates a PID tree. Serialized close/terminate guards also
// prevent a delayed callback from acting on a recycled, already-closed handle.
type nativeJobHandles struct {
	mu   sync.Mutex
	live map[windows.Handle]*nativeJobHandle
	adds int
	done int
}

func newNativeJobHandles() *nativeJobHandles {
	return &nativeJobHandles{live: make(map[windows.Handle]*nativeJobHandle)}
}

func (l *nativeJobHandles) add(h windows.Handle, kind string, closeFn func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if h == 0 || h == windows.InvalidHandle || l.live[h] != nil {
		return errors.New("invalid or duplicate fixture handle")
	}
	if closeFn == nil {
		closeFn = func() error { return windows.CloseHandle(h) }
	}
	l.live[h] = &nativeJobHandle{handle: h, kind: kind, close: closeFn}
	l.adds++
	return nil
}

func (l *nativeJobHandles) close(h windows.Handle) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.live[h]
	if entry == nil {
		return errors.New("refusing to close an unowned/already-closed fixture handle")
	}
	if err := entry.close(); err != nil {
		return err
	}
	delete(l.live, h)
	l.done++
	return nil
}

func (l *nativeJobHandles) terminateJob(h windows.Handle, code uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.live[h]
	if entry == nil || entry.kind != "launcher-job" {
		return errors.New("refusing to terminate an unowned/closed Job")
	}
	return windows.TerminateJobObject(h, code)
}

func (l *nativeJobHandles) cleanup(t *testing.T) {
	t.Helper()
	// Emergency cleanup is not an acceptance oracle. All explicit assertions
	// precede it, and any still-live launcher resource makes the test fail.
	l.mu.Lock()
	entries := make([]*nativeJobHandle, 0, len(l.live))
	for _, entry := range l.live {
		entries = append(entries, entry)
	}
	l.mu.Unlock()
	for _, entry := range entries {
		if entry.kind == "launcher-job" {
			t.Error("launcher retained its Job; emergency cleanup cannot qualify Close")
			if err := l.terminateJob(entry.handle, 92); err != nil {
				t.Error(err)
			}
		}
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.kind, "launcher-") && entry.kind != "launcher-job" {
			t.Errorf("launcher leaked %s", entry.kind)
		}
		if err := l.close(entry.handle); err != nil {
			t.Error(err)
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.live) != 0 || l.adds != l.done {
		t.Errorf("fixture handle imbalance: added=%d closed=%d live=%d", l.adds, l.done, len(l.live))
	}
	t.Logf("fixture handles: added=%d closed=%d live=%d", l.adds, l.done, len(l.live))
}

func nativeJobMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func nativeJobStdio(t *testing.T, l *nativeJobHandles) [3]windows.Handle {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	nativeJobMust(t, err)
	h := windows.Handle(f.Fd())
	nativeJobMust(t, l.add(h, "fixture-stdio", f.Close))
	return [3]windows.Handle{h, h, h}
}

func nativeJobEnv(dir, token, role string) []string {
	return []string{
		"SystemRoot=" + os.Getenv("SystemRoot"), "TEMP=" + dir, "TMP=" + dir,
		"GOMAXPROCS=1", "GOMEMLIMIT=16MiB", "GOGC=off", "GOTELEMETRY=off",
		"AO_JOB_NATIVE_CANARY=1", "AO_JOB_NATIVE_DIR=" + dir,
		"AO_JOB_NATIVE_TOKEN=" + token, "AO_JOB_NATIVE_ROLE=" + role,
	}
}

func nativeJobConfig(exe, dir, token, role string, stdio [3]windows.Handle) experimentalJobLaunch {
	return experimentalJobLaunch{
		Enabled: true, Owner: experimentalJobOwner{SessionID: "native-canary-" + token, OwnerGeneration: 1, RuntimeGeneration: 1},
		Executable: exe, Args: []string{"-test.run=^TestExperimentalWindowsJobNativeHelper$", "-test.count=1", "-test.timeout=60s"},
		Workdir: dir, Environment: nativeJobEnv(dir, token, role), Stdio: stdio,
		ProcessLimit: 3, CommitBytes: nativeJobCommit,
	}
}

func nativeJobPath(dir, role, event, suffix string) string {
	return filepath.Join(dir, role+"-"+event+suffix)
}

func nativeJobCommand(t *testing.T, dir, token, role, event string) {
	t.Helper()
	name := nativeJobPath(dir, role, event, ".cmd")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	nativeJobMust(t, err)
	_, writeErr := f.WriteString(token)
	nativeJobMust(t, errors.Join(writeErr, f.Close()))
}

func nativeJobRead(t *testing.T, ctx context.Context, dir, token, role, event string) nativeJobReport {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		name := nativeJobPath(dir, role, event, ".json")
		if info, err := os.Stat(name); err == nil {
			if info.Size() > 2048 {
				t.Fatal("oversized native helper report")
			}
			data, err := os.ReadFile(name)
			nativeJobMust(t, err)
			var report nativeJobReport
			nativeJobMust(t, json.Unmarshal(data, &report))
			if report.Token != token || report.Role != role || report.Event != event || report.Error != "" {
				t.Fatalf("invalid native report: %+v", report)
			}
			t.Logf("native report: %+v", report)
			return report
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("native helper deadline: role=%s event=%s context=%v", role, event, ctx.Err())
	return nativeJobReport{}
}

func nativeJobAlive(t *testing.T, h windows.Handle) {
	t.Helper()
	event, err := windows.WaitForSingleObject(h, 0)
	nativeJobMust(t, err)
	if event != uint32(windows.WAIT_TIMEOUT) {
		t.Fatalf("expected live fixture process; wait result=%d", event)
	}
}

func nativeJobExited(t *testing.T, h windows.Handle, want uint32) {
	t.Helper()
	event, err := windows.WaitForSingleObject(h, 3000)
	nativeJobMust(t, err)
	if event != windows.WAIT_OBJECT_0 {
		t.Fatal("owned process did not terminate within 3 seconds")
	}
	var code uint32
	nativeJobMust(t, windows.GetExitCodeProcess(h, &code))
	if code != want {
		t.Fatalf("native exit=%d want=%d; watchdog/helper failure is not containment evidence", code, want)
	}
}

func nativeJobWitness(t *testing.T, l *nativeJobHandles, api experimentalJobCalls, job windows.Handle, pid uint32) windows.Handle {
	t.Helper()
	// Only a PID reported by our fixed helper protocol is opened, read-only.
	// Membership in this exact owned Job is mandatory before it is a witness.
	h, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	nativeJobMust(t, err)
	nativeJobMust(t, l.add(h, "fixture-witness", nil))
	member, err := api.inJob(h, job)
	nativeJobMust(t, err)
	if !member {
		t.Fatal("reported process is not a member of this fixture Job")
	}
	nativeJobAlive(t, h)
	return h
}

func nativeJobLimits(t *testing.T, job windows.Handle) windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
	t.Helper()
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	nativeJobMust(t, windows.QueryInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil))
	return info
}

func nativeJobSetLimits(t *testing.T, job windows.Handle, info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) {
	t.Helper()
	_, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	nativeJobMust(t, err)
}

func nativeJobCount(t *testing.T, job windows.Handle, want uint32) {
	t.Helper()
	// Win32 JOBOBJECT_BASIC_ACCOUNTING_INFORMATION; no PID enumeration.
	var info struct {
		TotalUserTime, TotalKernelTime, ThisPeriodTotalUserTime, ThisPeriodTotalKernelTime int64
		TotalPageFaultCount, TotalProcesses, ActiveProcesses, TotalTerminatedProcesses     uint32
	}
	nativeJobMust(t, windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil))
	if info.ActiveProcesses != want {
		t.Fatalf("Job active processes=%d want=%d", info.ActiveProcesses, want)
	}
}

func nativeJobTrackedCalls(t *testing.T, l *nativeJobHandles, raw experimentalJobCalls) (experimentalJobCalls, *windows.Handle, *bool, *bool) {
	t.Helper()
	api := raw
	var job, process windows.Handle
	atCreate := false
	verified := false
	api.createJob = func() (windows.Handle, error) {
		h, err := raw.createJob()
		if err == nil {
			err = l.add(h, "launcher-job", nil)
			job = h
		}
		return h, err
	}
	api.duplicate = func(h windows.Handle) (windows.Handle, error) {
		dup, err := raw.duplicate(h)
		if err == nil {
			err = l.add(dup, "launcher-stdio", nil)
		}
		return dup, err
	}
	api.create = func(cfg experimentalJobLaunch, stdio [3]windows.Handle, ownedJob windows.Handle) (windows.ProcessInformation, error) {
		if ownedJob == 0 || ownedJob != job {
			return windows.ProcessInformation{}, errors.New("creation omitted the fixture's limited Job")
		}
		pi, err := raw.create(cfg, stdio, ownedJob)
		if err == nil {
			err = errors.Join(l.add(pi.Process, "launcher-process", nil), l.add(pi.Thread, "launcher-thread", nil))
			process = pi.Process
			member, memberErr := raw.inJob(process, ownedJob)
			parent, parentErr := raw.inJob(windows.CurrentProcess(), ownedJob)
			if memberErr != nil || parentErr != nil || !member || parent {
				err = errors.Join(err, memberErr, parentErr, errors.New("native creation returned without exclusive fixture Job ownership"))
			} else {
				atCreate = true
			}
		}
		return pi, err
	}
	api.resume = func(thread windows.Handle) (uint32, error) {
		member, err := raw.inJob(process, job)
		parent, parentErr := raw.inJob(windows.CurrentProcess(), job)
		if err != nil || parentErr != nil || !member || parent || !atCreate {
			return 0, errors.New("pre-resume root membership/parent exclusion failed")
		}
		verified = true
		return raw.resume(thread)
	}
	api.close = l.close
	api.terminateJob = func(h windows.Handle) error { return l.terminateJob(h, 1) }
	return api, &job, &atCreate, &verified
}

func nativeJobMemory(t *testing.T, ctx context.Context, dir, token string, job windows.Handle) {
	t.Helper()
	limits := nativeJobLimits(t, job)
	probe := func(event string, want bool) {
		nativeJobCommand(t, dir, token, "P", event)
		r := nativeJobRead(t, ctx, dir, token, "P", event)
		if r.Allocated != want || want && (!r.Freed || r.Errno != 0) || !want &&
			(r.Errno != uint32(windows.ERROR_NOT_ENOUGH_QUOTA) && r.Errno != uint32(windows.ERROR_COMMITMENT_LIMIT) && r.Errno != uint32(windows.ERROR_NOT_ENOUGH_MEMORY)) {
			t.Fatalf("64 KiB commit control failed: %+v", r)
		}
	}
	probe("probe-before", true)
	func() {
		// Change only this disposable Job. Never fill 128 MiB to test its cap.
		// Failure to support a below-current-use limit is a failing environment
		// contract, not a skipped assertion or permission to allocate more.
		reduced := limits
		reduced.JobMemoryLimit = 1
		nativeJobSetLimits(t, job, reduced)
		defer nativeJobSetLimits(t, job, limits)
		probe("probe-denied", false)
	}()
	probe("probe-after", true)
}

func TestExperimentalWindowsJobNativeAcceptance(t *testing.T) {
	if os.Getenv("AO_JOB_NATIVE_CANARY") != "1" {
		t.Skip("requires explicit AO_JOB_NATIVE_CANARY=1 and both experimental build tags")
	}
	if os.Getenv("AO_JOB_NATIVE_ROLE") != "" {
		t.Fatal("controller cannot run in helper mode")
	}
	home, err := os.UserHomeDir()
	nativeJobMust(t, err)
	scratch := os.Getenv("AO_JOB_NATIVE_SCRATCH")
	rel, err := filepath.Rel(filepath.Join(home, ".ao", "scratch"), scratch)
	if err != nil || !filepath.IsAbs(scratch) || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		t.Fatal("SOURCE_CONTRACT_HOLD: AO_JOB_NATIVE_SCRATCH must be a dedicated directory under ~/.ao/scratch")
	}
	t.Setenv("TMP", scratch)
	t.Setenv("TEMP", scratch)
	root := t.TempDir()
	exe, err := os.Executable()
	nativeJobMust(t, err)
	raw, err := nativeExperimentalJobCalls()
	nativeJobMust(t, err)
	inJob, err := raw.inJob(windows.CurrentProcess(), 0)
	nativeJobMust(t, err)
	if inJob {
		t.Fatal("ENVIRONMENT_BLOCKED: controller must be outside every Job")
	}
	beforeWD, err := os.Getwd()
	nativeJobMust(t, err)
	beforeSession, beforeData := os.Getenv("AO_SESSION_ID"), os.Getenv("AO_DATA_DIR")
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate reviewed source")
	}
	snapshots := make(map[string][32]byte)
	for _, name := range []string{"host.go", "child_windows.go", "child_unix.go", "child_windows_job.go"} {
		p := filepath.Join(filepath.Dir(source), name)
		data, err := os.ReadFile(p)
		nativeJobMust(t, err)
		snapshots[p] = sha256.Sum256(data)
	}
	var nonce [16]byte
	_, err = rand.Read(nonce[:])
	nativeJobMust(t, err)
	token := hex.EncodeToString(nonce[:])
	siblingDir := filepath.Join(root, "sibling")
	nativeJobMust(t, os.Mkdir(siblingDir, 0o700))
	siblingLedger := newNativeJobHandles()
	t.Cleanup(func() { siblingLedger.cleanup(t) })
	siblingCfg := nativeJobConfig(exe, siblingDir, token, "S", nativeJobStdio(t, siblingLedger))
	var siblingStdio [3]windows.Handle
	for i, h := range siblingCfg.Stdio {
		siblingStdio[i], err = raw.duplicate(h)
		nativeJobMust(t, err)
		nativeJobMust(t, siblingLedger.add(siblingStdio[i], "fixture-sibling-stdio", nil))
	}
	// S starts directly, with no suspended orphan window. Its fixed helper
	// branch starts its own deadline and cannot spawn children. It is never
	// killed by H, including constructor/readiness failures and test timeouts.
	sibling, err := nativeJobFixtureCreate(siblingCfg, siblingStdio, false)
	nativeJobMust(t, err)
	nativeJobMust(t, siblingLedger.add(sibling.Process, "fixture-sibling", nil))
	nativeJobMust(t, siblingLedger.add(sibling.Thread, "fixture-sibling-thread", nil))
	for _, h := range siblingStdio {
		nativeJobMust(t, siblingLedger.close(h))
	}
	nativeJobMust(t, siblingLedger.close(sibling.Thread))
	t.Cleanup(func() {
		if err := os.WriteFile(nativeJobPath(siblingDir, "S", "quit", ".cmd"), []byte(token), 0o600); err != nil {
			t.Error(err)
		}
		// A failing cooperative exit is still bounded by S's own 45-second
		// watchdog. Code 93 fails; a watchdog exit never qualifies the canary.
		event, err := windows.WaitForSingleObject(sibling.Process, 46000)
		if err != nil || event != windows.WAIT_OBJECT_0 {
			t.Errorf("independent sibling exit wait=%d error=%v", event, err)
			return
		}
		var code uint32
		if err := windows.GetExitCodeProcess(sibling.Process, &code); err != nil || code != 0 {
			t.Errorf("independent sibling exit=%d error=%v", code, err)
		}
	})
	siblingMember, err := raw.inJob(sibling.Process, 0)
	nativeJobMust(t, err)
	if siblingMember {
		t.Fatal("independent sibling unexpectedly belongs to a Job")
	}
	siblingCtx, siblingCancel := context.WithTimeout(context.Background(), nativeJobSiblingLife+time.Second)
	t.Cleanup(siblingCancel)
	_ = nativeJobRead(t, siblingCtx, siblingDir, token, "S", "ready")
	for index, action := range []string{"cancel", "close", "wait"} {
		if !t.Run(action, func(t *testing.T) {
			dir := filepath.Join(root, action)
			nativeJobMust(t, os.Mkdir(dir, 0o700))
			ledger := newNativeJobHandles()
			t.Cleanup(func() { ledger.cleanup(t) })
			ctx, cancel := context.WithTimeout(context.Background(), nativeJobCaseDeadline)
			t.Cleanup(cancel)
			launchCtx, cancelLaunch := context.WithCancel(ctx)
			api, job, atCreate, preResume := nativeJobTrackedCalls(t, ledger, raw)
			cfg := nativeJobConfig(exe, dir, token, "P", nativeJobStdio(t, ledger))
			p, err := launchExperimentalWindowsJobWithCalls(launchCtx, cfg, api)
			nativeJobMust(t, err)
			t.Cleanup(func() { cancelLaunch(); _ = p.Close() })
			if !*atCreate || !*preResume || p.owner != cfg.Owner {
				t.Fatal("creation-time membership, pre-resume membership, or positive owner identity missing")
			}
			limits := nativeJobLimits(t, *job)
			flags := uint32(windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
			if limits.BasicLimitInformation.LimitFlags != flags || limits.BasicLimitInformation.ActiveProcessLimit != 3 || limits.JobMemoryLimit != nativeJobCommit {
				t.Fatal("native limits differ from the launcher contract")
			}
			rootReport := nativeJobRead(t, ctx, dir, token, "P", "ready")
			witnesses := []windows.Handle{nativeJobWitness(t, ledger, raw, *job, rootReport.PID)}
			seenPIDs := map[uint32]bool{rootReport.PID: true}
			if index == 0 {
				nativeJobMemory(t, ctx, dir, token, *job)
			}
			for _, roles := range [][2]string{{"P", "L"}, {"L", "G"}} {
				nativeJobCommand(t, dir, token, roles[0], "create")
				r := nativeJobRead(t, ctx, dir, token, roles[0], "create")
				if r.ChildRole != roles[1] || seenPIDs[r.PID] {
					t.Fatal("unexpected child role or duplicate process identity")
				}
				seenPIDs[r.PID] = true
				witnesses = append(witnesses, nativeJobWitness(t, ledger, raw, *job, r.PID))
				// Membership was asserted while this child is still suspended.
				nativeJobCommand(t, dir, token, roles[0], "resume")
				_ = nativeJobRead(t, ctx, dir, token, roles[0], "resume")
				ready := nativeJobRead(t, ctx, dir, token, roles[1], "ready")
				if ready.PID != r.PID {
					t.Fatal("created child and ready helper identities differ")
				}
				if index == 0 && roles[1] == "L" {
					nativeJobCount(t, *job, 2) // Spare slot: breakaway denial cannot be overflow.
					nativeJobCommand(t, dir, token, "P", "breakaway")
					_ = nativeJobRead(t, ctx, dir, token, "P", "breakaway")
				}
			}
			nativeJobCount(t, *job, 3)
			if index == 0 {
				nativeJobCommand(t, dir, token, "G", "overflow")
				_ = nativeJobRead(t, ctx, dir, token, "G", "overflow")
				nativeJobCount(t, *job, 3)
			}
			for _, h := range witnesses {
				nativeJobAlive(t, h)
			}
			siblingInJob, err := raw.inJob(sibling.Process, *job)
			nativeJobMust(t, err)
			if siblingInJob {
				t.Fatal("sibling entered the target Job")
			}
			nativeJobAlive(t, sibling.Process)
			nativeJobMust(t, ctx.Err())
			rootExit := uint32(1)
			switch action {
			case "cancel":
				cancelLaunch()
				if _, err := p.Wait(launchCtx); !errors.Is(err, context.Canceled) {
					t.Fatalf("Wait cancellation=%v", err)
				}
				nativeJobMust(t, p.Close())
			case "close":
				nativeJobMust(t, p.Close()) // Explicit Close oracle, before emergency cleanup.
				code, err := p.Wait(ctx)
				nativeJobMust(t, err)
				if code != 1 {
					t.Fatal("Close did not return its native termination code")
				}
			case "wait":
				rootExit = 0
				nativeJobCommand(t, dir, token, "P", "exit")
				nativeJobExited(t, witnesses[0], 0)
				nativeJobAlive(t, witnesses[1])
				nativeJobAlive(t, witnesses[2])
				code, err := p.Wait(ctx)
				nativeJobMust(t, err)
				if code != 0 {
					t.Fatal("Wait lost the root's successful exit")
				}
				nativeJobMust(t, p.Close())
			}
			nativeJobMust(t, ctx.Err())
			for i, h := range witnesses {
				want := uint32(1)
				if i == 0 {
					want = rootExit
				}
				nativeJobExited(t, h, want)
			}
			nativeJobAlive(t, sibling.Process)
		}) {
			return // Do not start another tree after a failed native cleanup gate.
		}
	}
	for p, before := range snapshots {
		data, err := os.ReadFile(p)
		nativeJobMust(t, err)
		if sha256.Sum256(data) != before {
			t.Fatal("reviewed original worktree source changed")
		}
	}
	afterWD, err := os.Getwd()
	nativeJobMust(t, err)
	if beforeWD != afterWD || beforeSession != os.Getenv("AO_SESSION_ID") || beforeData != os.Getenv("AO_DATA_DIR") {
		t.Fatal("original controller workdir/session environment changed")
	}
}

func nativeJobEmit(dir string, report nativeJobReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	name := nativeJobPath(dir, report.Role, report.Event, ".json")
	if err := os.WriteFile(name+".tmp", data, 0o600); err != nil {
		return err
	}
	return os.Rename(name+".tmp", name)
}

func nativeJobErrno(err error) uint32 {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return uint32(errno)
	}
	return 0
}

func TestExperimentalWindowsJobNativeHelper(t *testing.T) {
	role := os.Getenv("AO_JOB_NATIVE_ROLE")
	if role == "" {
		return // Ordinary test discovery never launches a helper.
	}
	if os.Getenv("AO_JOB_NATIVE_CANARY") != "1" || (role != "P" && role != "L" && role != "G" && role != "S") {
		t.Fatal("invalid native helper invocation")
	}
	os.Exit(nativeJobHelper(role))
}

func nativeJobHelper(role string) (code int) {
	dir, token := os.Getenv("AO_JOB_NATIVE_DIR"), os.Getenv("AO_JOB_NATIVE_TOKEN")
	wd, err := os.Getwd()
	if err != nil || !filepath.IsAbs(dir) || wd != dir || len(token) != 32 || os.Getenv("AO_SESSION_ID") != "" || os.Getenv("AO_DATA_DIR") != "" {
		return 91
	}
	life := nativeJobHelperLife
	if role == "S" {
		life = nativeJobSiblingLife
	}
	watchdog := time.AfterFunc(life, func() { os.Exit(93) })
	defer watchdog.Stop()
	raw, err := nativeExperimentalJobCalls()
	if err != nil {
		return 91
	}
	inJob, err := raw.inJob(windows.CurrentProcess(), 0)
	if err != nil || (role == "S") == inJob {
		return 91
	}
	emit := func(event string, err error) nativeJobReport {
		r := nativeJobReport{Token: token, Role: role, Event: event, PID: uint32(os.Getpid())}
		if err != nil {
			r.Error, r.Errno = err.Error(), nativeJobErrno(err)
		}
		return r
	}
	if nativeJobEmit(dir, emit("ready", nil)) != nil {
		return 91
	}
	exe, err := os.Executable()
	if err != nil {
		return 91
	}
	var child windows.ProcessInformation
	resumed := false
	defer func() {
		if child.Thread != 0 {
			if windows.CloseHandle(child.Thread) != nil {
				code = 91
			}
		}
		if child.Process != 0 {
			if windows.CloseHandle(child.Process) != nil {
				code = 91
			}
		}
	}()
	seen := make(map[string]bool)
	deadline := time.Now().Add(life)
	for time.Now().Before(deadline) {
		for _, op := range []string{"probe-before", "probe-denied", "probe-after", "create", "resume", "breakaway", "overflow", "exit", "quit"} {
			if seen[op] {
				continue
			}
			data, err := os.ReadFile(nativeJobPath(dir, role, op, ".cmd"))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil || string(data) != token {
				return 91
			}
			seen[op] = true // Each fixed command can run at most once.
			r := emit(op, nil)
			switch op {
			case "probe-before", "probe-denied", "probe-after":
				if role != "P" {
					return 91
				}
				address, allocErr := windows.VirtualAlloc(0, nativeJobProbeBytes, windows.MEM_RESERVE|windows.MEM_COMMIT, windows.PAGE_READWRITE)
				r.Allocated, r.Errno = address != 0, nativeJobErrno(allocErr)
				if address != 0 {
					freeErr := windows.VirtualFree(address, 0, windows.MEM_RELEASE)
					r.Freed = freeErr == nil
					if freeErr != nil {
						r.Error = freeErr.Error()
					}
				}
			case "create":
				next := map[string]string{"P": "L", "L": "G"}[role]
				if next == "" || child.Process != 0 {
					return 91
				}
				child, err = nativeJobHelperCreate(raw, nativeJobConfig(exe, dir, token, next, [3]windows.Handle{}))
				r = emit(op, err)
				r.PID, r.ChildRole = child.ProcessId, next
			case "resume":
				if child.Thread == 0 || resumed {
					return 91
				}
				previous, resumeErr := raw.resume(child.Thread)
				if resumeErr != nil || previous != 1 {
					return 91
				}
				if windows.CloseHandle(child.Thread) != nil {
					return 91
				}
				child.Thread, resumed = 0, true
			case "breakaway":
				if role != "P" || !resumed {
					return 91
				}
				r = emit(op, nativeJobBreakaway(raw, nativeJobConfig(exe, dir, token, "PROBE", [3]windows.Handle{})))
			case "overflow":
				if role != "G" {
					return 91
				}
				probe, createErr := nativeJobHelperCreate(raw, nativeJobConfig(exe, dir, token, "PROBE", [3]windows.Handle{}))
				if createErr == nil {
					// Never resume an unexpectedly created fourth process. It is
					// inherited into H's Job and emergency Job cleanup owns it.
					_ = windows.CloseHandle(probe.Thread)
					_ = windows.CloseHandle(probe.Process)
					r.Error = "fourth process unexpectedly created"
				} else if !errors.Is(createErr, windows.ERROR_NOT_ENOUGH_QUOTA) && !errors.Is(createErr, windows.ERROR_ACCESS_DENIED) {
					r = emit(op, createErr)
				} else {
					r.Errno = nativeJobErrno(createErr)
				}
			case "exit":
				if role != "P" {
					return 91
				}
				return 0 // L and G keep running; no stdin/parent-exit dependency.
			case "quit":
				if role != "S" {
					return 91
				}
				return 0
			}
			if nativeJobEmit(dir, r) != nil {
				return 91
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 93
}

func nativeJobHelperCreate(raw experimentalJobCalls, cfg experimentalJobLaunch) (pi windows.ProcessInformation, result error) {
	var stdio [3]windows.Handle
	defer func() {
		for _, h := range stdio {
			if h != 0 {
				result = errors.Join(result, windows.CloseHandle(h))
			}
		}
	}()
	for i, h := range [3]windows.Handle{windows.Handle(os.Stdin.Fd()), windows.Handle(os.Stdout.Fd()), windows.Handle(os.Stderr.Fd())} {
		dup, err := raw.duplicate(h)
		if err != nil {
			return windows.ProcessInformation{}, err
		}
		stdio[i] = dup
	}
	// P/L/G already belong to H's exact Job (checked by H before each create
	// command). Win32 inherits that membership at child creation with no
	// breakaway flag; no Job handle is inherited or post-create assignment used.
	return nativeJobFixtureCreate(cfg, stdio, true)
}

func nativeJobFixtureCreate(cfg experimentalJobLaunch, stdio [3]windows.Handle, inheritJob bool) (windows.ProcessInformation, error) {
	var pi windows.ProcessInformation
	raw, err := nativeExperimentalJobCalls()
	if err != nil {
		return pi, experimentalJobBlocked("fixture native APIs", err)
	}
	member, err := raw.inJob(windows.CurrentProcess(), 0)
	if err != nil || member != inheritJob {
		return pi, experimentalJobBlocked("fixture parent Job ownership", errors.Join(err, errors.New("unexpected inherited/unowned fixture parent")))
	}
	app, err := windows.UTF16PtrFromString(cfg.Executable)
	if err != nil {
		return pi, err
	}
	command, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{cfg.Executable}, cfg.Args...)))
	if err != nil {
		return pi, err
	}
	dir, err := windows.UTF16PtrFromString(cfg.Workdir)
	if err != nil {
		return pi, err
	}
	env, err := experimentalJobEnvironment(cfg.Environment)
	if err != nil {
		return pi, err
	}
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return pi, err
	}
	defer attributes.Delete()
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&stdio[0]), unsafe.Sizeof(stdio)); err != nil {
		return pi, err
	}
	si := windows.StartupInfoEx{}
	si.Cb = uint32(unsafe.Sizeof(si))
	si.Flags = windows.STARTF_USESTDHANDLES
	si.StdInput, si.StdOutput, si.StdErr = stdio[0], stdio[1], stdio[2]
	si.ProcThreadAttributeList = attributes.List()
	flags := uint32(windows.CREATE_NO_WINDOW | windows.CREATE_UNICODE_ENVIRONMENT | windows.EXTENDED_STARTUPINFO_PRESENT)
	if inheritJob {
		flags |= windows.CREATE_SUSPENDED // Membership is inherited at birth, not at resume.
	}
	err = windows.CreateProcess(app, &command[0], nil, nil, true, flags, &env[0], dir, &si.StartupInfo, &pi)
	runtime.KeepAlive(stdio)
	runtime.KeepAlive(attributes)
	return pi, err
}

func nativeJobBreakaway(raw experimentalJobCalls, cfg experimentalJobLaunch) (result error) {
	// This guard contains an unexpectedly permitted breakaway AT CREATION.
	// It is non-inherited and kill-on-close, so P's own deadline/Job death also
	// destroys the guard. The probe is never resumed, even on test failure.
	guard, err := raw.createJob()
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, windows.TerminateJobObject(guard, 92), windows.CloseHandle(guard))
	}()
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{JobMemoryLimit: nativeJobCommit}
	limits.BasicLimitInformation.ActiveProcessLimit = 1
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if err := raw.setLimits(guard, limits); err != nil {
		return err
	}
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return err
	}
	defer attributes.Delete()
	if err := attributes.Update(nativeJobJobList, unsafe.Pointer(&guard), unsafe.Sizeof(guard)); err != nil {
		return err // Unsupported Job-list APIs fail the fixture; never skip/fallback.
	}
	app, err := windows.UTF16PtrFromString(cfg.Executable)
	if err != nil {
		return err
	}
	command, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{cfg.Executable}, cfg.Args...)))
	if err != nil {
		return err
	}
	dir, err := windows.UTF16PtrFromString(cfg.Workdir)
	if err != nil {
		return err
	}
	env, err := experimentalJobEnvironment(cfg.Environment)
	if err != nil {
		return err
	}
	si := windows.StartupInfoEx{}
	si.Cb = uint32(unsafe.Sizeof(si))
	si.ProcThreadAttributeList = attributes.List()
	var pi windows.ProcessInformation
	flags := uint32(windows.CREATE_SUSPENDED | windows.CREATE_NO_WINDOW | windows.CREATE_UNICODE_ENVIRONMENT |
		windows.EXTENDED_STARTUPINFO_PRESENT | windows.CREATE_BREAKAWAY_FROM_JOB)
	err = windows.CreateProcess(app, &command[0], nil, nil, false, flags, &env[0], dir, &si.StartupInfo, &pi)
	runtime.KeepAlive(guard)
	runtime.KeepAlive(attributes)
	if err == nil {
		stopErr := windows.TerminateJobObject(guard, 92)
		event, waitErr := windows.WaitForSingleObject(pi.Process, 3000)
		closeErr := errors.Join(windows.CloseHandle(pi.Thread), windows.CloseHandle(pi.Process))
		return errors.Join(errors.New("breakaway unexpectedly succeeded"), stopErr, waitErr, closeErr, fmt.Errorf("probe wait=%d", event))
	}
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return fmt.Errorf("breakaway denied for an unexpected reason: %w", err)
	}
	return nil
}
