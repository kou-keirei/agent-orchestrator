//go:build windows && ao_experimental_persistenthost_job

package persistenthost

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// PROC_THREAD_ATTRIBUTE_JOB_LIST (WinSDK JobList=13, input=true). x/sys v0.45.0
// exposes UpdateProcThreadAttribute but not this Windows 10+ attribute constant.
// It assigns the limited Job as part of CreateProcessW, not in a later step.
const experimentalJobListAttribute = 0x0002000d

// This experiment requires an explicit build tag and Enabled=true. It is
// deliberately not called by ConnectOrStart or Run. It does
// not support command shims, provider discovery, host reconnection, or brokers.
// A future integration must map real AO generations into this explicit owner;
// these proof-local positive counters are not receipts for an existing host.
type experimentalJobOwner struct {
	SessionID         string
	OwnerGeneration   uint64
	RuntimeGeneration uint64
}

type experimentalJobLaunch struct {
	Enabled      bool
	Owner        experimentalJobOwner
	Executable   string
	Args         []string
	Workdir      string
	Environment  []string // Explicit replacement; nil (inheritance) is unsupported.
	Stdio        [3]windows.Handle
	ProcessLimit uint32
	CommitBytes  uintptr
}

// experimentalJobHold is intentionally local to the experiment. Default chat
// launch errors and daemon/CLI/HTTP behavior are unchanged.
type experimentalJobHold struct {
	Code  string
	Stage string
	Cause error
}

func (e *experimentalJobHold) Error() string {
	return fmt.Sprintf("%s: experimental Windows Job %s: %v", e.Code, e.Stage, e.Cause)
}

func (e *experimentalJobHold) Unwrap() error { return e.Cause }

func experimentalJobBlocked(stage string, err error) error {
	return &experimentalJobHold{Code: "ENVIRONMENT_BLOCKED", Stage: stage, Cause: err}
}

func validateExperimentalJobLaunch(cfg experimentalJobLaunch) error {
	if !cfg.Enabled || strings.TrimSpace(cfg.Owner.SessionID) == "" ||
		cfg.Owner.OwnerGeneration == 0 || cfg.Owner.RuntimeGeneration == 0 {
		return &experimentalJobHold{Code: "SOURCE_CONTRACT_HOLD", Stage: "opt-in/owner", Cause: errors.New("explicit opt-in and positive owner/runtime generations required")}
	}
	// Proof envelope only, not a production provider policy. No breakaway flags
	// or caller-supplied process creation flags are accepted by this API.
	if cfg.ProcessLimit == 0 || cfg.ProcessLimit > 3 || cfg.CommitBytes == 0 || cfg.CommitBytes > 1<<30 {
		return &experimentalJobHold{Code: "SOURCE_CONTRACT_HOLD", Stage: "limits", Cause: errors.New("require 1..3 Job processes and 1..1 GiB Job commit bytes")}
	}
	if !filepath.IsAbs(cfg.Executable) || !strings.EqualFold(filepath.Ext(cfg.Executable), ".exe") ||
		strings.ContainsAny(cfg.Executable, "\x00\"") || !filepath.IsAbs(cfg.Workdir) || strings.ContainsRune(cfg.Workdir, 0) || cfg.Environment == nil {
		return &experimentalJobHold{Code: "SOURCE_CONTRACT_HOLD", Stage: "invocation", Cause: errors.New("absolute native .exe, absolute workdir, and explicit environment required")}
	}
	for _, handle := range cfg.Stdio {
		if handle == 0 || handle == windows.InvalidHandle {
			return &experimentalJobHold{Code: "SOURCE_CONTRACT_HOLD", Stage: "stdio", Cause: errors.New("three live caller-owned stdio handles required")}
		}
	}
	return nil
}

// Function injection is per invocation, only for source regression fixtures;
// there is no global launcher override or fallback to the default start path.
// create must atomically place the child in its supplied, already-limited Job;
// an error must not return a live process outside that Job.
type experimentalJobCalls struct {
	inJob        func(windows.Handle, windows.Handle) (bool, error)
	createJob    func() (windows.Handle, error)
	setLimits    func(windows.Handle, windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error
	duplicate    func(windows.Handle) (windows.Handle, error)
	create       func(experimentalJobLaunch, [3]windows.Handle, windows.Handle) (windows.ProcessInformation, error)
	resume       func(windows.Handle) (uint32, error)
	terminateJob func(windows.Handle) error
	wait         func(windows.Handle, uint32) (uint32, error)
	exitCode     func(windows.Handle, *uint32) error
	close        func(windows.Handle) error
}

// launchExperimentalWindowsJob requires caller-owned stdio endpoints to remain
// valid until it returns. It duplicates only these endpoints; it never closes
// the originals. The returned object owns only its Job/process handles. No
// console is created or inherited. Callers own pumping/draining their pipes.
func launchExperimentalWindowsJob(ctx context.Context, cfg experimentalJobLaunch) (*experimentalJobProcess, error) {
	if err := validateExperimentalJobLaunch(cfg); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	api, err := nativeExperimentalJobCalls()
	if err != nil {
		return nil, experimentalJobBlocked("native API availability", err)
	}
	return launchExperimentalWindowsJobWithCalls(ctx, cfg, api)
}

func launchExperimentalWindowsJobWithCalls(ctx context.Context, cfg experimentalJobLaunch, api experimentalJobCalls) (result *experimentalJobProcess, err error) {
	if err := validateExperimentalJobLaunch(cfg); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	foreign, err := api.inJob(windows.CurrentProcess(), 0)
	if err != nil || foreign {
		return nil, experimentalJobBlocked("parent Job ownership", errors.Join(err, errors.New("parent must be outside every Job")))
	}
	p := &experimentalJobProcess{api: api, owner: cfg.Owner}
	var stdio [3]windows.Handle
	var thread windows.Handle
	defer func() {
		var cleanupErr error
		for _, handle := range stdio {
			if handle != 0 {
				cleanupErr = errors.Join(cleanupErr, api.close(handle))
			}
		}
		if thread != 0 {
			cleanupErr = errors.Join(cleanupErr, api.close(thread))
		}
		if cleanupErr != nil {
			err = errors.Join(err, experimentalJobBlocked("launch handle cleanup", cleanupErr))
		}
		if err != nil {
			if closeErr := p.Close(); closeErr != nil {
				err = errors.Join(err, experimentalJobBlocked("abort cleanup", closeErr))
			}
			result = nil
		}
	}()
	p.job, err = api.createJob()
	if err != nil {
		return nil, experimentalJobBlocked("create Job", err)
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{JobMemoryLimit: cfg.CommitBytes}
	limits.BasicLimitInformation.ActiveProcessLimit = cfg.ProcessLimit
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS |
		windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if err := api.setLimits(p.job, limits); err != nil {
		return nil, experimentalJobBlocked("configure Job", err)
	}
	for i, handle := range cfg.Stdio {
		stdio[i], err = api.duplicate(handle)
		if err != nil {
			return nil, experimentalJobBlocked("duplicate stdio", err)
		}
	}
	// The creation API accepts only the already-created/limited Job. A
	// successful return guarantees membership at birth, even if this host dies
	// before the next instruction. The Job handle is never inherited by P.
	pi, err := api.create(cfg, stdio, p.job)
	if err != nil {
		return nil, experimentalJobBlocked("create Job-owned provider", err)
	}
	p.process, thread = pi.Process, pi.Thread
	member, err := api.inJob(p.process, p.job)
	if err != nil || !member {
		return nil, experimentalJobBlocked("verify creation-time membership", errors.Join(err, errors.New("membership not established by native creation")))
	}
	// Inheritance has completed. Drop our child-side stdio duplicates before
	// any resume or cancellation callback can run; caller endpoints stay open.
	for i, handle := range stdio {
		if err := api.close(handle); err != nil {
			return nil, experimentalJobBlocked("close inherited stdio duplicate", err)
		}
		stdio[i] = 0
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count, err := api.resume(thread)
	if err != nil || count != 1 {
		return nil, experimentalJobBlocked("resume provider", errors.Join(err, fmt.Errorf("previous suspend count = %d; require 1", count)))
	}
	if err := api.close(thread); err != nil {
		return nil, experimentalJobBlocked("close primary thread", err)
	}
	thread = 0
	// All temporary handles have been closed before the callback may compete
	// with Close/Wait. A canceled startup is still cleaned up through our Job.
	p.mu.Lock()
	p.stopCancel = context.AfterFunc(ctx, func() { _ = p.Close() })
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Unlock()
	return p, nil
}

type experimentalJobProcess struct {
	mu         sync.Mutex
	api        experimentalJobCalls
	owner      experimentalJobOwner
	job        windows.Handle
	process    windows.Handle
	stopCancel func() bool
	closed     bool
	exit       uint32
	closeErr   error
}

// Close is idempotent and handle-bound. Closing the non-inherited Job is also
// the kill-on-close fallback if explicit termination fails. Never target a PID.
func (p *experimentalJobProcess) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeLocked()
}

func (p *experimentalJobProcess) closeLocked() error {
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	if p.stopCancel != nil {
		p.stopCancel()
	}
	var errs []error
	if p.job != 0 {
		if err := p.api.terminateJob(p.job); err != nil {
			errs = append(errs, err)
		}
	}
	if p.job != 0 {
		errs = append(errs, p.api.close(p.job))
	}
	if p.process != 0 {
		event, err := p.api.wait(p.process, 5000)
		errs = append(errs, err)
		if err == nil && event == windows.WAIT_OBJECT_0 {
			errs = append(errs, p.api.exitCode(p.process, &p.exit))
		} else if err == nil {
			errs = append(errs, errors.New("owned provider termination wait timed out"))
		}
		errs = append(errs, p.api.close(p.process))
	}
	p.closeErr = errors.Join(errs...)
	return p.closeErr
}

// Wait returns a native exit code, then closes this Job to reap any descendants
// that outlived the root. Its context cancellation terminates only this launch.
func (p *experimentalJobProcess) Wait(ctx context.Context) (uint32, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(err, p.Close())
		}
		p.mu.Lock()
		if p.closed {
			code, err := p.exit, p.closeErr
			p.mu.Unlock()
			return code, err
		}
		event, err := p.api.wait(p.process, 50)
		if err == nil && event == windows.WAIT_OBJECT_0 {
			err = p.closeLocked()
			code := p.exit
			p.mu.Unlock()
			return code, err
		}
		p.mu.Unlock()
		if err != nil || event != uint32(windows.WAIT_TIMEOUT) {
			return 0, errors.Join(experimentalJobBlocked("wait provider", errors.Join(err, fmt.Errorf("wait result = %d", event))), p.Close())
		}
	}
}

func nativeExperimentalJobCalls() (experimentalJobCalls, error) {
	dll := windows.NewLazySystemDLL("kernel32.dll")
	// Resolve before allocating native resources. Missing APIs block this
	// experiment; they never select an uncontained launch path.
	for _, name := range []string{"IsProcessInJob", "CreateJobObjectW", "SetInformationJobObject",
		"CreateProcessW", "InitializeProcThreadAttributeList", "UpdateProcThreadAttribute", "DeleteProcThreadAttributeList",
		"DuplicateHandle", "ResumeThread", "TerminateJobObject", "WaitForSingleObject", "GetExitCodeProcess", "CloseHandle"} {
		if err := dll.NewProc(name).Find(); err != nil {
			return experimentalJobCalls{}, err
		}
	}
	inJob := dll.NewProc("IsProcessInJob")
	return experimentalJobCalls{
		inJob: func(process, job windows.Handle) (bool, error) {
			var member int32
			ok, _, err := inJob.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&member)))
			if ok == 0 {
				return false, err
			}
			return member != 0, nil
		},
		createJob: func() (windows.Handle, error) { return windows.CreateJobObject(nil, nil) },
		setLimits: func(job windows.Handle, limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error {
			_, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)))
			return err
		},
		duplicate: func(handle windows.Handle) (windows.Handle, error) {
			var dup windows.Handle
			err := windows.DuplicateHandle(windows.CurrentProcess(), handle, windows.CurrentProcess(), &dup, 0, true, windows.DUPLICATE_SAME_ACCESS)
			return dup, err
		},
		create:       nativeExperimentalJobCreate,
		resume:       windows.ResumeThread,
		terminateJob: func(job windows.Handle) error { return windows.TerminateJobObject(job, 1) },
		wait:         windows.WaitForSingleObject,
		exitCode:     windows.GetExitCodeProcess,
		close:        windows.CloseHandle,
	}, nil
}

func nativeExperimentalJobCreate(cfg experimentalJobLaunch, stdio [3]windows.Handle, job windows.Handle) (pi windows.ProcessInformation, result error) {
	defer func() {
		if result != nil {
			result = experimentalJobBlocked("atomic Job creation", result)
		}
	}()
	if job == 0 || job == windows.InvalidHandle {
		return pi, errors.New("native creation requires the owned, limited Job handle")
	}
	app, err := windows.UTF16PtrFromString(cfg.Executable)
	if err != nil {
		return pi, err
	}
	command, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{cfg.Executable}, cfg.Args...)))
	if err != nil || len(command) > 32767 {
		return pi, errors.Join(err, errors.New("invalid or oversized native command line"))
	}
	dir, err := windows.UTF16PtrFromString(cfg.Workdir)
	if err != nil {
		return pi, err
	}
	env, err := experimentalJobEnvironment(cfg.Environment)
	if err != nil {
		return pi, err
	}
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return pi, err
	}
	defer attributes.Delete()
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&stdio[0]), unsafe.Sizeof(stdio)); err != nil {
		return pi, err
	}
	jobs := [1]windows.Handle{job}
	if err := attributes.Update(experimentalJobListAttribute, unsafe.Pointer(&jobs[0]), unsafe.Sizeof(jobs)); err != nil {
		return pi, err // Unsupported Job-list attributes NEVER select an unowned fallback.
	}
	si := windows.StartupInfoEx{}
	si.Cb = uint32(unsafe.Sizeof(si))
	si.Flags = windows.STARTF_USESTDHANDLES | windows.STARTF_USESHOWWINDOW
	si.ShowWindow = windows.SW_HIDE
	si.StdInput, si.StdOutput, si.StdErr = stdio[0], stdio[1], stdio[2]
	si.ProcThreadAttributeList = attributes.List()
	// The kernel establishes Job membership during creation. Suspension remains
	// for pre-execution verification; it is not the containment boundary. No
	// breakaway, parent spoofing, shell, or inherited Job handle is supplied.
	flags := uint32(windows.CREATE_SUSPENDED | windows.CREATE_NO_WINDOW |
		windows.CREATE_UNICODE_ENVIRONMENT | windows.EXTENDED_STARTUPINFO_PRESENT)
	err = windows.CreateProcess(app, &command[0], nil, nil, true, flags, &env[0], dir, &si.StartupInfo, &pi)
	runtime.KeepAlive(jobs)
	runtime.KeepAlive(stdio)
	runtime.KeepAlive(attributes)
	return pi, err
}

func experimentalJobEnvironment(entries []string) ([]uint16, error) {
	values := append([]string(nil), entries...)
	sort.Slice(values, func(i, j int) bool { return strings.ToUpper(values[i]) < strings.ToUpper(values[j]) })
	seen := make(map[string]bool, len(values))
	var block []uint16
	for _, entry := range values {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" || seen[strings.ToUpper(key)] {
			return nil, errors.New("explicit environment requires unique nonempty names")
		}
		seen[strings.ToUpper(key)] = true
		encoded, err := windows.UTF16FromString(entry)
		if err != nil {
			return nil, err
		}
		block = append(block, encoded...)
		if len(block) > 32765 {
			return nil, errors.New("explicit environment exceeds proof envelope")
		}
	}
	if len(block) == 0 {
		block = append(block, 0)
	}
	return append(block, 0), nil
}
