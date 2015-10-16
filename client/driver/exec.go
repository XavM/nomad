package driver

import (
	"fmt"
        "log"
        "os/exec"
        "path"
        "path/filepath"
	"runtime"
	"syscall"
	"time"

        "github.com/hashicorp/go-getter"
        "github.com/hashicorp/nomad/client/allocdir"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/client/executor"
	"github.com/hashicorp/nomad/nomad/structs"
)

// ExecDriver fork/execs tasks using as many of the underlying OS's isolation
// features.
type ExecDriver struct {
	DriverContext
}

// execHandle is returned from Start/Open as a handle to the PID
type execHandle struct {
	cmd    executor.Executor
	waitCh chan error
	doneCh chan struct{}
}

// NewExecDriver is used to create a new exec driver
func NewExecDriver(ctx *DriverContext) Driver {
	return &ExecDriver{*ctx}
}

func (d *ExecDriver) Fingerprint(cfg *config.Config, node *structs.Node) (bool, error) {
	// Only enable if we are root when running on non-windows systems.
	if runtime.GOOS != "windows" && syscall.Geteuid() != 0 {
		d.logger.Printf("[DEBUG] driver.exec: must run as root user, disabling")
		return false, nil
	}

	node.Attributes["driver.exec"] = "1"
	return true, nil
}

func (d *ExecDriver) Start(ctx *ExecContext, task *structs.Task) (DriverHandle, error) {
        // Get the command to be ran, or if omitted, download an artifact for
        // execution. Currently a supplied command takes precedence, and an artifact
        // is only downloaded if no command is supplied
	command, ok := task.Config["command"]
	if !ok || command == "" {
                source, sok := task.Config["artifact_source"]
                if !sok || source == "" {
                        return nil, fmt.Errorf("missing command or source for exec driver")
                }

                // Proceed to download an artifact to be executed.
                // We use go-getter to support a variety of protocols, but need to change
                // file permissions of the resulted download to be executable
                taskDir, ok := ctx.AllocDir.TaskDirs[d.DriverContext.taskName]
                if !ok {
                        return nil, fmt.Errorf("[Err] driver.Exec: Could not find task directory for task: %v", d.DriverContext.taskName)
                }

                destDir := filepath.Join(taskDir, allocdir.TaskLocal)

                // Create a location to download the artifact.
                artifactName := path.Base(source)
                command = filepath.Join(destDir, artifactName)
                if err := getter.GetFile(command, source); err != nil {
                        return nil, fmt.Errorf("[Err] driver.Exec: Error downloading source for Exec driver: %s", err)
                }

                cmd := exec.Command("chmod", "+x", command)
                if err := cmd.Run(); err != nil {
                        log.Printf("[Err] driver.Exec: Error making artifact executable: %s", err)
                }

                // re-assign the command to be the local execution path
                command = filepath.Join(allocdir.TaskLocal, artifactName)
	}

	// Get the environment variables.
	envVars := TaskEnvironmentVariables(ctx, task)

	// Look for arguments
	var args []string
	if argRaw, ok := task.Config["args"]; ok {
		args = append(args, argRaw)
	}

	// Setup the command
	cmd := executor.Command(command, args...)
	if err := cmd.Limit(task.Resources); err != nil {
		return nil, fmt.Errorf("failed to constrain resources: %s", err)
	}

	// Populate environment variables
	cmd.Command().Env = envVars.List()

	if err := cmd.ConfigureTaskDir(d.taskName, ctx.AllocDir); err != nil {
		return nil, fmt.Errorf("failed to configure task directory: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start command: %v", err)
	}

	// Return a driver handle
	h := &execHandle{
		cmd:    cmd,
		doneCh: make(chan struct{}),
		waitCh: make(chan error, 1),
	}
	go h.run()
	return h, nil
}

func (d *ExecDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	// Find the process
	cmd, err := executor.OpenId(handleID)
	if err != nil {
		return nil, fmt.Errorf("failed to open ID %v: %v", handleID, err)
	}

	// Return a driver handle
	h := &execHandle{
		cmd:    cmd,
		doneCh: make(chan struct{}),
		waitCh: make(chan error, 1),
	}
	go h.run()
	return h, nil
}

func (h *execHandle) ID() string {
	id, _ := h.cmd.ID()
	return id
}

func (h *execHandle) WaitCh() chan error {
	return h.waitCh
}

func (h *execHandle) Update(task *structs.Task) error {
	// Update is not possible
	return nil
}

func (h *execHandle) Kill() error {
	h.cmd.Shutdown()
	select {
	case <-h.doneCh:
		return nil
	case <-time.After(5 * time.Second):
		return h.cmd.ForceStop()
	}
}

func (h *execHandle) run() {
	err := h.cmd.Wait()
	close(h.doneCh)
	if err != nil {
		h.waitCh <- err
	}
	close(h.waitCh)
}
