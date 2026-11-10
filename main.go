package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func usage() {
	fmt.Fprintln(os.Stderr,
		"usage: timeout-go [-k DURATION | --kill-after=DURATION] DURATION COMMAND [ARG...]")
}

func forwardSignal(
    pgid int,
    sig syscall.Signal,
    killAfter time.Duration,
    hasKillAfter bool,
    done <-chan error,
    signals <-chan os.Signal,
) int {
    signalGroup(pgid, sig)

    if !hasKillAfter {
        <-done
        return 128 + int(sig)
    }

    timer := time.NewTimer(killAfter)
    defer timer.Stop()

    for {
        select {
        case <-done:
            return 128 + int(sig)

        case received := <-signals:
            if s, ok := received.(syscall.Signal); ok {
                signalGroup(pgid, s)
            }

        case <-timer.C:
            signalGroup(pgid, syscall.SIGKILL)
            <-done
            return 128 + int(sig)
        }
    }
}

func parseDuration(s string) (time.Duration, error) {
	// GNU-style bare numbers mean seconds.
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		if v < 0 {
			return 0, errors.New("negative duration")
		}
		d := time.Duration(v * float64(time.Second))
		if v > 0 && d <= 0 {
			return 0, errors.New("duration overflow")
		}
		return d, nil
	}

	// GNU-style days.
	if strings.HasSuffix(s, "d") {
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if err != nil || v < 0 {
			return 0, errors.New("invalid duration")
		}
		d := time.Duration(v * float64(24*time.Hour))
		if v > 0 && d <= 0 {
			return 0, errors.New("duration overflow")
		}
		return d, nil
	}

	// Go duration syntax: ns, us, ms, s, m, h.
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, errors.New("invalid duration")
	}
	return d, nil
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}

	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if code := ee.ExitCode(); code >= 0 {
			return code
		}

		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return 128 + int(ws.Signal())
		}
	}

	return 125
}

func signalGroup(pgid int, sig syscall.Signal) {
	// Negative PID targets the entire process group.
	_ = syscall.Kill(-pgid, sig)
}

func main() {
	os.Exit(run())
}

func run() int {
	args := os.Args[1:]

	var killAfter time.Duration
	var hasKillAfter bool

	// Parse options.
	for len(args) > 0 {
		switch {
		case args[0] == "-k", args[0] == "--kill-after":
			if len(args) < 2 {
				usage()
				return 125
			}

			d, err := parseDuration(args[1])
			if err != nil {
				fmt.Fprintln(os.Stderr, "timeout-go:", err)
				return 125
			}

			killAfter = d
			hasKillAfter = true
			args = args[2:]

		case strings.HasPrefix(args[0], "--kill-after="):
			d, err := parseDuration(strings.TrimPrefix(args[0], "--kill-after="))
			if err != nil {
				fmt.Fprintln(os.Stderr, "timeout-go:", err)
				return 125
			}

			killAfter = d
			hasKillAfter = true
			args = args[1:]

		case args[0] == "--":
			args = args[1:]
			goto parsed

		case strings.HasPrefix(args[0], "-"):
			usage()
			return 125

		default:
			goto parsed
		}
	}

parsed:
	if len(args) < 2 {
		usage()
		return 125
	}

	timeout, err := parseDuration(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "timeout-go:", err)
		return 125
	}
	args = args[1:]

	// Start command in a new process group.
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "timeout-go:", err)

		if errors.Is(err, exec.ErrNotFound) {
			return 127
		}
		return 126
	}

	pgid := cmd.Process.Pid

	// Reap the command asynchronously.
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// Forward external signals to the command's process group.
	signals := make(chan os.Signal, 4)
	signal.Notify(
		signals,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
	)
	defer signal.Stop(signals)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Normal completion.
	select {
	case err := <-done:
		return exitStatus(err)

	case sig := <-signals:
		if s, ok := sig.(syscall.Signal); ok {
			return forwardSignal(
			pgid,
			s,
			killAfter,
			hasKillAfter,
			done,
			signals,
			)
		}
		return 125

	case <-timer.C:
		// Timeout expired.
	}

	// First escalation stage.
	signalGroup(pgid, syscall.SIGTERM)

	if hasKillAfter {
		// Wait for the escalation interval, forwarding signals
		// received during that interval.
		timer.Reset(killAfter)

		for {
			select {
			case sig := <-signals:
				if s, ok := sig.(syscall.Signal); ok {
					signalGroup(pgid, s)
				}

			case <-timer.C:
				goto escalate
			}
		}
	}

	// Without -k, wait for the command to exit after SIGTERM.
	for {
		select {
		case <-done:
			return 124

		case sig := <-signals:
			if s, ok := sig.(syscall.Signal); ok {
				signalGroup(pgid, s)
			}
		}
	}

escalate:
	// Forcefully terminate the entire process group.
	signalGroup(pgid, syscall.SIGKILL)

	// Reap the direct child.
	<-done

	return 124
}
