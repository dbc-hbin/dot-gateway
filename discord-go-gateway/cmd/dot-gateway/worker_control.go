package main

import (
	"dot-gateway/internal/bridge"
	"errors"
	"strconv"
)

// These commands only record attestations by the real reasoning controller.
// They never call native collaboration tools or start/interrupt a process.
func runWorkerControl(cmd string, pos []string, f map[string]string, store *bridge.Store) (any, error) {
	if cmd == "worker-status" {
		return store.WorkerStatus()
	}
	if len(pos) != 1 {
		return nil, errors.New("invalid_arguments")
	}
	worker := pos[0]
	seconds := 60
	if f["lease-seconds"] != "" {
		n, e := strconv.Atoi(f["lease-seconds"])
		if e != nil {
			return nil, errors.New("invalid_worker_lease")
		}
		seconds = n
	}
	switch cmd {
	case "worker-register":
		state := f["state"]
		if state == "" {
			state = "running"
		}
		return store.RegisterWorker(worker, f["incarnation"], f["controller"], state, seconds, f["previous-incarnation"], f["evidence-ref"])
	case "worker-observe":
		return store.ObserveWorker(worker, f["incarnation"], f["controller"], f["state"], seconds, f["evidence-ref"])
	case "worker-bind":
		return store.BindWorkerClaim(pos[0], f["claim"], f["worker-id"], f["incarnation"], f["controller"], f["evidence-ref"])
	case "worker-cancellations":
		pending, err := store.WorkerCancellations(worker, f["incarnation"], f["controller"])
		return map[string]any{"cancellations": pending}, err
	case "worker-cancel-ack":
		return store.AcknowledgeWorkerCancellation(pos[0], f["worker-id"], f["incarnation"], f["controller"], f["evidence-ref"])
	}
	return nil, errors.New("unsupported_worker_command")
}
