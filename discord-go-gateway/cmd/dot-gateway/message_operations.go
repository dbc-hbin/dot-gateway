package main

import (
	"context"
	"dot-gateway/internal/bridge"
	"errors"
	"io"
	"os"
	"time"
)

func runMessageOperation(cmd, request, claim string, f map[string]string, settings bridge.Settings, store *bridge.Store) (any, error) {
	var spec bridge.MessageOperationSpec
	if cmd == "message-operation" {
		if f["json-file"] == "" {
			return nil, errors.New("operation_json_required")
		}
		file, err := os.Open(f["json-file"])
		if err != nil {
			return nil, errors.New("operation_input_failed")
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, 16385))
		if err != nil {
			return nil, errors.New("operation_input_failed")
		}
		spec, err = bridge.ParseMessageOperation(raw)
		if err != nil {
			return nil, err
		}
	} else if f["operation-id"] == "" || f["verified-in-discord"] != "true" {
		return nil, errors.New("explicit_operation_reconciliation_required")
	}
	rest, err := bridge.NewRESTClient(settings)
	if err != nil {
		return nil, err
	}
	defer rest.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if cmd == "message-operation" {
		return rest.ExecuteMessageOperation(ctx, store, request, claim, spec)
	}
	return rest.ReconcileMessageOperation(ctx, store, request, claim, f["operation-id"], true)
}
