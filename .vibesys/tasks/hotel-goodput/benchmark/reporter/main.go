// Report Python measurements using the official VibeSys evaluator SDK.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval"
)

type spec struct {
	Unit      string `json:"unit"`
	Direction string `json:"direction"`
	Required  bool   `json:"required"`
}
type message struct {
	Schema map[string]spec    `json:"schema"`
	Values map[string]float64 `json:"values"`
	Error  string             `json:"error"`
}

func report() (err error) {
	r, err := vseval.OpenPath(os.Args[1])
	if err != nil {
		return err
	}
	defer r.Close()
	defer func() {
		if err != nil && !r.OutcomeWritten() {
			_ = r.EmitError(err)
		}
	}()
	decoder := json.NewDecoder(os.Stdin)
	var first message
	if err = decoder.Decode(&first); err != nil {
		return err
	}
	if first.Error != "" {
		return r.EmitError(errors.New(first.Error))
	}
	schema := vseval.NewSchema()
	metrics := map[string]vseval.Metric{}
	for name, item := range first.Schema {
		direction := vseval.Max
		if item.Direction == "min" {
			direction = vseval.Min
		}
		opts := []vseval.MetricOption{vseval.Unit(item.Unit), vseval.Direction(direction)}
		if !item.Required {
			opts = append(opts, vseval.Optional())
		}
		metrics[name] = schema.Number(name, opts...)
	}
	run, err := r.Declare(schema)
	if err != nil {
		return err
	}
	var result message
	if err = decoder.Decode(&result); err != nil {
		return err
	}
	if result.Error != "" {
		return run.EmitError(errors.New(result.Error))
	}
	for name, value := range result.Values {
		metric, ok := metrics[name]
		if !ok {
			return fmt.Errorf("undeclared metric %s", name)
		}
		run.Set(metric, value)
	}
	return run.Emit()
}
func main() {
	if err := report(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
