package config

import (
	"flag"
	"fmt"
)

// Apply fills in values derived from the selected host entry for every flag the
// user did not set explicitly on the command line (flag.Visit reports only the
// flags that were set), so an explicit flag always wins over the config file:
//
//	explicit flag > config file > built-in default
//
// Supported flags mirror the serve command line. Unknown flag names are ignored
// so the same helper can serve run/repl/bench, which only share a subset.
func Apply(fs *flag.FlagSet, h *Host, join string) error {
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	set := func(name, value string) error {
		if fs.Lookup(name) == nil {
			return nil // flag not registered in this FlagSet (run/repl/bench)
		}
		if explicit[name] {
			return nil
		}
		if err := fs.Set(name, value); err != nil {
			return fmt.Errorf("config: %s: %w", name, err)
		}
		return nil
	}

	apply := []struct {
		name  string
		value string
	}{
		{"addr", h.GRPCAddr()},
		{"data", h.Data},
		{"raft-addr", h.RaftAddr()},
		{"node-id", h.ID},
		{"bootstrap", boolStr(h.Bootstrap)},
	}
	if join != "" {
		apply = append(apply, struct{ name, value string }{"join", join})
	}
	for _, a := range apply {
		if err := set(a.name, a.value); err != nil {
			return err
		}
	}
	return nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
