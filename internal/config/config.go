// Package config loads the YDB-style cluster configuration: a single YAML file
// describing every node of the cluster. Each node finds its own entry by
// -node-id (or uses the only entry when there is one) and derives its listen
// addresses, data dir and join target from the topology in the file.
//
// Example:
//
//	config:
//	  hosts:
//	  - {host: 127.0.0.1, grpc: 2135, raft: 7001, data: ./ydbgo-data, id: n1, bootstrap: true}
//	  - {host: 127.0.0.1, grpc: 2136, raft: 7002, data: ./ydbgo-n2, id: n2}
package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Host is one node of the cluster.
type Host struct {
	ID        string `yaml:"id"`
	Host      string `yaml:"host"`    // listen host for grpc and raft (default 127.0.0.1)
	GRPC      int    `yaml:"grpc"`    // SQL/gRPC listen port
	Raft      int    `yaml:"raft"`    // raft listen port
	Data      string `yaml:"data"`    // data directory
	Bootstrap bool   `yaml:"bootstrap"` // this node creates the meta group
}

// GRPCAddr returns the host:port SQL listen address.
func (h *Host) GRPCAddr() string {
	return joinHostPort(h.Host, h.GRPC)
}

// RaftAddr returns the host:port raft listen address.
func (h *Host) RaftAddr() string {
	return joinHostPort(h.Host, h.Raft)
}

func joinHostPort(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}

// Config is the top-level cluster configuration file.
type Config struct {
	Config struct {
		Hosts []Host `yaml:"hosts"`
	} `yaml:"config"`
}

// Load reads and validates the configuration file at path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks the topology invariants: at least one host, unique ids,
// valid ports and exactly one bootstrap node.
func (c *Config) Validate() error {
	hosts := c.Config.Hosts
	if len(hosts) == 0 {
		return fmt.Errorf("config.hosts: at least one host is required")
	}
	seen := map[string]bool{}
	bootstraps := 0
	for i := range hosts {
		h := &hosts[i]
		if h.Host == "" {
			h.Host = "127.0.0.1"
		}
		if h.ID == "" {
			return fmt.Errorf("host %d: id is required", i)
		}
		if seen[h.ID] {
			return fmt.Errorf("host %d: duplicate id %q", i, h.ID)
		}
		seen[h.ID] = true
		if h.GRPC < 1 || h.GRPC > 65535 {
			return fmt.Errorf("host %q: invalid grpc port %d", h.ID, h.GRPC)
		}
		if h.Raft < 1 || h.Raft > 65535 {
			return fmt.Errorf("host %q: invalid raft port %d", h.ID, h.Raft)
		}
		if h.Data == "" {
			return fmt.Errorf("host %q: data is required", h.ID)
		}
		if h.Bootstrap {
			bootstraps++
		}
	}
	if bootstraps == 0 {
		return fmt.Errorf("config.hosts: exactly one host must have bootstrap: true (found none)")
	}
	if bootstraps > 1 {
		return fmt.Errorf("config.hosts: exactly one host must have bootstrap: true (found %d)", bootstraps)
	}
	return nil
}

// SelectNode returns the host entry for the given node id. With an empty id a
// single-host config is used; otherwise the config is ambiguous and an error is
// returned so the caller can ask for -node-id.
func (c *Config) SelectNode(id string) (*Host, error) {
	hosts := c.Config.Hosts
	if id == "" {
		if len(hosts) == 1 {
			return &hosts[0], nil
		}
		return nil, fmt.Errorf("config has %d hosts; pass -node-id to select one", len(hosts))
	}
	for i := range hosts {
		if hosts[i].ID == id {
			return &hosts[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in config hosts", id)
}

// BootstrapHost returns the single host with bootstrap: true.
func (c *Config) BootstrapHost() *Host {
	for i := range c.Config.Hosts {
		if c.Config.Hosts[i].Bootstrap {
			return &c.Config.Hosts[i]
		}
	}
	return nil
}

// JoinTarget returns the SQL address of the bootstrap node, used as the join
// address for non-bootstrap nodes. Empty when h is the bootstrap node.
func (c *Config) JoinTarget(h *Host) string {
	b := c.BootstrapHost()
	if b == nil || b.ID == h.ID {
		return ""
	}
	return b.GRPCAddr()
}
