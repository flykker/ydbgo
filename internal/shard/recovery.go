package shard

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// deadProbeThreshold is how many consecutive failed probes mark a node as
// dead in the meta leader's liveness view.
const deadProbeThreshold = 2

// recoveryLoop is the meta leader's ticker that reconciles every shard's
// replica set with the liveness view, restoring the target RF on live nodes.
func (m *Manager) recoveryLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.recoveryTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.reconcile()
		}
	}
}

// reconcile probes registered nodes and heals any under-replicated shard.
func (m *Manager) reconcile() {
	if !m.meta.IsLeader() {
		return
	}
	m.probeNodes()
	cat := m.meta.FSM().Catalog()
	for name, ts := range cat.Tables {
		for _, spec := range ts.Shards {
			if err := m.healShard(spec); err != nil {
				fmt.Fprintf(os.Stderr, "recovery: heal %s/%s: %v\n", name, spec.ID, err)
			}
		}
	}
}

// probeNodes refreshes the leader's liveness view of the catalog nodes.
func (m *Manager) probeNodes() {
	cat := m.meta.FSM().Catalog()
	m.deadMu.Lock()
	defer m.deadMu.Unlock()
	for _, id := range cat.Nodes {
		if id == m.id {
			delete(m.probeFail, id)
			m.dead[id] = false
			continue
		}
		n, ok := cat.Specs[id]
		if ok && nodeReachable(n.SQLAddr) {
			delete(m.probeFail, id)
			m.dead[id] = false
			continue
		}
		m.probeFail[id]++
		m.dead[id] = m.probeFail[id] >= deadProbeThreshold
	}
	for id := range m.dead {
		if !contains(cat.Nodes, id) {
			delete(m.dead, id)
		}
	}
}

func nodeReachable(addr string) bool {
	if addr == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// liveNodes returns registered nodes currently considered alive (in order).
func (m *Manager) liveNodes() []string {
	cat := m.meta.FSM().Catalog()
	m.deadMu.Lock()
	defer m.deadMu.Unlock()
	var out []string
	for _, id := range cat.Nodes {
		if !m.dead[id] {
			out = append(out, id)
		}
	}
	return out
}

func (m *Manager) isDead(id string) bool {
	m.deadMu.Lock()
	defer m.deadMu.Unlock()
	return m.dead[id]
}

// livePlacementAddr returns the SQL address of the most suitable reachable
// placement node of a shard: local first, then a node in the liveness view,
// then any reachable member. Used by DML routing so reads/writes survive a
// failed placement node even on managers that are not the recovery leader.
func (m *Manager) livePlacementAddr(spec *ShardSpec) string {
	for _, id := range spec.Nodes {
		if id == m.id {
			return m.sqlAddr
		}
		addr := m.nodeSQLAddr(id)
		if addr == "" || m.isDead(id) {
			continue
		}
		return addr
	}
	for _, id := range spec.Nodes {
		addr := m.nodeSQLAddr(id)
		if addr != "" && nodeReachable(addr) {
			return addr
		}
	}
	return ""
}

// healShard restores a shard's replica set to rf live nodes, removing dead
// members. It is idempotent: it reconciles spec.Nodes with the liveness view.
func (m *Manager) healShard(spec *ShardSpec) error {
	var dead, alive []string
	for _, nid := range spec.Nodes {
		if m.isDead(nid) {
			dead = append(dead, nid)
		} else {
			alive = append(alive, nid)
		}
	}
	if len(dead) == 0 {
		return nil
	}
	// A config change needs quorum of the CURRENT config; if too many members
	// are down we must wait for them to return.
	if majority := len(spec.Nodes)/2 + 1; len(alive) < majority {
		return fmt.Errorf("shard %s: %d/%d replicas alive, cannot heal until quorum returns", spec.ID, len(alive), len(spec.Nodes))
	}

	target := append([]string(nil), alive...)
	for len(target) < m.rf {
		rep := m.pickReplacement(spec, target)
		if rep == "" {
			break
		}
		target = append(target, rep)
	}

	// mount replacements as pure followers before touching the raft config
	addrs := map[string]string{}
	for _, nid := range target {
		if contains(alive, nid) {
			continue
		}
		addr, err := m.mountReplicaOn(nid, spec, target)
		if err != nil {
			return fmt.Errorf("mount replacement %s for %s: %w", nid, spec.ID, err)
		}
		addrs[nid] = addr
	}

	drive := m.livePlacementAddr(spec)
	if drive == "" {
		return fmt.Errorf("shard %s: no live placement node to drive config change", spec.ID)
	}

	// add new voters, then remove the dead ones
	for _, nid := range target {
		if contains(alive, nid) {
			continue
		}
		if err := m.addShardVoter(drive, spec, nid, addrs[nid]); err != nil {
			return err
		}
	}
	for _, nid := range dead {
		if err := m.removeShardVoter(drive, spec, nid); err != nil {
			return err
		}
	}

	// commit the healed placement to the catalog
	return m.meta.SetShardNodes(spec.Table, spec.ID, target)
}

// pickReplacement chooses a replacement node not already in the placement.
func (m *Manager) pickReplacement(spec *ShardSpec, used []string) string {
	for _, nid := range m.liveNodes() {
		if !contains(used, nid) {
			return nid
		}
	}
	return ""
}

// mountReplicaOn instructs a live spare node to mount a fresh follower replica
// of spec. Returns the transport address of the new replica.
func (m *Manager) mountReplicaOn(nid string, spec *ShardSpec, nodes []string) (string, error) {
	addr := m.nodeSQLAddr(nid)
	if addr == "" {
		return "", fmt.Errorf("replacement node %s not registered", nid)
	}
	cmd := fmt.Sprintf("ADMIN MOUNT-SHARD %s %s %s %s false %s",
		spec.Table, spec.ID, encodeKey(spec.Start), encodeKey(spec.End), strings.Join(nodes, " "))
	resp, err := m.remoteAdmin(addr, cmd)
	if err != nil {
		return "", err
	}
	if !resp.OK {
		return "", errors.New(resp.Error)
	}
	return resp.Result.Note, nil
}

func (m *Manager) addShardVoter(driveAddr string, spec *ShardSpec, peerID, peerAddr string) error {
	cmd := fmt.Sprintf("ADMIN SHARD-ADD-PEER %s %s %s %s", spec.Table, spec.ID, peerID, peerAddr)
	resp, err := m.remoteAdmin(driveAddr, cmd)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *Manager) removeShardVoter(driveAddr string, spec *ShardSpec, peerID string) error {
	cmd := fmt.Sprintf("ADMIN SHARD-REMOVE-PEER %s %s %s", spec.Table, spec.ID, peerID)
	resp, err := m.remoteAdmin(driveAddr, cmd)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return nil
}
