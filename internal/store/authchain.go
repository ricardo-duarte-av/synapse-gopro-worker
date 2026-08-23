package store

import (
	"context"
	"fmt"
)

// ErrNoChainCoverIndex reports that the chain cover index does not cover the
// requested events, so the caller must fall back to walking event_auth.
type ErrNoChainCoverIndex struct{ RoomID string }

func (e ErrNoChainCoverIndex) Error() string {
	return fmt.Sprintf("store: no auth chain cover index for room %s", e.RoomID)
}

// GetAuthChainIDs returns the auth chain of the given events, excluding the
// events themselves.
//
// It uses Synapse's chain cover index. Each event is assigned a (chain_id,
// sequence_number), with the invariant that an event auth-reaches every earlier
// sequence number in its own chain, and links between chains record the
// furthest point reachable in the target chain. So instead of walking the auth
// DAG event by event, we compute a maximum reachable sequence number per chain
// and select everything at or below it.
//
// Ported from Synapse's _get_auth_chain_ids_using_cover_index_txn.
func (s *Store) GetAuthChainIDs(ctx context.Context, roomID string, eventIDs []string) ([]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}

	// Step 1: the chain position of each starting event.
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, chain_id, sequence_number FROM event_auth_chains WHERE event_id = ANY($1)`,
		eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: auth chain positions: %w", err)
	}

	seen := make(map[string]struct{}, len(eventIDs))
	eventChains := map[int64]int64{}
	for rows.Next() {
		var eventID string
		var chainID, seq int64
		if err := rows.Scan(&eventID, &chainID, &seq); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: scan chain position: %w", err)
		}
		seen[eventID] = struct{}{}
		if seq > eventChains[chainID] {
			eventChains[chainID] = seq
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: chain position rows: %w", err)
	}

	// Every event must have a chain position, or the index does not really
	// cover this room and the result would be silently incomplete.
	for _, id := range eventIDs {
		if _, ok := seen[id]; !ok {
			return nil, ErrNoChainCoverIndex{RoomID: roomID}
		}
	}

	// Step 2: expand through links to find the furthest point reachable in
	// every chain.
	chains, err := s.materialiseChains(ctx, eventChains)
	if err != nil {
		return nil, err
	}

	// Step 3: an event auth-reaches everything earlier in its own chain, but
	// not itself, hence seq-1. A first-in-chain event contributes nothing.
	for chainID, seq := range eventChains {
		if seq <= 1 {
			continue
		}
		if seq-1 > chains[chainID] {
			chains[chainID] = seq - 1
		}
	}

	if len(chains) == 0 {
		return nil, nil
	}

	// Step 4: everything at or below each chain's reachable sequence number.
	chainIDs := make([]int64, 0, len(chains))
	maxSeqs := make([]int64, 0, len(chains))
	for chainID, maxSeq := range chains {
		chainIDs = append(chainIDs, chainID)
		maxSeqs = append(maxSeqs, maxSeq)
	}

	const q = `
		SELECT c.event_id
		FROM event_auth_chains AS c
		JOIN unnest($1::bigint[], $2::bigint[]) AS l(chain_id, max_seq)
		  ON c.chain_id = l.chain_id
		WHERE c.sequence_number <= l.max_seq`

	resultRows, err := s.pool.Query(ctx, q, chainIDs, maxSeqs)
	if err != nil {
		return nil, fmt.Errorf("store: auth chain events: %w", err)
	}
	defer resultRows.Close()

	var out []string
	for resultRows.Next() {
		var id string
		if err := resultRows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan auth chain event: %w", err)
		}
		out = append(out, id)
	}
	if err := resultRows.Err(); err != nil {
		return nil, fmt.Errorf("store: auth chain rows: %w", err)
	}
	return out, nil
}

// materialiseChains follows event_auth_chain_links transitively, returning the
// highest sequence number reachable in each chain.
//
// Links are fetched a generation at a time rather than one chain at a time,
// which keeps the number of round trips proportional to the depth of the link
// graph instead of its size.
func (s *Store) materialiseChains(ctx context.Context, eventChains map[int64]int64) (map[int64]int64, error) {
	// reachable maps chain ID to the furthest sequence number reached so far.
	reachable := map[int64]int64{}

	// frontier holds chains whose outgoing links still need following, with the
	// origin sequence number we are allowed to traverse from.
	frontier := make(map[int64]int64, len(eventChains))
	for chainID, seq := range eventChains {
		frontier[chainID] = seq
	}

	for len(frontier) > 0 {
		originIDs := make([]int64, 0, len(frontier))
		for chainID := range frontier {
			originIDs = append(originIDs, chainID)
		}

		const q = `
			SELECT origin_chain_id, origin_sequence_number,
			       target_chain_id, target_sequence_number
			FROM event_auth_chain_links
			WHERE origin_chain_id = ANY($1)`

		rows, err := s.pool.Query(ctx, q, originIDs)
		if err != nil {
			return nil, fmt.Errorf("store: auth chain links: %w", err)
		}

		next := map[int64]int64{}
		for rows.Next() {
			var originChain, originSeq, targetChain, targetSeq int64
			if err := rows.Scan(&originChain, &originSeq, &targetChain, &targetSeq); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: scan auth chain link: %w", err)
			}
			// The link is only usable if we can reach its origin point.
			if originSeq > frontier[originChain] {
				continue
			}
			if targetSeq > reachable[targetChain] {
				reachable[targetChain] = targetSeq
				// Reaching further into a chain can open up further links.
				if targetSeq > next[targetChain] {
					next[targetChain] = targetSeq
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("store: auth chain link rows: %w", err)
		}
		frontier = next
	}

	return reachable, nil
}
