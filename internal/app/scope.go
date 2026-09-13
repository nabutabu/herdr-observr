package app

// scope holds the live runtime state of the active subscription that App owns.
// pane.agent_status_changed is the only per-pane subscription, so the exact set
// of pane IDs the current subscription covers must be remembered between
// subscribes — the tracker can't answer it (it also knows panes created after
// the subscribe). It is replaced wholesale whenever the subscription is
// (re)built, from the authoritative list returned by SubscribeFromSnapshot, and
// is read/written solely on the Run event-loop goroutine — no locking.
type scope struct {
	subscribedPanes map[string]struct{}
}

func newScope(paneIDs []string) *scope {
	s := &scope{subscribedPanes: make(map[string]struct{}, len(paneIDs))}
	for _, id := range paneIDs {
		s.subscribedPanes[id] = struct{}{}
	}
	return s
}

// subscribed reports whether the current subscription covers paneID for
// pane.agent_status_changed.
func (s *scope) subscribed(paneID string) bool {
	_, ok := s.subscribedPanes[paneID]
	return ok
}
