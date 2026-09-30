package delayqueue

// ScanPending iterates ids of messages waiting for their delivery time.
// It follows ZSCAN semantics: an id may be returned more than once, and ids added or
// removed during the iteration may or may not be returned. Iteration ends when the
// returned cursor is 0. count is a hint of the batch size.
func (q *DelayQueue) ScanPending(cursor uint64, count int64) (ids []string, next uint64, err error) {
	return q.redisCli.ZScan(q.pendingKey, cursor, count)
}

// GetPayloads returns payloads of the given message ids.
// Ids whose payload no longer exists (consumed, intercepted or expired) are omitted.
func (q *DelayQueue) GetPayloads(ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = q.genMsgKey(id)
	}
	values, err := q.redisCli.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(values))
	for i, id := range ids {
		if value, ok := values[keys[i]]; ok {
			result[id] = value
		}
	}
	return result, nil
}
