package examples_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nnikolash/go-timeline"
	"github.com/stretchr/testify/require"
)

type ChatMessage struct {
	Time time.Time
	Text string
}

var chatMessagesOnServer = []ChatMessage{
	{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "Hello"},
	{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "World"},
	{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "How"},
	{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "Are"},
	{time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), "You"},
	{time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), "Doing"},
	{time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC), "Today"},
	{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "Good"},
	{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "Bye"},
	{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), ":*"},
}

func GetMessagesFromServer(chatID string, from, to time.Time) ([]ChatMessage, error) {
	if chatID != "test-chat-1" {
		return nil, fmt.Errorf("invalid chat ID: %v", chatID)
	}

	var res []ChatMessage

	for _, m := range chatMessagesOnServer {
		if m.Time.Before(from) {
			continue
		}
		if m.Time.After(to) {
			break
		}

		res = append(res, m)
	}

	return res, nil
}

func TestExampleBasicUsage(t *testing.T) {
	t.Parallel()

	// Create load function
	loadMessages := func(key string,
		periodStart, periodEnd time.Time,
		closestFromStart, closestFromEnd *ChatMessage,
		extra interface{},
	) (timeline.CacheFetchResult[ChatMessage], error) {
		msgs, err := GetMessagesFromServer(key, periodStart, periodEnd)
		if err != nil {
			return timeline.CacheFetchResult[ChatMessage]{}, fmt.Errorf("loading chat messages from server: %w", err)
		}

		// Note that period is explicitly set. It must contain at least period [periodStart, periodEnd],
		// but can be bigger. Even if there is no data at that period, PeriodStart and PeriodEnd must be set
		// to indicate that the period is empty.
		return timeline.CacheFetchResult[ChatMessage]{
			Data:        msgs,
			PeriodStart: periodStart,
			PeriodEnd:   periodEnd,
		}, nil
	}

	// Create cache
	cache := timeline.NewMemoryCache(timeline.MemoryCacheOptions[ChatMessage, string]{
		GetTimestamp:  func(d *ChatMessage) time.Time { return d.Time },
		GetFromSource: loadMessages,
	})

	// Check that we have no messages cached
	_, areCached, _ := cache.GetCached("test-chat-1", time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC))
	require.False(t, areCached)

	// Fetch messages from server
	msgs, err := cache.Get("test-chat-1", time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), nil)
	require.NoError(t, err)
	require.Len(t, msgs, 3) // How, Are, You

	// Check that messages are cached
	cachedMsgs, areCached, _ := cache.GetCached("test-chat-1", time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC))
	require.True(t, areCached)
	require.Equal(t, msgs, cachedMsgs)

	// Fetch another period from server
	msgs, err = cache.Get("test-chat-1", time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), nil)
	require.NoError(t, err)
	require.Len(t, msgs, 2) // Good, Bye

	// Check that messages in-between those periods were not loaded
	_, areCached, _ = cache.GetCached("test-chat-1", time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC))
	require.False(t, areCached)
}
