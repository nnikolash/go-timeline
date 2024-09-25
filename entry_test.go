package timeline_test

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheEntyIndex_Compare(t *testing.T) {
	t.Parallel()

	data := []time.Time{
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i].Compare(data[j]) < 0
	})

	require.Equal(t, []time.Time{
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
	}, data)
}
