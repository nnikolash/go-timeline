# go-timeline - Timeline data cache for Go

## What can this library do?

* Retrives data for given key and custom period.
* If data is missing for some part of requested period - automatically loads that part (or more) from source.
* Merges overlapping or conjuncted periods.
* Does NOT support deletion.
* Divides timelines by `Key` to be able to store multiple separate timelines - like a map of timelines.
* Able to support any kind of key and data.
* Supports empty periods.
* Supports entries with equal time.
* Is thread-safe.
* Allows to persist and load cache index and data.
* Has multiple drop-in implementations: memory, file, sqlite, two-layer.

This library uses [go-sparse](https://github.com/nnikolash/go-sparse) as base container for storing sparse timeline data.

## Intention

This library is a good choice if:

* You algorythm reads custom **periods of continuous historical data**. Examples of such data could be **market data** and **chat messages**.
* Reading of (almost) **same periods** is expected to occur many times (and maybe by many **parallel** algorythms).
* Reading data from **source** is rather **slow**.
* Feeding the **entire history** to algorythm is **not an option**: either it is too big and you don't know which smaller periods will be requested, or you just don't wan't to configure it every time.

So the idea is that the **algorythm decided** which periods of data it requires, and requests them through this lib.

I personally used it to implement **Binance** and **Telegram** clients for my crypto trading software.
My software has optimization module, which concurrently runs throusands replicas of trading strategy. These replicas are all requesting **candlestick** history and **chat messages** history. Loading them from the server every time would have multiple disadvantages:

* Either I would need to **manually configure** period of data to load, or
* it would be **super slow** to load each small period and for each replica.
* It would result in a ban of my account, because I would immediatelly reach **API limits**.

By using this library my algorythms decides on its own which period to load, the library loads the data which is missing, and it is merged with already cached data. And it is done only once per replica of strategy.

## Usage

###### Have some API server (dummy in this case)

```
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
```

###### Create load function

```
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
```

###### Create cache

```
cache := timeline.NewMemoryCache(timeline.MemoryCacheOptions[ChatMessage, string]{
	GetTimestamp:  func(d *ChatMessage) time.Time { return d.Time },
	GetFromSource: loadMessages,
})
```

###### Check that messages are not yer cached

```
chatID := "test-chat-1"
periodStart := time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC)
periodEnd := time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC)

_, areCached, _ := cache.GetCached(chatID, periodStart, periodEnd)
require.False(t, areCached)
```

###### Fetch messages from server

```
msgs, err := cache.Get(chatID, periodStart, periodEnd, nil)
require.NoError(t, err)
require.Len(t, msgs, 3) // How, Are, You
```

For examples, see folder `examples` or test files `*_test.go`.

## Implemented caches

#### Memory cache

#### File cache

#### SQLite cache

#### Two-layer cache

## Special use-cases

#### Composite keys

*Key* is an identification of specific historical timeline. For chats history that would be ID or name of a chat. For candlestick history that would be coin name + timeframe (e.g. `(BTCUSDT, 1h)`). And in such an example of **coin name + timeframe** the key will be a composite key - a structure of two fields. This is supported by the library, but correct funtion for "stringify key" function must be provided.

#### Sorting by Time + ID

Some entries not only have timestamp, but also other index fields, e.g. **ID**. This is supported seemlesly by the library, because order of entries is preserved. So if you want stable ordering of the entries - sort them by `Time + ID` in load funtion.

#### Updating the most recent entry

In some historical data the entries not added, but also updated. Example of such data is **candlestik** history: last candle is updated maybe time before new candle is added.

The library supports **updating entries** - when you add new period, it **overwrites** any cached entries in that period.
So in case of candlestick history, the function, which loads data from source, must update not the requested period exactly, but period of at least one candle length. In that case, when the library requests data from source for period [last update; now] it will actually get data at least for period [candle start ; now] and thus will overwrite last candle. But don't forget to also extend the period returned to the load funtion - it must at least include ALL returned entries.

For such case not only last entry must be updated, but every edge entry on older side of requested period. See next point for explanation why.

#### Updating edge entries

If the nature of your historical data includes updates of last entry, you need to implement load function, so that it **always loads the last** **entry** before requested period. This is because at the time, when that period was fetched, that entry was most likely **incomplete** and later received more updates, which are missing in the cache. So you need to extend fetched period to overwrite that entry with its latest version.

For example, image loading 1h candles first for date `[10:00; now]` at `12:15`. That would candles for `[10:00; 11:00), [11:00; 12:00)` and `[12:00:13:00)`. Then some time passes, and at `14:30` you want to load  candles for bigger period `[10:00; now]` (so `[10:00; 14:30]`). The library would ask your load funtion to load period `[12:15; now]`. But the candles are identifies by their `Open Time `, that would load only candles `[13:00; 14:00)` and `[14:00; 15:00)`. Candle `[12:00; 13:00)` is not included, because its `Open Time` is smaller than `12:15`. But because of this that edge candle `[12:00; 13:00)` won't received updated, which were done between `(12:15 and 13:00)`.
To fix that, load funtion must load one candle more than requested. It will return it to the library and library will overwrite the obsolete version. But don't forget to also extend the period returned to the load funtion - it must at least include ALL returned entries.

#### Source does not provide convenient interface

Sometimes source of data may not provide interface for getting data for a **custom period** - sometimes only **pagination** is available.
This is not a problem. In that case, the funtion for loadig data from source must load all the pages until requested period and the period itself, an return everything it loaded. The library will cache all of that, which might be useful in future, especially you persist cache content.

Some sources may provide pagination, but with an ability to **start from specific entry** or page other than first. If that is the case, the load function can use border entries information provided by the library to determine from with entry or page it should start, instead of doing than from the beggining.

Example of such interface is **Telegram API**. In Telegram, there is only pagination available, so you cannot request messages for a custom period. But you can request them starting from specific message ID. So the load function an start from newest loaded message after requested period, instead of starting from the latest message in chat.
