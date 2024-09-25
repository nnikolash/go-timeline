# go-timeline - Timeline data cache for Go

## What can this library do?

* Retrives data for given key and custom period.
* If data is missing for some part of requested period - automatically loads that part (or more) from source.
* Merges overlapping or conjuncted periods.
* Does NOT support deletion.
* Is thread-safe.
* Able to support any kind of key and data.
* Supports empty periods.
* Supports entries with equal time.
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

###### Create cache

## Implemented caches

#### Memory cache

#### File cache

#### SQLite cache

#### Two-layer cache

## Special use-cases

#### Composite keys

Key is a identification of specific historical timeline. For chats history that would be ID or name of chat. For candlestick history that would be coin name + timeframe. And in such an example of **coin name + timeframe** the key will be a composite key - a structure of two fields. This is supported by the library, but it must be provided with correct funtion for converting that key to string.

#### Sorting by Time + ID

Some entries only have timestamp, but also other index fields, e.g. **ID**. This is supported seemlesly by the library, because order of entries is preserved. So if you want stable order of such entries - sort them by Time + ID in load funtion.

#### Updating the most recent entry

In some historical data the entries not added, but also updated. Example of such data is **candlestik** history: last candle is updated maybe time before new candle is added.

The library supports **updating entries** - when you add new period, it **overwrites** any cached entries in that period.
So in case of candlestick history, the function, which loads data from source, must update not the requested period exactly, but period of at least one candle length. In that case, when the library requests data from source for period [last update; now] it will actually get data at least for period [candle start ; now] and thus will overwrite last candle.

#### Source does not provide convenient interface

Sometimes source of data may not provide interface for getting data for a **custom period** - sometimes only **pagination** is available.
This is not a problem. In that case, the funtion for loadig data from source must load all the pages until requested period and the period itself, an return everything it loaded. The library will cache all of that, which might be useful in future, especially you persist cache content.

Some sources may provide pagination, but with an ability to **start from specific entry** or page other than first. If that is the case, the load function can use border entries information provided by the library to determine from with entry or page it should start, instead of doing than from the beggining.

Example of such interface is **Telegram API**. In Telegram, there is only pagination available, so you cannot request messages for a custom period. But you can request them starting from specific message ID. So the load function an start from newest loaded message after requested period, instead of starting from the latest message in chat.
