![image](polar-bear-and-elephant-friends--t3chat--1.jpg)
## pg_rs

This extension is *mostly* to provide a lazy postgres query scanner and (eventually) a lazy postgres sink.

### How it works

It uses [tokio-postgres](https://docs.rs/tokio-postgres/latest/tokio_postgres/) to communicate with the server and uses [polars io extension](https://docs.pola.rs/user-guide/plugins/io_plugins/) to turn that into a LazyFrame, all in rust.

### Example Usage


```python
from pg_rs import connect
DSN ="host=postgres.com port=1234 dbname=yours user=postgres password=hunter2"
conn = connect(PG)

lf = conn.scan_pq("select * from big_table")

lf.sink_parquet("big_file.parquet")
```

### Extras

##### Keep-alive

`connect` also takes an optional second argument to induce a keep alive. You can do `conn = connect(DSN, 30)` and if the connection is idle for 30 seconds it'll send `select 1` to the server in the background.

##### async

You can also make the connection with `await async_connect`

Regardless of whether you use `connect` or `await async_connect` you'll get the same class object in return. There's `await fetchall` which will return results as a list of tuples kind of like psycopg would do.

##### check_connection

`conn.check_connection()` will return True or False according to the underlying client. There's a 1 second timeout so if the connection is mysteriously dropped it'll return False right away (well in 1 second) rather than taking forever.

### Data type details

##### UUID
If you query a UUID column you'll get a binary column in polars. It's the same encoding that python's `UUID` uses. For example:

```python

df = conn.scan_pq("select uuid_col, uuid_col::text as str_col").collect()
str(UUID(bytes=df.item(0,0))) == df.item(0,1)
```

I suppose it could be a settable option to get a string instead of binary.
##### Enum

If you query a custom enum type you'll get a string back. I'd like to make it produce a polars Enum, one day.

##### Decimal

¯\_(ツ)_/¯

haven't done it yet

##### Arrays/Lists

nope, those are extra tricky and there will be compatibility issues. [See duckdb's summary from #7 here](https://github.com/duckdb/pg_duckdb/blob/main/docs/types.md). Since duckdb and polars both use arrow memory, the exact same limitation will exist.

> neither database supports the thing that the other supports exactly. Specifically in Postgres it's allowed for different arrays in a column to have a different number of dimensions, e.g. [1] and [[1], [2]] can both occur in the same column. In ~~DuckDB~~polars that's not allowed, i.e. the amount of nesting should always be the same. On the other hand, in ~~DuckDB~~polars it's valid for different lists at the same nest-level to contain a different number of elements, e.g. [[1], [1, 2]]. This is not allowed in Postgres. So conversion between these types is only possible when the arrays follow the subset.

### Implementation details

At a high level, the way this works is a little bit convoluted. For `scan_pq` to return a LazyFrame it calls `pl.io.plugins.register_io_source` which returns a LazyFrame. That function requires a generator that produces DataFrames as well as the final schema. We don't get the schema from the database until we get at least one row of data. That means we can't do things in a linear order. On top of that, I didn't want to only get results when `next` was called on the generator so, instead, there's a background task which asyncronously gets the results and puts them in a buffer. When `next` is called on the generator it will get them from the buffer. That means it'll keep your download speed saturated (almost) no matter what the sink is. Of course if the download is filling the buffer much faster than the sink can upload then there will be waiting but if they're comparable in speed it'll be continuous downloading.