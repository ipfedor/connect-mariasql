# connect-mariasql

A lightweight, functional MariaDB session storage wrapper for connect.

It uses an already-created `mariasql` (their [[github]](https://github.com/mscdex/node-mariasql/)) connection, creates the `sessions` table on the fly (if necessary), and will not burn your house down.

## license

MIT license

## usage

``` js
var express = require('express'),
	MariaSQL = require('mariasql'),
	MariaStore = require('connect-mariasql')(express),
	db = new MariaSQL()

db.connect({
  host: '127.0.0.1',
  user: 'foo',
  password: 'bar'
})

var app = express()
app.use(express.session({
	store: new MariaStore({ client: db }),
	secret: 'mysecret',
}))

// ...
```

#### new MariaStore(options)

* `options`:
  * `table` (string, default: `sessions`) - the name of the table to store sessions inside
  * `client` - (**required!**) the MariaSQL client connection to utilize
