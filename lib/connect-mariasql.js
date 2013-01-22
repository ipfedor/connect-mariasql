module.exports = function(connect) {
	var Store = connect.session.Store

	function MariaStore(options) {
		var self = this
		Store.call(self, options)

		if(!options.client)
			throw new Error('An active MariaSQL client connection must be provided to MariaStore on instantiation')

		self.db = options.client
		self.table = options.table || 'sessions'
		self.db.query('CREATE TABLE IF NOT EXISTS ' + self.table + ' (sid VARCHAR(255) NOT NULL, session TEXT NOT NULL, expires INT, PRIMARY KEY (sid))')
			.on('error', function(err) {
				throw err
			})
	}
	MariaStore.prototype.__proto__ = Store.prototype;
	MariaStore.prototype.get = function(sid, fn) {
		this.db.query('SELECT session FROM ' + self.table + ' WHERE sid = ? LIMIT 1', [sid], true)
			.on('result', function(result) {
				result.on('row', function(row) {
					fn(JSON.parse(row[0]))
				})
			})
			.on('error', function(err) {
				fn(err)
			})
			.on('end', function(info) {
				if(info.numRows == 0)
					fn(null, null)
			})
	}

	MariaStore.prototype.set = function(sid, session, fn) {
		var expires = new Date(session.cookie.expires).getTime() / 1000, s = JSON.stringify(session)
		this.db.query('INSERT INTO ' + self.table + ' (sid, session, expires) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE session = ?, expires = ?', [sid, session, expires, session, expires])
			.on('error', function(err) {
				fn(err)
			})
    }

	MariaStore.prototype.destroy = function (sid, fn) {
		this.db.query('DELETE FROM ' + this.table + ' WHERE sid = ?', [sid])
			.on('error', function(err) {
				fn(err)
			})
	}

	MariaStore.prototype.length = function(fn) {
		this.db.query('SELECT COUNT(sid) AS count FROM ' + this.table, null, true)
			.on('result', function(result) {
				result.on('row', function(row) {
					fn(null, row[0].count)
				})
			})
			.on('error', function(err) {
				fn(err)
			})
	}

	MariaStore.prototype.clear = function (sid, fn) {
		this.db.query('TRUNCATE TABLE ' + this.table)
			.on('error', function(err) {
				fn(err)
			})
	}

	MariaStore.prototype.gc = function(fn) {
		this.db.query('DELETE FROM ' + this.table + ' WHERE expires < UNIX_TIMESTAMP()')
			.on('error', function(err) {
				fn(err)
			})
	}

	return MariaStore
}
