var hashring = require('hashring');
var multilevel = require('multilevel');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits
var net = require('net');

module.exports = Sharded;

function Sharded (databases) {
	if (!(this instanceof Sharded)) return new Sharded(databases);

	EventEmitter.call(this);

	this.databases = {};
	this.ring = new hashring();
	this.performingMaintenance = false;

	if (databases) databases.forEach(this.addNode.bind(this));
}

inherits(Sharded, EventEmitter);

/**
 * Read data from the right database.
 *
 * @param {String} key
 * @param {Object=} opts
 * @param {Function} cb
 */

Sharded.prototype.get = function (args) {
	var cb = [].slice.call(arguments).pop();
	var addr = this.ring.get(key);
	if (!addr) return cb('no matching db in cluster');
	var db = this.databases[addr];
	db.get.call(db, arguments)
};

/**
 * Write data to the right database.
 *
 * @param {String} key
 * @param {String|Object} value
 * @param {Object=} opts
 * @param {Function=} cb
 */

Sharded.prototype.put = function (args) {
	var cb = [].slice.call(arguments).pop();
	var addr = this.ring.get(key);
	if (!addr) return cb('no matching db in cluster');
	var db = this.databases[addr];
	if (typeof cb == 'function') db.put.call(db, arguments);
};

/**
 * Add a node to the cluster.
 *
 * @param {String} newAddr
 * @param {Function=} cb
 */

Sharded.prototype.addNode = function (newAddr, cb) {
  if (!cb) cb = function () {};
  if (this.databases[newAddr]) return cb('node already in cluster');
  
  if (this.performingMaintenance) {
    this.on('maintenance finished', this.addNode.call(this, arguments));
  }
  this.performingMaintenance = true;
  
	var databases = this.databases;
	var newRing = new hashring(Object.keys(this.databases).concat(newAddr));
	var movedKeys = [];
	var self = this;
	
	var client = multilevel.client();
	client.on('remote', function (newDb) {
		// move over data
		var addresses = Object.keys(databases);
		if (addresses.length) {
			var databasesToMove = addresses.length;
			var readStreams = []
			addresses.forEach(function (addr) {
				var db = databases[addr];
				var error = false;
				var ended = false;

				var rs = db.readStream();
				readStreams.push(rs);
				
				var keysLeft = 0;
				
				rs.on('data', function (key, value) {
					if (newRing.get(key) != addr) {
					  keysLeft++;
						newDb.put(key, value, function (err) {
						  keysLeft--;
							if (err) {
							  error = err;
								return rs.end();
							}
							movedKeys.push({ from : addr, key : key });
							if (ended && !keysLeft) onEnded();
						})
					}
				})
				
				rs.on('end', onEnded);
				
				function onEnded () {
				  ended = true;
				  if (error) {
				    var oldCb = cb;
				    cb = function () {};
				    oldCb(error);
				    self.finishMaintenance();
				    readStreams.forEach(function (readStream) {
				      // it is fine to call stream.end() multiple times
				      readStream.end();
				    });
				    return;
				  }
				  if (keysLeft) return;
				  if (!--databasesToMove) onMoved();
				}
			});
		} else {
		  onMoved();
		}

    function onMoved () {
      self.databases[newAddr] = newDb;
  		self.ring.add(newAddr);
  		if (cb) cb();
  		self.finishMaintenance();
  		
  		// remove duplicated keys
  		movedKeys.forEach(function (move) {
  		  self.databases[move.from].del(move.key);
  		});
    }
	});
	client.pipe(net.connect(addr)).pipe(client);
};

/**
 * Remove a node from the cluster.
 *
 * @param {String} addr
 * @param {Function=} cb
 */

Sharded.prototype.removeNode = function (addr, cb) {
	// move over data
	
	delete this.databases[addr];
	this.ring.remove(addr);
	if (cb) cb();
};

/**
 * Finish maintenance.
 *
 * @api private
 */

Sharded.prototype.finishMaintenance = function () {
  this.performingMaintenance = false;
	this.emit('maintenance finished');
}
