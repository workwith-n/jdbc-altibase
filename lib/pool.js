/* jshint node: true */
"use strict";
var _ = require('lodash');
var asyncjs = require('async');
var uuid = require('uuid');
var jinst = require("./jinst");
var dm = require('./drivermanager');
var Connection = require('./connection');
var winston = require('winston');

var java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

var keepalive = function (conn, query) {
  var self = this;
  conn.createStatement(function (err, statement) {
    if (err) return winston.error(err);
    statement.executeQuery(query, function (err, result) {
      if (err) return winston.error(err);
      winston.silly("%s - Keep-Alive", new Date().toUTCString());
    });
  });
};

var addConnection = function (url, props, ka, maxIdle, callback) {
  dm.getConnection(url, props, function (err, conn) {
    if (err) {
      return callback(err);
    } else {
      var connobj = {
        uuid: uuid.v4(),
        conn: new Connection(conn),
        keepalive: ka.enabled ? setInterval(keepalive, ka.interval, conn, ka.query) : false
      };

      if (maxIdle) {
        connobj.lastIdle = new Date().getTime();
      }

      return callback(null, connobj);
    }
  });
};

var addConnectionSync = function (url, props, ka, maxIdle) {
  var conn = dm.getConnectionSync(url, props);
  var connobj = {
    uuid: uuid.v4(),
    conn: new Connection(conn),
    keepalive: ka.enabled ? setInterval(keepalive, ka.interval, conn, ka.query) : false
  };

  if (maxIdle) {
    connobj.lastIdle = new Date().getTime();
  }

  return connobj;
};

function Pool(config) {
  this._url = config.url;
  this._props = (function (config) {
    var Properties = java.import('java.util.Properties');
    var properties = new Properties();

    for (var name in config.properties) {
      properties.putSync(name, config.properties[name]);
    }

    // NOTE: https://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#getProperty(java.lang.String)
    // if property does not exist it returns 'null' in the new java version, so we can use _.isNil to support
    // older versions as well

    if (config.user && _.isNil(properties.getPropertySync('user'))) {
      properties.putSync('user', config.user);
    }

    if (config.password && _.isNil(properties.getPropertySync('password'))) {
      properties.putSync('password', config.password);
    }

    return properties;
  })(config);
  this._drivername = config.drivername ? config.drivername : '';
  this._minpoolsize = config.minpoolsize ? config.minpoolsize : 1;
  this._maxpoolsize = config.maxpoolsize ? config.maxpoolsize : 1;
  this._keepalive = config.keepalive ? config.keepalive : {
    interval: 60000,
    query: 'select 1',
    enabled: false
  };
  this._maxidle = (!this._keepalive.enabled && config.maxidle) || null;
  this._logging = config.logging ? config.logging : {
    level: 'error'
  };
  this._pool = [];
  this._reserved = [];
}

var connStatus = function (acc, pool) {
  _.reduce(pool, function (conns, connobj) {
    var conn = connobj.conn;
    var closed = conn.isClosedSync();
    var readonly = conn.isReadOnlySync();
    var valid = conn.isValidSync(1000);
    conns.push({
      uuid: connobj.uuid,
      closed: closed,
      readonly: readonly,
      valid: valid
    });
    return conns;
  }, acc);
  return acc;
};

Pool.prototype.status = function (callback) {
  var self = this;
  var status = {};
  status.available = self._pool.length;
  status.reserved = self._reserved.length;
  status.pool = connStatus([], self._pool);
  status.rpool = connStatus([], self._reserved);
  callback(null, status);
};

Pool.prototype._addConnectionsOnInitialize = function (callback) {
  var self = this;
  asyncjs.times(self._minpoolsize, function (n, next) {
    addConnection(self._url, self._props, self._keepalive, self._maxidle, function (err, conn) {
      next(err, conn);
    });
  }, function (err, conns) {
    if (err) {
      return callback(err);
    } else {
      _.each(conns, function (conn) {
        self._pool.push(conn);
      });
      return callback(null);
    }
  });
};

Pool.prototype.initialize = function (callback) {
  var self = this;

  winston.level = this._logging.level;

  // If a drivername is supplied, initialize the via the old method,
  // Class.forName()
  if (this._drivername) {
    java.newInstance(this._drivername, function (err, driver) {
      if (err) {
        return callback(err);
      } else {
        dm.registerDriver(driver, function (err) {
          if (err) {
            return callback(err);
          }
          self._addConnectionsOnInitialize(callback);
        });
      }
    });
  } else {
    self._addConnectionsOnInitialize(callback);
  }

  jinst.events.emit('initialized');
};

Pool.prototype.reserve = function (callback) {
  var self = this;
  var conn = null;
  self._closeIdleConnections();

  // 변경
  shuffle(self._pool);
  var conns = self._pool.concat(self._reserved);
  var times = this._minpoolsize - conns.length;
  if (times > 0) {
    // console.log(`minpoolsize maintain times: ${times}`);
    for (let i = 0; i < times; i++) {
      self._pool.unshift(addConnectionSync(self._url, self._props, self._keepalive, self._maxidle));
    }
  }

  // 변경
  // console.log(`pool.js pool(P): ${self._reserved.length} / ${self._pool.length}`);
  if (self._pool.length > 0) {
    conn = self._pool.shift();

    // 변경
    // if (conn.lastIdle) {
    //   conn.lastIdle = new Date().getTime();
    // }

    self._reserved.unshift(conn);
  } else if (self._reserved.length < self._maxpoolsize) {
    try {
      conn = addConnectionSync(self._url, self._props, self._keepalive, self._maxidle);
      self._reserved.unshift(conn);
    } catch (err) {
      winston.error(err);
      conn = null;
      return callback(err);
    }
  }

  // 변경
  // console.log(`pool.js pool(R): ${self._reserved.length} / ${self._pool.length}`);

  if (conn === null) {
    callback(new Error("No more pool connections available"));
  } else {
    callback(null, conn);
  }
};

Pool.prototype._closeIdleConnections = function () {
  if (!this._maxidle) {
    return;
  }

  var self = this;

  closeIdleConnectionsInArray(self._pool, this._maxidle);

  // 변경
  // closeIdleConnectionsInArray(self._reserved, this._maxidle);
};

function closeIdleConnectionsInArray(array, maxIdle) {
  var time = new Date().getTime();
  var maxLastIdle = time - maxIdle;

  for (var i = array.length - 1; i >= 0; i--) {
    var conn = array[i];
    if (typeof conn === 'object' && conn.conn !== null) {
      // console.log(`conn.lastIdle: ${conn.lastIdle}(${maxLastIdle})`);
      if (conn.lastIdle < maxLastIdle) {
        conn.conn.close(function (err) {
          if(err) {
            console.error('closeIdleConnectionsInArray: ', err);
            throw err;
          }
        });
        array.splice(i, 1);
      }
    }
  }
}

Pool.prototype.release = function (conn, callback) {
  var self = this;
  if (typeof conn === 'object') {
    var uuid = conn.uuid;
    self._reserved = _.reject(self._reserved, function (conn) {
      return conn.uuid === uuid;
    });

    // 변경
    // if (conn.lastIdle) {
    //   conn.lastIdle = new Date().getTime();
    // }

    self._pool.unshift(conn);
    return callback(null);
  } else {
    return callback(new Error("INVALID CONNECTION"));
  }
};

Pool.prototype.purge = function (callback) {
  var self = this;
  var conns = self._pool.concat(self._reserved);

  asyncjs.each(conns,
    function (conn, done) {
      if (typeof conn === 'object' && conn.conn !== null) {
        conn.conn.close(function (err) {
          //we don't want to prevent other connections from being closed
          done();
        });
      } else {
        done();
      }
    },
    function () {
      self._pool = [];
      self._reserved = [];

      callback();
    }
  );
};

function shuffle(array) {
  for (
    let index = array.length - 1; index > 0; index--) {
    // 무작위 index 값을 만든다. (0 이상의 배열 길이 값)
    const randomPosition = Math.floor(Math.random() * (index + 1));
    // 임시로 원본 값을 저장하고, randomPosition을 사용해 배열 요소를 섞는다.
    const temporary = array[index];
    array[index] = array[randomPosition];
    array[randomPosition] = temporary;
  }
}


module.exports = Pool;
