var fs = require('fs')
  , path = require('path')
  , knox = require('knox');

// interface
var Documents = function() {

}

Documents.prototype.get = function(userId, filename, buffer, cb) {
  throw 'not implemented';
}

Documents.prototype.save = function(userId, filename, buffer, cb) {
  throw 'not implemented';
}

Documents.prototype.remove = function(userId, filename, cb) {
  throw 'not implemented';
}

// filesystem
var FilesystemDocuments = function(location) {
  Documents.apply(this, Array.prototype.slice.call(arguments));
  this.location = location;
  if (!fs.existsSync(this.location)) {
    throw 'Directory does not exist: ' + this.location;
  }
}

FilesystemDocuments.prototype = new Documents();

FilesystemDocuments.prototype.get = function(userId, filename, cb) {
  var p = path.join(this.location, '' + userId, filename);
  fs.readFile(p, cb);
}

FilesystemDocuments.prototype.save = function(userId, filename, buffer, cb) {
  var p = path.join(this.location, '' + userId);

  if (!fs.existsSync(p))
    fs.mkdirSync(p);
  fs.writeFile(path.join(p, filename), buffer, cb);
}

FilesystemDocuments.prototype.remove = function(userId, filename, cb) {
  var p = path.join(this.location, '' + userId, filename);
  if (fs.existsSync(p))
    fs.unlink(p, cb);
  else
    cb();
}

// s3
var S3Documents = function(bucket, key, secret) {
  Documents.apply(this, Array.prototype.slice.call(arguments));
  this.client = knox.createClient({
    key: key,
    secret: secret,
    bucket: bucket
  });
}

S3Documents.prototype = new Documents();

S3Documents.prototype.get = function(userId, filename, callback) {
  var p = path.join('' + userId, filename);

  var chunks = [];

  this.client.get(p).on('response', function(res) {
    res.on('data', function(chunk) {
      chunks.push(chunk);
    });
    res.on('end', function() {
      var buffer = Buffer.concat(chunks);

      if (res.statusCode < 200 || res.statusCode >= 300) {
        callback(buffer.toString());
      } else {
        callback(null, buffer);
      }
    });
    res.on('error', function(err) {
      callback(err);
    });
  }).end();
}

S3Documents.prototype.save = function(userId, filename, buffer, cb) {
  var p = path.join('' + userId, filename);

  var req = this.client.putBuffer(buffer, p, function(err, res) {
    if (200 == res.statusCode) {
      cb();
    } else {
      cb(res);
    }
  }).on('error', function(err) {
    console.error('Error', err);
    cb(err);
  });
}

S3Documents.prototype.remove = function(userId, filename, cb) {
  var p = path.join('' + userId, filename);

  this.client.del(p).on('response', function(res) {
    if (204 == res.statusCode) {
      cb();
    } else {
      cb(res);
    }
  }).on('error', function(err) {
    console.error('Error', err);
    cb(err);
  }).end();
}


module.exports.FilesystemDocuments = FilesystemDocuments;
module.exports.S3Documents = S3Documents;
