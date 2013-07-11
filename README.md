
# multilevel-sharding

``` 
                     /--> DB (A-I)
                    /
multilevel-sharding <---> DB (J-R)
                    \
                     \--> DB (S-Z)
```

## Usage

First **spin up some databases** with
[multilevel](https://github.com/juliangruber/multilevel):

```bash
$ multilevel --port=5001 /db/one
$ multilevel --port=5002 /db/two
$ multilevel --port=5003 /db/three
```

Then **create a sharding server**:

```js
var sharded = require('multilevel-sharding');
var db = sharded(['localhost:5001', 'localhost:5002', 'localhost:5003']);

db.get('key', function (err, value) {});
db.put('foo', 'bar', function (err) {});
```

And **add or remove nodes**:

```js
db.addNode('localhost:5004', function (err) {});
db.removeNode('localhost:5003', function (err) {});
```

Finally expose those management functions over an **http interface**:

```js
var http = require('http');
http.createServer(function (req, res) {
  db.handle(req, res);
}).listen(6000);
```

and **manage**:

```bash
$ # get a list of nodes
$ curl http://localhost:6000/nodes/
$ # add a node
$ curl -XPOST -d"localhost:5004" http://localhost:6000/nodes/
$ # remove a node
$ curl -XDELETE http://localhost:6000/nodes/localhost:5004
```

## License

MIT
