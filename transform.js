/**
 * Download wiki dump from https://dumps.wikimedia.org/other/cirrussearch/current/
 */

var fs = require("fs");
var ndjson = require("ndjson");
var through2 = require("through2");
var elasticsearch = require('elasticsearch');
var zlib = require('zlib');

var gunzip = zlib.createGunzip();
var es = new elasticsearch.Client({
	host: 'http://localhost:9200'
});

var file = 'enwiki-20170501-cirrussearch-content.json.gz';
var total = 0;
var batch = [];
fs.createReadStream(file)
	.pipe(gunzip)
	.pipe(ndjson.parse())
	.pipe(through2({objectMode: true},
		function transform(obj, enc, done) {
			
			if (obj.index) {
				obj.index._index = 'item2';
			} else {
				obj = {content: obj.text};
				total++;
			}
			
			batch.push(obj);
			
			if (batch.length >= 1000) {
				es.bulk({body: batch}, function (err, results) {
					console.log(total);
					done()
				});
				batch = [];
			} else {
				done();
			}
		}, function flush(done) {
			if (batch.length) {
				es.bulk({body: batch}, function (err, results) {
					console.log(total);
					done();
				});
			}
		}));
