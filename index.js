// jshint esnext:true
// jshint node:true
'use strict';
const AWS = require('aws-sdk');
const path = require('path');
const rimraf = require('rimraf');
const fs = require('fs');
const EventEmitter = require('events').EventEmitter;

exports.createClient = function(options) {
    return new Client(options);
};

function Client(options) {
    options = options || {};
    this.s3 = new AWS.S3(options.s3Options);

    this.s3RetryCount = options.s3RetryCount || 3;
    this.s3RetryDelay = options.s3RetryDelay || 1000;
    console.log('Client Created');
}

Client.prototype.downloadDir = function(params) {

    return downloadDir(this, params);
};


function downloadDir(self, params) {
    //if folder exists delete it 
    var downloader = new EventEmitter();
    var keys;
    if (params.localDir === undefined || params.localDir === null) {
        downloader.emit('error', 'localDir cannot be empty');
        return downloader;
    }

    if (params.s3Params === undefined || params.s3Params === null) {
        downloader.emit('error', 's3Params cannot be empty');
        return downloader;
    }

    try {
        initDirectorySync(params.localDir);
    } catch (err) {
        if (err) {
            downloader.emit('error', err);
            return downloader;
        }
    }

    //lets get list items from S3 
    let items = [];
    listKeys(self, params.s3Params, function(error, keys) {
        console.log('Error = ' + JSON.stringify(error));
        if (error) {
            downloader.emit('error', error);
            return downloader;
        }

        items = items.concat(keys);
        items.forEach(function(item) {
            var withoutPrefixKey = item.replace(params.s3Params.Prefix, '');

            //Lets check each item - key
            //if ends in '/' then we have a directory
            //else it is a file and we should get and download
            if (withoutPrefixKey) {
                //console.log(withoutPrefixKey);
                if (withoutPrefixKey.slice(-1) === '/') {
                    //  console.log('Directory');
                    //lets create it 
                    var fullpath = path.join(params.localDir, withoutPrefixKey);
                    fs.mkdirSync(fullpath);
                    console.log('Created Directory ' + fullpath);

                } else {
                    downloadFile(self, params.s3Params.Bucket, item, path.join(params.localDir, withoutPrefixKey), function(err, result) {
                        if (err) {
                            console.log('Cannot create file ' + withoutPrefixKey);
                            console.log(err);
                        } else {
                            console.log('Created file ' + path.join(params.localDir, withoutPrefixKey));
                        }
                    });

                }
            }

        });



        downloader.emit('end');

        return downloader;
    });

}


/**
 * List keys from the specified bucket.
 *
 * If providing a prefix, only keys matching the prefix will be returned.
 *
 * If providing a delimiter, then a set of distinct path segments will be
 * returned from the keys to be listed. This is a way of listing "folders"
 * present given the keys that are there.
 *
 * @param {Object} options
 * @param {String} options.bucket - The bucket name.
 * @param {String} [options.prefix] - If set only return keys beginning with
 *   the prefix value.
 * @param {String} [options.delimiter] - If set return a list of distinct
 *   folders based on splitting keys by the delimiter.
 * @param {Function} callback - Callback of the form function (error, string[]).
 */
function listKeys(self, options, callback) {
    var keys = [];

    console.log('Options are ' + JSON.stringify(options));

    /**
     * Recursively list keys.
     *
     * @param {String|undefined} marker - A value provided by the S3 API
     *   to enable paging of large lists of keys. The result set requested
     *   starts from the marker. If not provided, then the list starts
     *   from the first key.
     */
    function listKeysRecusively(marker) {
        options.marker = marker;

        listKeyPage(
            self,
            options,
            function(error, nextMarker, keyset) {
                if (error) {
                    return callback(error, keys);
                }

                keys = keys.concat(keyset);

                if (nextMarker) {
                    listKeysRecusively(nextMarker);
                } else {
                    callback(null, keys);
                }
            }
        );
    }

    // Start the recursive listing at the beginning, with no marker.
    listKeysRecusively();
}

/**
     * List one page of a set of keys from the specified bucket.
     *
     * If providing a prefix, only keys matching the prefix will be returned.
     *
     * If providing a delimiter, then a set of distinct path segments will be
     * returned from the keys to be listed. This is a way of listing "folders"
     * present given the keys that are there.
     *
     * If providing a marker, list a page of keys starting from the marker
     * position. Otherwise return the first page of keys.
     *
     * @param {Object} options
     * @param {String} options.bucket - The bucket name.
     * @param {String} [options.prefix] - If set only return keys beginning with
     *   the prefix value.
     * @param {String} [options.delimiter] - If set return a list of distinct
     *   folders based on splitting keys by the delimiter.
     * @param {String} [options.marker] - If set the list only a paged set of keys
     *   starting from the marker.
     * @param {Function} callback - Callback of the form
        function (error, nextMarker, keys).
     */
function listKeyPage(self, options, callback) { // jshint ignore:line
    console.log('Options in listKeyPage are :' + JSON.stringify(options));
    var params = {
        Bucket: options.Bucket,
        Delimiter: options.delimiter,
        Marker: options.marker,
        MaxKeys: 1000,
        Prefix: options.Prefix
    };

    self.s3.listObjects(params, function(error, response) {
        if (error) {
            console.log(error);
            return callback(error);
        } else if (response.err) {
            console.log(response.err);
            return callback(new Error(response.err));
        }

        // Convert the results into an array of key strings, or
        // common prefixes if we're using a delimiter.
        var keys;
        if (options.delimiter) {
            // Note that if you set MaxKeys to 1 you can see some interesting
            // behavior in which the first response has no response.CommonPrefix
            // values, and so we have to skip over that and move on to the
            // next page.
            keys = response.CommonPrefixes.map(function(item) {
                return item.Prefix;
            });
        } else {
            keys = response.Contents.map(function(item) {
                return item.Key;
            });
        }

        // Check to see if there are yet more keys to be obtained, and if so
        // return the marker for use in the next request.
        var nextMarker;
        if (response.IsTruncated) {
            if (options.delimiter) {
                // If specifying a delimiter, the response.NextMarker field exists.
                nextMarker = response.NextMarker;
            } else {
                // For normal listing, there is no response.NextMarker
                // and we must use the last key instead.
                nextMarker = keys[keys.length - 1];
            }
        }
        callback(null, nextMarker, keys);
    });
}

// If directory exists delete it and create from scratch
// If directory doesnot exist, create it
function initDirectorySync(directory) {
    try {
        fs.statSync(directory);
        //if directory exists, remove it 
        cleanDir(directory);
        //create it clean
        fs.mkdirSync(directory);

    } catch (e) {
        fs.mkdirSync(directory);
    }
}


function cleanDir(dir) {
    rimraf.sync(dir, {
        maxBusyTries: 1000
    });
}


function downloadFile(self, bucket, key, localPath, callback) {

    var params = { Bucket: bucket, Key: key };
    var file = fs.createWriteStream(localPath);

    file.on('close', function() {
        callback();
    });

    file.on('error', function(err) {
        callback(err);
    });

    self.s3.getObject(params).createReadStream().pipe(file);
}