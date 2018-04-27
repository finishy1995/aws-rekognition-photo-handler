// dependencies
var async = require('async');
var AWS = require('aws-sdk');
var DataClient = require("./src/dataClient");
var gm = require('gm').subClass({ imageMagick: true }); // Enable ImageMagick integration.

// constants, no need height limit for now.
var MAX_WIDTH  = 240;
// var MAX_HEIGHT = 100;

// get reference to S3 client and Rek client
var s3 = new AWS.S3();
var rekognition = new AWS.Rekognition();
var dataClient = new DataClient();

dataClient.hostsConfig([ '<YOUR_ENDPOINT>' ]);

// Transfer array to string.
function arrayToString(arr) {
	var len = arr.length;
  var tree = "[";
	for (var i = 0; i < len; i++) {
    tree += "[";
    tree += "'" + arr[i][0] + "',";
    tree += "'" + arr[i][1] + "'";
    tree += "]";
    if (i < len - 1) {
      tree += ",";
    }
  }
  tree += "]";
  return tree;
}

function deletePhoto(bucket, key) {
  var params = {
    Bucket: bucket, 
    Key: key
  };
   
  s3.deleteObject(params, function(err, data) {
    if (err) console.error(err, err.stack);
  });
}

function eventCheck(srcBucket, srcKey, dstBucket, callback) {
  // Sanity check: validate that source and destination are different buckets.
  if (srcBucket == dstBucket) {
    callback("Source and destination buckets are the same.");
    return false;
  }

  // Infer the image type.
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    callback("Could not determine the image type.");
    return false;
  }
  var imageType = typeMatch[1];
  if (imageType != "jpg" && imageType != "png" && imageType != "jpeg") {
    callback('Unsupported image type: ' + imageType);
    return false;
  }
  
  return true;
}

function eventHandle(srcBucket, srcKey, dstBucket, dstKey, callback) {
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  var imageType = typeMatch[1];
  var flag = false;
  
  async.parallel([
    function thumbnailMake(cb) {
      // Download the image from S3, transform, and upload to a different S3 bucket.
      async.waterfall([
        function download(next) {
          // Download the image from S3 into a buffer.
          s3.getObject({
            Bucket: srcBucket,
            Key: srcKey
          }, next);
        },
        
        function transform(response, next) {
          gm(response.Body).size(function(err, size) {
            // Infer the scaling factor to avoid stretching the image unnaturally.
            // var scalingFactor = Math.min(MAX_WIDTH / size.width, MAX_HEIGHT / size.height);
            var scalingFactor = MAX_WIDTH / size.width;
            var width  = scalingFactor * size.width;
            var height = scalingFactor * size.height;
    
            // Transform the image buffer in memory.
            this.resize(width, height)
              .toBuffer(imageType, function(err, buffer) {
                if (err) {
                  next(err);
                } else {
                  next(null, response.ContentType, buffer);
                }
            });
          });
        },
        
        function upload(contentType, data, next) {
          // Stream the transformed image to a different S3 bucket.
          s3.putObject({
            Bucket: dstBucket,
            Key: dstKey,
            Body: data,
            ContentType: contentType
          }, next);
        }
      ], function (err, results) {
        if (err) {
          cb(err, null);
        } else {
          flag = true;
          cb(null, results);
        }
      });
    },
  
    function detectLabels(cb) {
      var params = {
        Image: {
          S3Object: {
            Bucket: srcBucket,
            Name: srcKey
          }
        },
        MaxLabels: 20,
        MinConfidence: 60
      };
      
      rekognition.detectLabels(params, function(err, data) {
        if (err) {
          cb(err);
        } else {
          cb(null, data);
        }
      });
    }
  ], function (err, results) {
    if (err) {
      if (flag)
        deletePhoto(dstBucket, dstKey);
      deletePhoto(srcBucket, srcKey);
      
      callback(err);
    } else {
      var labels = {};
      var labelsIndex = [];
      for (var i=0; i<results[1].Labels.length; i++) {
        labels[results[1].Labels[i].Name] = Math.round(results[1].Labels[i].Confidence * 100) / 100;
        labelsIndex.push(results[1].Labels[i].Name);
      }
  
      dataClient.insertPhoto('test', srcKey.replace(/\//g, '-'), {
        photo_key: srcKey,
        thumbnail_key: dstKey,
        tags: labels,
        tagsIndex: labelsIndex
      }, function (error, response) {
        if (error) {
          deletePhoto(dstBucket, dstKey);
          deletePhoto(srcBucket, srcKey);
          
          callback(error);
        }
      });
    }
  });
}

exports.handler = function(event, context, callback) {
  var srcBucket = event.Records[0].s3.bucket.name;
  // Object key may have spaces or unicode non-ASCII characters.
  var srcKey    = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));  
  var dstBucket = srcBucket + "-thumbnail";
  var dstKey    = srcKey;

  if (!eventCheck(srcBucket, srcKey, dstBucket, callback)) {
    deletePhoto(srcBucket, srcKey);
    return;
  }

  eventHandle(srcBucket, srcKey, dstBucket, dstKey, callback);
};
