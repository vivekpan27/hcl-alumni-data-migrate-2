const csv=require('csvtojson'),
aws = require('aws-sdk'),
config = require('./config'),
hash = require('object-hash'),
async = require('async'),
mysql = require('mysql');
// fs = require('fs'),

// From Here

const s3 = new aws.S3({
  "accessKeyId" : config.access_key_id,
  "secretAccessKey": config.secret_access_key,
});

exports.handler = (event, context, callback) => {

  let params = {
    Bucket: config.bucket_name,
    Prefix: 'json/',
  };

  let jsonFolderContents = {};
  let splitJsonFileKeys = [];
  let oldCsvMappingKey = '', newCsvMappingKey = '';
  let newInsertedData = [];
  let newUpdatedData = [];
  let oldCsvJson = '';
  let oldCsvMappingJson = '';

  s3.listObjectsV2(params, function(err, data) {
    if (err) throw err;
    jsonFolderContents = data.Contents;

    for (var i = jsonFolderContents.length - 1; i >= 0; i--) {

      let key = jsonFolderContents[i].Key;
      let split_key = key.split('/');
      if (!(split_key[1] == '')) {
        if (split_key[1].search(/file/) == 0) {
          splitJsonFileKeys.push(key);
        }
        else if (split_key[1].search(/new/) == 0) {
          newCsvMappingKey = key;
        }
        else if (split_key[1].search(/old/) == 0) {
          oldCsvMappingKey = key;
        }
      }
    }

    console.log('Valid ', splitJsonFileKeys);
    console.log('New ', newCsvMappingKey);
    console.log('Old' , oldCsvMappingKey);

    let performOps = function(file, done) {
      console.log('In performOps for file', file);
      s3.getObject({ Bucket: config.bucket_name, Key: file }, function(err, data) {
        jsonFileData = data.Body.toString();
        jsonFileData = JSON.parse(jsonFileData);
        for (var i = jsonFileData.length - 1; i >= 0; i--) {
        // Check if 'id' of new_csv json file exists in the mapping file of old csv.
          if(jsonFileData[i].id in oldCsvMappingJson) {
            oldCsvRowVal = oldCsvMappingJson[jsonFileData[i].id];
            oldCsvRow = oldCsvJson[oldCsvRowVal];
            oldCsvRowHash = hash(oldCsvRow);
            newCsvRowHash = hash(jsonFileData[i]);

            if (oldCsvRowHash != newCsvRowHash) {
              newUpdatedData.push(jsonFileData[i]);
              console.log('Updated!');
            }
          }
          else {
            newInsertedData.push(jsonFileData[i]);
            console.log("Inserted");
          }
        }

        console.log('Returning from performops for file ', file);
        return done(null);
      });
    }

    async.series([
      function(callback) {
        s3.getObject({ Bucket: config.bucket_name, Key: 'template-sap-user-data/b.csv' }, function(err, data) {
           console.log('Old Csv data object fetched');
           csv()
            .fromString(data.Body.toString())
            .then((json) => {
              oldCsvJson = json;
              console.log('Old Csv Json loaded');
              callback(null);
            });
        });
      },
      function(callback) {
        s3.getObject({ Bucket: config.bucket_name, Key: 'json/old_csv_mapping.json' }, function(err, data){
          console.log('Old csv mapping file fetched');
          oldCsvMappingJson = JSON.parse(data.Body.toString());
          callback(null);
        });
      },
      function(callback) {
        async.eachLimit(splitJsonFileKeys, 5, performOps, function(err) {
          if (err) throw err;
          console.log(newInsertedData);
          console.log(newUpdatedData);
          console.log('Done');
        });
      }
    ]);

  });

  callback(null, 'Hello from Lambda');
};
