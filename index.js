const csv=require('csvtojson');
const aws = require('aws-sdk');
const config = require('./config');
const mysql = require('mysql');
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
  let validKeys = [];
  let oldCsvMapping = '', newCsvMapping = '';

  s3.listObjectsV2(params, function(err, data) {
    if (err) throw err;
    jsonFolderContents = data.Contents;
    // console.log(jsonFolderContents);
    // console.log(jsonFolderContents.length);
    for (var i = jsonFolderContents.length - 1; i >= 0; i--) {

      let key = jsonFolderContents[i].Key;
      let split_key = key.split('/');
      if (!(split_key[1] == '')) {
        if (split_key[1].search(/file/) == 0) {
          validKeys.push(key);
        }
        else if (split_key[1].search(/new/) == 0) {
          newCsvMapping = key;
        }
        else if (split_key[1].search(/old/) == 0) {
          oldCsvMapping = key;
        }
      }
    }
    console.log('Valid ', validKeys);
    console.log('New ', newCsvMapping);
    console.log('Old' , oldCsvMapping);
  });

  callback(null, 'Hello from Lambda');
};
