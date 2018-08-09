console.log('In lambda 2');
console.log('csvtojson');
const csv=require('csvtojson');
console.log('awssdk');
aws = require('aws-sdk');
console.log('config');
config = require('./config');
console.log('hash');
hash = require('object-hash');
console.log('async');
async = require('async');
// JsonToCsvParser = require('json2csv').Parser,
console.log('fastcsv');
fastCsv = require('fast-csv');
console.log('mysql');
mysql = require('mysql');
// fs = require('fs'),
console.log('s3 var');

// From Here

const s3 = new aws.S3({
  "accessKeyId" : config.access_key_id,
  "secretAccessKey": config.secret_access_key,
});

// const con = mysql.createConnection({
//   host: config.mysql.host,
//   user: config.mysql.user,
//   password: config.mysql.password,
//   database: config.mysql.database,
// });

exports.handler = (event, context, callback) => {
  console.log('In exports handler of lambda 2');
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
        // Get the split json file values.
        jsonFileData = data.Body.toString();
        jsonFileData = JSON.parse(jsonFileData);
        console.log('oldcsvmappingjson ', oldCsvMappingJson[1]);
        // Loop through all the elements in the new file.
        for (var i = jsonFileData.length - 1; i >= 0; i--) {
          // Check if 'id' of new_csv json file exists in the mapping file of old csv.
          if(jsonFileData[i].id in oldCsvMappingJson) {
            // Get the row number of that id in the old csv file.
            oldCsvRowVal = oldCsvMappingJson[jsonFileData[i].id];
            // Get the row details in the old file.
            oldCsvRow = oldCsvJson[oldCsvRowVal];
            // Get the hash value of the row in the old file.
            oldCsvRowHash = hash(oldCsvRow);
            // Get the hash value of the row in the new file.
            newCsvRowHash = hash(jsonFileData[i]);
            // If hashes not equal, that means data is updated.
            if (oldCsvRowHash != newCsvRowHash) {
              oldCsvJson[oldCsvRowVal] = jsonFileData[i];
              // newUpdatedData.push(jsonFileData[i]);
              console.log('Updated!');
            }
          }
          else {
            // If id is not present in old csv file, that means insertion has occured.
            newInsertedData.push(jsonFileData[i]);
            console.log("Inserted", jsonFileData[i]);
          }
        }

        console.log('Returning from performops for file ', file);

        return done(null, oldCsvJson);
      });
    }

    var oldCsvFileKey = '';
    async.series([
      function(callback) {
        s3.listObjectsV2({Bucket: config.bucket_name, Prefix: 'template-sap-user-data/'}, function(err, template_folder) {
          var template_folder_files = template_folder.Contents;
          console.log('template_folder_files', template_folder_files);
          var temp = [];

          // Sort the template files according to timestamp and get last updated file.
          // temp[0] will be the last updated file.
          for (var i = 0; i < template_folder_files.length; i++) {
            date = template_folder_files[i].LastModified;
            timestamp = Date.parse(date);
            temp.push({ 'key': i, 'timestamp': timestamp });
          }

          temp.sort(function(x, y) {
            return y.timestamp - x.timestamp;
          });
          console.log('temp ', temp);
          oldCsvFileKey = template_folder_files[temp[0].key].Key;
          console.log('Old csv file key', oldCsvFileKey);

          callback();
        });
      },
      function(callback) {
        s3.getObject({ Bucket: config.bucket_name, Key: oldCsvFileKey }, function(err, data) {
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
          console.log('In callback function for fetching old csv mapping json');
          if (err) throw err;
          console.log('Old csv mapping file fetched');
          oldCsvMappingJson = JSON.parse(data.Body.toString());
          console.log('Old csv mapping file body read');
          // callback(null, oldCsvJson, oldCsvMappingJson);
          callback(null);
        });
      },
      function(callback) {
        async.mapLimit(splitJsonFileKeys, 5, performOps, function(err, updatedExistingJsonData) {
          console.log('In callback for async.maplimit');
          if (err) throw err;
          console.log('Length ', updatedExistingJsonData.length);
          console.log(Object.keys(updatedExistingJsonData));
          for (var i = 1; i <= updatedExistingJsonData.length - 1;  i++) {
            console.log('Length of  updatedExistingJsonData[i]', updatedExistingJsonData[i].length);
            updatedExistingJsonData[0].concat(updatedExistingJsonData[i]);
          }
          // console.log('new inserted data', newInsertedData);
          // console.log('new updated data', newUpdatedData);
          let dbData = {};
          dbData.insert = newInsertedData;
          dbData.update = updatedExistingJsonData[0];
          dbData.complete = dbData.update.concat(dbData.insert);

          console.log("Finished calculatin dbData.complete");

          var db_insert_values_array = Object.keys(dbData.complete).map(function(key) {
            return [dbData.complete[key].id, dbData.complete[key].sap, dbData.complete[key].email];
          });
          console.log("Before connect");

          //Mysql insert
          var con = mysql.createConnection({
            host: config.mysql.host,
            user: config.mysql.user,
            password: config.mysql.password,
            database: config.mysql.database,
          });

          con.connect(function(err) {
            if(err) throw err;
            console.log('Connection established to database.');
            var table = config.mysql.table;

            // First delete existing records from table.
            console.log("In delete function");
            var sql = "DELETE FROM " + table;
            con.query(sql, function(err, result) {
              if (err) throw error;
              console.log(table + ' table truncated!');
              console.log("Number of records deleted: " + result.affectedRows);

              // Then insert complete data into table.
              console.log("In connect function");
              var sql = "INSERT INTO " + table + " (id, sap, email) VALUES ?";

              con.query(sql, [db_insert_values_array], function(err, result){
                if (err) throw err;
                console.log('Insertion complete : ' + result.affectedRows);
                fastCsv.writeToString(dbData.complete, {headers: true}, function(err, csvData) {
                  if (err) throw err;

                  s3.upload({
                    Bucket: config.bucket_name,
                    Key: 'template-sap-user-data/FullData_' + Math.round(new Date().getTime() / 100) + '.csv',
                    Body: csvData,
                    Metadata: {'Content-Type': 'text/csv'}
                  },
                  function(err, doc) {
                    if (err) throw err;
                    console.log('New csv containing updated data saved ', doc);
                  });
                });

                con.end();
              });
            });
          });
          callback(null);
        });
      }
    ]);

  });

  callback(null, 'Hello from Lambda');
};
