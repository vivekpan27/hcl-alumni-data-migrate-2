const csv=require('csvtojson'),
aws = require('aws-sdk'),
config = require('./config'),
hash = require('object-hash'),
async = require('async'),
JsonToCsvParser = require('json2csv').Parser,
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
  console.log("Before mysql initialization");
  const con = mysql.createConnection({
    host: config.mysql_config.host,
    user: config.mysql_config.user,
    password: config.mysql_config.password,
    database: config.mysql_config.database,
  });

  // removing promise
  let insertDataIntoTable = function(bucket, key, tableName, callback) {
    // Insert data into Table from S3 Bucket
    console.log("IN insertDataIntoTable");
    let s3ObjectPath = 's3://' + bucket + '/' + key;
    console.log(s3ObjectPath);
    let query = `LOAD DATA FROM S3 '${s3ObjectPath}' INTO TABLE ${tableName} FIELDS TERMINATED BY ','`;
    //let query = `LOAD DATA FROM S3 '${s3ObjectPath}' INTO TABLE ${tableName} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (${firstline})`;
    console.log(query);
    con.connect(function(err) {
      if(err) throw err;
      con.query(query, function(err, result) {
        if (err) {
          console.log(err);
          callback(`Couldn't insert data into table ${tableName}. Error Message : ${err}`);
        }
        else {
          let query = `DELETE FROM ${tableName} LIMIT 1`;
          con.query(query, err, result, function(err){
            if(err) {
              console.log(err);
              callback(`Couldn't delete first row data from table ${tableName}. Error : ${err}`);
            }
            else {
              callback(null, `Inserted data and deleted first drom from table ${tableName}`);
            }
          });
        }
      });
    });
  };

  console.log("In exports handler");
  insertDataIntoTable(config.bucket_name, 'template-sap-user-data/b1.csv', 'hcl_alumni.users_sap_clone', function(err, result) {
    console.log(result);
  });

  callback(null, 'Hello from Lambda');
};
