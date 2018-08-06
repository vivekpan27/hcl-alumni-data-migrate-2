console.log("Beginnning of lambda 1");
const csv=require('csvtojson'),
aws = require('aws-sdk'),
config = require('./config'),
mysql = require('mysql'),
async = require('async'),
s3 = new aws.S3({
  "accessKeyId" : config.access_key_id,
  "secretAccessKey": config.secret_access_key,
}),
lambda = new aws.Lambda({
  region: 'us-west-2' //change to your region
});

exports.handler = (event, context, callback) => {

console.log('In exports handler of lambda 1');
  /*
  Function is called for old csv file.
  */
  function csvToJsonIdRowMapping(file) {
    let s3Stream = s3.getObject({ Bucket: config.bucket_name, Key: file }).createReadStream();
    let jsonBreakDown = [];
    let count = 0;
    let id = 0;
    let id_column = -1;
    let id_column_value = 'id';
    let tempjsonObj={}, jsonObj = [];
    let rowNumber = 0;
    let params = {
      Body: '',
      Bucket: config.bucket_name,
      Key: '',
    }

    let csvStream = csv()
      .on('header', (header) => {
        id_column = header.indexOf(id_column_value);
      })
      .on('data', (data) => {
        const jsonStr = data.toString('utf8');
        tempjsonObj = JSON.parse(jsonStr);
        jsonObj[tempjsonObj['id']] = rowNumber;

        rowNumber++;
      })
      .on('done', (error)=> {
        params.Body = JSON.stringify(jsonObj, null, 2);
        params.Key = 'json/old_csv_mapping.json';
        s3.putObject(params, function(err, data) {
          if (err) throw err;
        });
      });

    s3Stream.pipe(csvStream);
  }

  /*
  Function is called for new csv file.
  */
  function csvToJsonIdRowMappingAndCsvSplit(file) {
    let s3Stream = s3.getObject({ Bucket: config.bucket_name, Key: file }).createReadStream();
    let jsonBreakDown = [];
    let count = 0;
    let id = 0;
    let id_column = -1;
    let id_column_value = 'id';
    let tempjsonObj={}, jsonObj = [];
    let rowNumber = 0;
    let params = {
      Body: '',
      Bucket: config.bucket_name,
      Key: '',
    }

    let csvStream = csv()
      .on('header', (header) => {
        id_column = header.indexOf(id_column_value);
      })
      .on('data', (data) => {
        const jsonStr = data.toString('utf8');

        tempjsonObj = JSON.parse(jsonStr);
        jsonObj.push({'id' : tempjsonObj['id'], 'row' : rowNumber});

        jsonBreakDown.push(jsonStr);
        if (jsonBreakDown.length === 10000) {
          params.Body = '[' + jsonBreakDown + ']';
          params.Key = 'json/file' + count + '.json';
          s3.putObject(params, function(err, data) {
            if (err) throw err;
          });
          jsonBreakDown = [];
          count++;
        }

        rowNumber++;
      })
      .on('done', (error)=> {
        if (jsonBreakDown.length > 0) {
          params.Body = '[' + jsonBreakDown + ']';
          params.Key = 'json/file' + count + '.json';
          s3.putObject(params, function(err, data) {
            if (err) throw err;
          });
        }


        params.Body = JSON.stringify(jsonObj, null, 2);
        params.Key = 'json/new_csv_mapping.json';
        s3.putObject(params, function(err, data) {
          if (err) throw err;
        });
      });

    s3Stream.pipe(csvStream);
  }
  /* Function definitions end */

  let newCsvFile = event.Records[0].s3.object.key;
  let oldCsvFile = 'template-sap-user-data/b.csv';

  async.parallel([
    function(callback) {
      csvToJsonIdRowMappingAndCsvSplit(newCsvFile);
      console.log('New csv file mapping created.');
      callback();
    },
    function(callback) {
      csvToJsonIdRowMapping(oldCsvFile);
      console.log('Old csv file mapping created.');
      callback();
    },
  ],
  function(err, results) {
    console.log('In callback function for async parallel.');
    var params = {
        FunctionName: 'hcl-alumni-data-migrate-2',
        InvocationType: 'Event',
        Payload: '',
        Qualifier: context.functionVersion,
    };
    lambda.invoke(params, function(err, result) {
        if (err) throw err;
        console.log('Called 2nd lambda function successful');
        callback(null, result.message);
    });
  });

  callback(null, 'Hello from Lambda');
};
