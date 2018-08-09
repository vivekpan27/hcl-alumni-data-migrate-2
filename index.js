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
  function csvToJsonIdRowMapping(file, callback) {
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
          callback();
        });
      });

    s3Stream.pipe(csvStream);
  }


  function saveToS3(arr, key, callback) {
    s3.putObject({Bucket: config.bucket_name, Body: arr, Key: 'json/file' + key + '.json'}, function(err, data) {
      if (err) throw err;
      callback();
    });
  }

  /*
  Function is called for new csv file.
  */
  function csvToJsonIdRowMappingAndCsvSplit(file, callback) {
    let s3Stream = s3.getObject({ Bucket: config.bucket_name, Key: file }).createReadStream();
    let jsonBreakDown = [];
    let jsonBreakDownComplete = [];
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

        // if (jsonBreakDown.length === 10000) {
        //   params.Body = '[' + jsonBreakDown + ']';
        //   params.Key = 'json/file' + count + '.json';
        //   s3.putObject(params, function(err, data) {
        //     if (err) throw err;
        //   });
        //   jsonBreakDown = [];
        //   count++;
        // }
        if (jsonBreakDown.length === 10000) {
          jsonBreakDownComplete[count] = '[' + jsonBreakDown + ']';
          // params.Body = '[' + jsonBreakDown + ']';
          // params.Key = 'json/file' + count + '.json';
          // s3.putObject(params, function(err, data) {
            // if (err) throw err;
          // });
          jsonBreakDown = [];
          count++;
        }

        rowNumber++;
      })
      .on('done', (error)=> {
        if (jsonBreakDown.length > 0) {
          jsonBreakDownComplete[count] = '[' + jsonBreakDown + ']';
          // params.Body = '[' + jsonBreakDown + ']';
          // params.Key = 'json/file' + count + '.json';
          // s3.putObject(params, function(err, data) {
            // if (err) throw err;
          // });
        }

        // Save the new json files split into individual files of size 10,000 rows.
        async.eachOfLimit(jsonBreakDownComplete, 5, saveToS3, function(err) {
          if (err) throw err;

          params.Body = JSON.stringify(jsonObj, null, 2);
          params.Key = 'json/new_csv_mapping.json';
          s3.putObject(params, function(err, data) {
            if (err) throw err;
            callback();
          });
        });

      });

    s3Stream.pipe(csvStream);
  }

  /*
  Function is called for creating Json objects.
  */
  function callMappingFunctions() {
    async.parallel([
      function(callback) {
        csvToJsonIdRowMappingAndCsvSplit(newCsvFile, function(err) {
          if (err) throw err;
          console.log('New csv file mapping created.');
          callback();
        });
      },
      function(callback) {
        csvToJsonIdRowMapping(oldCsvFile, function(err){
          if (err) throw err;
          console.log('Old csv file mapping created.');
          callback();
        });
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
          console.log('Called 2nd lambda function successful. REsult', result);
          // callback(null, result.message);
        });
      }
    );
  }




  /* Function definitions end */

  let newCsvFile = event.Records[0].s3.object.key;
  console.log('New csv file ', newCsvFile);
  let oldCsvFile = '';

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
    oldCsvFile = template_folder_files[temp[0].key].Key;
    console.log('Old csv file ', oldCsvFile);

    s3.listObjectsV2({Bucket: config.bucket_name, Prefix: 'json/'}, function(err, data) {
      console.log('IN listObjectsV2');
      if (err) throw err;
      jsonFolderContents = data.Contents;

      jsonFolderKeys = [];

      console.log('Before for loop');
      for (var i = 0; i < jsonFolderContents.length; i++) {
        key = jsonFolderContents[i].Key;
        split_key = key.split('/');
        if (!(split_key[1] == '')) {
          jsonFolderKeys.push({Key: jsonFolderContents[i].Key});
        }
      }

      var params1 = {
        Bucket: config.bucket_name, /* required */
        Delete: { /* required */
          Objects: jsonFolderKeys,
        },
      };

      console.log('Before deleteObjects');

      if (jsonFolderKeys.length > 0) {
        s3.deleteObjects(params1, function(err, data) {
          if (err) throw err;
          console.log("Deleted old json folder objects");
          callMappingFunctions();
        });
      }
      else {
        callMappingFunctions();
      }
    });

  });



  callback(null, 'Hello from Lambda');
};
