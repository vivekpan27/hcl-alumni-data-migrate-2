console.log('At beginning');
const aws = require('aws-sdk');
const config = require('./config');
const s3 = new aws.S3({
  "accessKeyId" : config.access_key_id,
  "secretAccessKey": config.secret_access_key,
});


exports.handler = (event, context, callback) => {

  var params = {Bucket: config.bucket_name, Key: 'newCreate/', Body: 'sdasasdadasd'};

  s3.deleteObject({Bucket: config.bucket_name, Key: 'newCreate/'}, function(err, data) {
    if (err) throw err;
    s3.upload(params, function(err ,data) {
      console.log(data);
      if (err) {
        console.log('Error creating folder', err);
      }
      else {
        console.log('Successfully created folder on s3.');
      }
    });
  });

  callback(null, 'Hello from Lambda');
};
