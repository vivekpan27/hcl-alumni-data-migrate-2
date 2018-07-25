const csv=require('csvtojson'),
aws = require('aws-sdk'),
config = require('./config'),
hash = require('object-hash');
async = require('async'),
mysql = require('mysql')
fs = require('fs');

fs.readFile('../hcl-alumni-data-migrate/b.csv', function(err, data) {
  console.log(data.toString());

  csv()
    .fromString(data)
    .then((obj) => {
      console.log(obj);
    });
});
