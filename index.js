const csv = require("fast-csv");
const aws = require('aws-sdk');
const config = require('./config');
const mysql = require('mysql');
const s3 = new aws.S3({
  "accessKeyId" : config.access_key_id,
  "secretAccessKey": config.secret_access_key,
});

exports.handler = (event, context, callback) => {
  /* Function definitions begin. */
  function csvToArray(fileName) {
    return new Promise((resolve, reject) => {
      console.log("In csvtoarray");
      console.log('Bucket : ', config.bucket_name);
      console.log('filename  : ', fileName);
    let array = [];
    let s3Stream = s3.getObject({ Bucket: config.bucket_name, Key: fileName }).createReadStream();

    let csvStream = csv()
    .on("data", (data) => {
      array.push(data);
    })
    .on("end", () => {
      console.log("Finished receiving stream data");
      resolve(array);
    });
      s3Stream.pipe(csvStream);
    });
  }

  function dbOps(data) {
    return new Promise((resolve, reject) => {
      console.log("In dbops");
      var con = mysql.createConnection({
        host: config.mysql.host,
        user: config.mysql.user,
        password: config.mysql.password,
        database: config.mysql.database,
      });

      con.connect(function(err) {
        if(err) throw err;
        var table = config.mysql.table;
        var inserted = data.inserted.join();
        var updated = data.updated.join();
        var sql_values = [];
        for (var i = 0; i < data.inserted.length; i++) {
          data.inserted[i].shift();
          data.inserted[i].pop();
          sql_values.push('("' + data.inserted[i][0] + '","' + data.inserted[i][1] + '")');
        }
        sql_values = sql_values.join();
        console.log(sql_values);
        var insert_sql = "INSERT INTO " + table + " (sap,email) VALUES" + sql_values;
        con.query(insert_sql, function(err, result){
          if (err) throw err;
          console.log('Insertion complete.')
        });
/*
UPDATE table_users
    SET cod_user = (case when user_role = 'student' then '622057'
                         when user_role = 'assistant' then '2913659'
                         when user_role = 'admin' then '6160230'
                    end),
        date = '12082014'
    WHERE user_role in ('student', 'assistant', 'admin') AND
          cod_office = '17389551';

*/
        // sql_values = [];
        // for (var i = 0; i < data.updated.length; i++) {
        //   data.updated[i].shift();
        //   data.updated[i].pop();
        //   sql_values.push('("' + data.updated[i][0] + '","' + data.updated[i][1] + '")');
        // }
        // sql_values = sql_values.join();
        // var update_sql = "UPDATE " + table + " SET email=(CASE WHEN ))" + sql_values;
        // con.query(update_sql, function(err, result){
        //   if (err) throw err;
        //   console.log('Insertion complete.')
        // });
        // con.query(update_sql, function(err, result){
          // if (err) throw err;
          // console.log('Updation complete.')
        // });

        con.end();
      });
    });
  }

  /*
    arr1 is array version of old csv.
    arr2 is array version of new csv.
  */
  function compareArray(old_csv_array, new_csv_array) {
    return new Promise((resolve, reject) => {
      let new_csv_length = new_csv_array.length;
      let old_csv_length = old_csv_array.length;
      let row_length = new_csv_array[0].length;

      let arr = {
        "updated": [],
        "inserted": [],
      };
      let index = 1;
      let id_column = 0;

      // Loop through rows of new csv.
      for (; index < new_csv_length; index++) {
        // Loop through rows of old csv.
        let flag = 0;
        for (let k=0; k < old_csv_length; k++) {
          // Check if id of new csv column = id of old csv column
          if (new_csv_array[index][id_column] == old_csv_array[k][id_column]) {
            // Same row found. Check if updation has occured in any column.
            flag = 1;
            for(let j=0; j<row_length; j++) {
              if (new_csv_array[index][j] != old_csv_array[index][j]) {
                arr.updated.push(new_csv_array[index]);
                // console.log("updated");
                flag = 2;
                break;
              }
            }
          }

          if (flag == 2) {
            break;
          }
        }

        if (flag == 0) {
          // console.log("Inserted");
          arr.inserted.push(new_csv_array[index]);
        }
      }
      resolve(arr);
      // console.log("completed : compareArray");
    });
  }

  /* Function definitions end */

  // let file1 = './a.csv';
  // let file2 = './b.csv';
  // console.log(JSON.stringify(event));
  let file1 = event.Records[0].s3.object.key;
  let file2 = 'template-sap-user-data/b.csv';

  let file1Array = [];
  let file2Array = [];

  csvToArray(file1).then((result) => {
    file1Array = result;
    return csvToArray(file2);
  }).then((result) => {
    file2Array = result;
    return compareArray(file1Array, file2Array);
  }).then((result) => {
    return dbOps(result);
  }).catch((err) => {
    console.log('Error : ',err);
  });

  callback(null, 'Hello from Lambda');
};

