var mysql = require('mysql');
var config = require('./config');

var con = mysql.createConnection({
  host: config.mysql.host,
  user: config.mysql.user,
  password: config.mysql.password,
  database: config.mysql.database,
});

con.connect(function(err) {
  if(err) throw err;
  var table = 'markers';
  var insert_sql = "INSERT INTO " + table + " VALUES('', 'test1', 'test address', '1212.23', '12312.232')";
  var update_sql = "UPDATE " + table + " SET name = 'asd' where name='test1'";

  con.query(insert_sql, function(err, result){
    if (err) throw err;
    console.log('Insertion complete.')
  });

  con.query(update_sql, function(err, result){
    if (err) throw err;
    console.log('Updation complete.')
  });

  con.end();

});


