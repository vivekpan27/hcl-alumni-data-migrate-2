module.exports = {
	bucket_name: process.env.BUCKET_NAME,
	key_prefix: process.env.KEY_PREFIX,
	HOST: process.env.HOST,
	USER: process.env.USER,
  access_key_id: process.env.ACCESS_KEY_ID,
  secret_access_key: process.env.SECRET_ACCESS_KEY,

  mysql : {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    table: process.env.MYSQL_TABLE,
  }

  // mysql : {
  //   host: 'localhost',
  //   user: 'root',
  //   password: '1234',
  //   database: 'hcl_alumni_portal',
  //   table: 'users_sap',
  // }
};
