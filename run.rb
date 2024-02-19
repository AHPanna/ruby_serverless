require 'aws-sdk-s3'
require 'aws-sdk-rds'
require 'csv'

def lambda_handler(event:, context:)
  # AWS RDS MySQL database connection configuration
  db_host = 'your_db_host'
  db_username = 'your_db_username'
  db_password = 'your_db_password'
  db_name = 'your_db_name'

  # S3 bucket and file details
  bucket_name = 'your_bucket_name'
  file_key = event['Records'][0]['s3']['object']['key']

  # Initialize an S3 client
  s3_client = Aws::S3::Client.new(region: 'us-east-1') # Change region as needed

  # Download the CSV file from S3
  file_contents = s3_client.get_object(bucket: bucket_name, key: file_key).body.read

  # Establish a connection to the AWS RDS MySQL database
  client = Mysql2::Client.new(
    host: db_host,
    username: db_username,
    password: db_password,
    database: db_name
  )

  # Read data from the CSV file and insert it into the database
  CSV.parse(file_contents, headers: true) do |row|
    column1_value = row['column1']
    column2_value = row['column2']
    # Add more columns as needed

    # Construct the SQL query to insert data into the table
    sql_query = "INSERT INTO your_table_name (column1, column2) VALUES ('#{column1_value}', '#{column2_value}')"
    # Adjust the query according to your table structure

    # Execute the SQL query
    client.query(sql_query)
  end

  # Close the database connection
  client.close

  { statusCode: 200, body: 'Data inserted successfully!' }
end
