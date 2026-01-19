# BigQuery CDC demo

architecture: MySQL -> BigQuery

## Overall

all these will be deployed on google cloud platform

we need 3 parts:

1. launch a new mysql database, slowly update a particular table data
2. create the table with same schema in BigQuery for later CDC usage
3. launch a 2 small e2 instances dataflow job, catch the data changes and update to bigquery

## Details

1. project root

- need a config file named conf.yml allows the user to define: 'project id (default to du-hast-mich)', 'region' (default to us-central1), 'mysql instance name' (default to dingomysql), 'bigquery dataset' (defualt to dingocdc), 'bigquery table name' (default to item) under the bigquery dataset, 'mysql db name' (default to dingocdc), 'mysql table name' (defualt to item), 'GCP service account path' (where we load the credentials json file)

- need a makefile to control and launch everything, targets are

init_mysql: use gcloud command that launches the mysql instance with version 8.0 and 'development edition preset' with configured instance id and generate a complex root password, write this password to file named "mysql.password", make it public accessible.  once the database is realy, used a python script to create a database acording to the config file and a table with the schema (id/integer/primary_key, description/string, price/float, created_at/datetime, updated_at/datetime), simply insert 10 records from id 1 to 10 with all random data. 

update_mysql: create a python script, that access the mysql table, read configs from the configuration file for table id, randomly update item 1 to 10, between every 1 or 3 seconds, simply give them a random price/integer, and update the updated_at column with the time updated, keep the program run till we hit control+C, print what has been updated from and to information on the screen.

init_bq: according to the configuration file, check the dataset existance create if not, then check the table existance create it if not with the same schema as mysql

run_cdc: launch a dataflow job with name "dingo_cdc" that reads data changes from MySQL then update the BigQuery in real time.

2. code assets

makefile and configuration file in project root

mysql related should be in a sub-folder called mysql

dataflow code file should be in java and a maven project, org: bindiego, then in a sub-folder called dataflow

bigquery related code should be in a sub-folder named bigquery

### Others 

- create a .gitignore properly
- create a README.md file properly with step by step instructions
- use service account file '~/workspace/google/sa.json' by default
- print useful information on the screen when doing tasks, so we know what is happenning and easy for debug
