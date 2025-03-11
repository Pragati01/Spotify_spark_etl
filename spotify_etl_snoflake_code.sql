create database spotify_db;

--storage intergation is a connection bw snowflake and s3
CREATE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::390844776536:role/spark-snowflake-role' --write your own ARN role which you can get from AWS
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-data-bucket-pr/'); -- write your AWS S3 bucket name

--this is required to get the arn and extrenal id of snowflake for this connection which we need to mention in AWS
DESC STORAGE INTEGRATION s3_int;

--file format gives a type to stage
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
field_delimiter = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '')
empty_field_as_null=TRUE
TRIM_SPACE = TRUE;

--Creating stage: stage is like a storage location where the data is kept before being loaded into the tables.
CREATE OR REPLACE STAGE spotify_s3_stage
STORAGE_INTEGRATION = s3_int
URL = 's3://spotify-data-bucket-pr/transformed_data/'
FILE_FORMAT = my_csv_format;

--to list the folders in the stage
List @spotify_s3_stage;

use database spotify_db;

--createing tables :

CREATE OR REPLACE TABLE tbl_album2 (
    album_id STRING PRIMARY KEY,
    album_name STRING,
    album_release_date DATE,
    duration_ms BIGINT,
    total_tracks BIGINT,
    album_url STRING,
    load_date DATE
);

CREATE OR REPLACE TABLE tbl_artist2 (
    artist_id STRING PRIMARY KEY,
    artist_name STRING NOT NULL,
    artist_url VARCHAR(255),
    load_date DATE
);
--drop table tbl_song2;
CREATE OR REPLACE TABLE tbl_song2 (
    song_id STRING PRIMARY KEY,
    song_name STRING NOT NULL,
    song_duration_ms BIGINT,
    song_url STRING,
    artist_id STRING,
    album_id STRING,
    load_date DATE
);


--Copy command: copy command to get the data from s3 to snowflake tables(this is one time load)

--here we are only testing if the data is properly flowing or not therefore using only 1 file from s3
copy into tbl_song2
from @spotify_s3_stage/song/song_transformed_2025-03-10/run-1741595816833-part-r-00000 ;

copy into tbl_album2
from @spotify_s3_stage/album/album_transformed_2025-03-10/run-1741595808073-part-r-00000 ;


copy into tbl_artist2
from @spotify_s3_stage/artist/artist_transformed_2025-03-10/run-1741595817783-part-r-00000 ;


select * from tbl_song2;

select * from tbl_album2;

select * from tbl_artist2;

--**creating snowpipe:

--now creating pipe:
create or replace pipe tbl_song_pipe
auto_ingest = TRUE
as copy into tbl_song2
from @spotify_s3_stage/song/;--this means that whenever there is a new file uploaded on song folder in s3 , this snowpipe will run and will pull the data into tbl_song table in snowflake


create or replace pipe tbl_album_pipe
auto_ingest = TRUE
as copy into tbl_album2
from @spotify_s3_stage/album/;

create or replace pipe tbl_artist_pipe
auto_ingest = TRUE
as copy into tbl_artist2
from @spotify_s3_stage/artist/;

select * from tbl_artist;

--now we have to use a snowflake channel and put it in s3 so that any change in s3 folder can trigger the movement of file into snowflake via a channel:

desc pipe tbl_song_pipe; --use notification_channel col info 
--arn:aws:sqs:us-east-1:724772060612:sf-snowpipe-AIDA2RP6H7XCHBOL4YTDP-0A8YouCr0nwnptq2zpsTLw
desc pipe tbl_album_pipe;
--arn:aws:sqs:us-east-1:724772060612:sf-snowpipe-AIDA2RP6H7XCHBOL4YTDP-0A8YouCr0nwnptq2zpsTLw
desc pipe tbl_artist_pipe;
--arn:aws:sqs:us-east-1:724772060612:sf-snowpipe-AIDA2RP6H7XCHBOL4YTDP-0A8YouCr0nwnptq2zpsTLw


--testing the connection :
select count(*) from tbl_song2;
select count(*) from tbl_album2;
select count(*) from tbl_artist2; 


select * from tbl_song2;


