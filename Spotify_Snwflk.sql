CREATE OR REPLACE DATABASE spotify_db;
CREATE OR REPLACE SCHEMA spotify_api;

--Create a IAM user to integrate s3 with snowflake, create with external id  as 00000.
-- Once IAM role is created provide it's ARN in 'STORAGE_AWS_ROLE_ARN'
CREATE OR REPLACE STORAGE INTEGRATION spotify_storageinit
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::#:role/snowflake-s3-integration-spotify'
STORAGE_ALLOWED_LOCATIONS=('s3://spotify-data-etl-harsha');

desc storage integration spotify_storageinit;
-- provide 'STORAGE_AWS_EXTERNAL_ID' and STORAGE_AWS_IAM_USER_ARN values that gets from above desc command's output
--in AWS IAM roles Trust relationships section at 'sts:ExternalId' and 'AWS' places respectively

-- this builds the connection but need to create stage to establish the connection to the bucket

CREATE OR REPLACE FILE FORMAT spotify_file
TYPE=CSV
FIELD_DELIMITER=','
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
NULL_IF=('NULL','null')
EMPTY_FIELD_AS_NULL=TRUE;

--create a stage using storage interaction this established the connection to s3, in this way no need to provide credentials
CREATE OR REPLACE STAGE spotify_stage
URL='s3://spotify-data-etl-harsha'
STORAGE_INTEGRATION=spotify_storageinit
FILE_FORMAT=spotify_file;

CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_API.ALBUM_DATA(
album_id STRING PRIMARY KEY,
album_name STRING,
release_date STRING,
album_url STRING,
album_tracks INTEGER
);

CREATE OR REPLACE PIPE album_pipe
AUTO_INGEST=TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_API.ALBUM_DATA
FROM @spotify_stage/transformed_data/album_data/;

DESC PIPE spotify_artist_pipe;
--now in s3bucket's properties create an event notification with SQS Queue and provide above desc commands notitication_channle value in 'Enter SQS Queue ARN'. This builds the pipe and notifies snowflake whenever there is a file in s3 bucket

--Event creation in s3 is created for the entire bucket not just to a folder in a bucket, picking the file and loading to db should be handled when creating and calling the snowpipe, mention the specific folder's path next to the stage to route to the specified file's location.
--multiple snowpipes can have the same notification_channel, so this creates error while creating a second notification event in s3 by using notification_channel(snowpipe) at 'Enter SQS Queue ARN' (notification event in s3 bucket)
-- Here used the same snowpipe and steered the route using the @stage/folder-name while creating snowpipe

--Simillarly do the same for artist_data and song_data  as well
truncate table album_data;
select * from album_data;


CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_API.ARTIST_DATA(
artist_id STRING PRIMARY KEY,
artist_url STRING,
artist_name STRING
);

CREATE OR REPLACE PIPE artist_pipe
AUTO_INGEST=TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_API.ARTIST_DATA
FROM @spotify_stage/transformed_data/artist_data/;

truncate table artist_data;
select * from artist_data;

CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_API.SONG_DATA(
song_id STRING PRIMARY KEY,
song_name STRING,
song_duration INTEGER,
song_popularity INTEGER,
song_added STRING,
song_url STRING,
album_id STRING,
artist_id STRING,
FOREIGN KEY (album_id) REFERENCES album_data (album_id),
FOREIGN KEY (artist_id) REFERENCES artist_data (artist_id)
);

CREATE OR REPLACE PIPE song_pipe
AUTO_INGEST=TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_API.SONG_DATA
FROM @spotify_stage/transformed_data/song_data/;

truncate table song_data;
select * from song_data;

create or replace procedure spotify_stored_prcdr()
returns string not null
language javascript
as
$$
    var res1 = snowflake.execute({sqlText:"truncate table spotify_db.spotify_api.album_data;"})
    var res2 = snowflake.execute({sqlText:"truncate table spotify_db.spotify_api.artist_data;"})
    var res3 = snowflake.execute({sqlText:"truncate table spotify_db.spotify_api.song_data;"})
    return 'Successfully truncated'
$$;

call spotify_stored_prcdr();

--create a task to automate the code in stored procedure by defining the frequency of code execution,
--If top songs list are to be listed for every two hours, then just before few minutes of data extract(cloudwatch's event trigger to pull data from api) this task should be schduled. schedule it accordingly.
create or replace task spotify_task
warehouse='compute_wh'
schedule='1 minute'
as
call spotify_stored_prcdr();

show tasks;
--alter the task to 'resume' to get started with the task, ideally it is in 'suspended' state
alter task spotify_task suspend;

