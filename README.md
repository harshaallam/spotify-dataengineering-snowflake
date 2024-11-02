# spotify-dataengineering-snowflake
This project is implemented with Snowflake Datawarehouse at load part, and the rest of the architecuture was same as previous project `spotify-data-analysis-python-aws`.

 ![Architecture](architecture.png)

 ## Changes Made:
 - Code was implemented to pick up the recent file. Before it's execution, all existing files in queue are moved to `to_processed/` from `to_process/`. As processing of older files results in duplicate records. Anyhow, we concern about the top 50 songs, this we can we can acheive it.
 - Changes were made to delete the processed csv files(existing), and only the recent processed files (album,artist,song) will be stored in their respective folders.
 - The load was part was replaced with snowflake, created tables accordingly in the snowflake.
 - Established connection between S3 and Snowflake by building Storage Integration. Later file format, stage, and snowpipe are defined.
 - Created a notification event in S3 bucket's properties and established connetion with snowpipe to notify a new file has arrived and to processed. Snowpipe executes and copies the data into their respective tables.
 - Refer
 
 
