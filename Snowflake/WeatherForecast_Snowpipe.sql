-- 
-- CREATE DATABASE SF_REFINED_PRD;
-- CREATE SCHEMA SF_REFINED_PRD.DW_R_APPL;
-- CREATE SCHEMA SF_REFINED_PRD.DW_R_STAGE;
-- CREATE SCHEMA SF_REFINED_PRD.DW_R_ANALYTICS;
-- CREATE DATABASE SF_CONFIRMED_PRD;
-- CREATE SCHEMA SF_CONFIRMED_PRD.DW_C_APPL;
-- CREATE SCHEMA SF_CONFIRMED_PRD.DW_C_STAGE;
-- CREATE SCHEMA SF_CONFIRMED_PRD.DW_C_ANALYTICS;
-- CREATE DATABASE SF_REFINED_PRD;

-- Deploying Snowpipe for WeatherForecast 

CREATE STORAGE INTEGRATION WeatherForecast_S3_Strorage_Integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXXXXXXXXXXXX:role/S3SnowflakeAccess'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sample-snowflake', 's3://sample-snowflake/*');

desc INTEGRATION WeatherForecast_S3_Strorage_Integration;
  
  
CREATE OR REPLACE STAGE SF_REFINED_PRD.DW_APPL.WeatherForecast_S3_Stage
URL = 's3://sample-snowflake'
STORAGE_INTEGRATION = WeatherForecast_S3_Strorage_Integration;


ls @WeatherForecast_S3_Stage;

-- SELECT metadata$filename, $1
-- FROM @WeatherForecast_S3_Stage;

CREATE FILE FORMAT SF_REFINED_PRD.DW_APPL.WeatherForecast_json
TYPE = 'JSON'
COMPRESSION = 'AUTO';

CREATE OR REPLACE PIPE SF_REFINED_PRD.DW_APPL.WeatherForecast_S3_Pipe
AUTO_INGEST = TRUE
AS
COPY INTO SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast(filename, src_json)
FROM (
  SELECT metadata$filename, $1
  FROM @SF_REFINED_PRD.DW_APPL.WeatherForecast_S3_Stage/
)
FILE_FORMAT = (FORMAT_NAME = 'WeatherForecast_json')
ON_ERROR = 'SKIP_FILE';

show pipes;

-- CREATE OR REPLACE TABLE SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast
-- (
-- FILENAME VARCHAR(2000),
-- SRC_JSON VARIANT,
-- CREATED_TS TIMESTAMP DEFAULT to_timestamp_ltz(CURRENT_TIMESTAMP()) 
-- );

alter pipe SF_REFINED_PRD.DW_APPL.WeatherForecast_S3_Pipe refresh;

select * from SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast;