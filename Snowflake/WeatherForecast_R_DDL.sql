-- DDL

--varaint
CREATE OR REPLACE TABLE SF_REFINED_PRD.DW_R_ANALYTICS.RA_WEATHERFORECAST
(
FILENAME VARCHAR(2000),
SRC_JSON VARIANT,
CREATED_TS TIMESTAMP DEFAULT to_timestamp_ltz(CURRENT_TIMESTAMP()) 
);

--select * from SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast;

-- variant stream
CREATE OR REPLACE STREAM SF_REFINED_PRD.DW_APPL.RA_WeatherForecast_R_STREAM ON table  SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast;

--select * from SF_REFINED_PRD.DW_APPL.RA_WeatherForecast_R_STREAM;

-- flat 
CREATE OR REPLACE TABLE SF_REFINED_PRD.DW_R_ANALYTICS.RA_WeatherForecast_FLAT(
  filename VARCHAR,
  source VARCHAR,
  country VARCHAR,
  region VARCHAR,
  latitude VARCHAR,
  longitude VARCHAR,
  local_time VARCHAR,
  localtime_epoch VARCHAR,
  timezone_id VARCHAR,
  is_moon_up VARCHAR,
  is_sun_up VARCHAR,
  moon_illumination VARCHAR,
  moon_phase VARCHAR,
  moonrise VARCHAR,
  moonset VARCHAR,
  sunrise VARCHAR,
  sunset VARCHAR,
  chance_of_rain VARCHAR,
  chance_of_snow VARCHAR,
  cloud VARCHAR,
  dewpoint_c VARCHAR,
  dewpoint_f VARCHAR,
  feelslike_c VARCHAR,
  feelslike_f VARCHAR,
  gust_kph VARCHAR,
  gust_mph VARCHAR,
  heatindex_c VARCHAR,
  heatindex_f VARCHAR,
  humidity VARCHAR,
  is_day VARCHAR,
  precip_in VARCHAR,
  precip_mm VARCHAR,
  pressure_in VARCHAR,
  pressure_mb VARCHAR,
  temp_c VARCHAR,
  temp_f VARCHAR,
  time VARCHAR,
  time_epoch VARCHAR,
  uv VARCHAR,
  vis_km VARCHAR,
  vis_miles VARCHAR,
  will_it_rain VARCHAR,
  will_it_snow VARCHAR,
  wind_degree VARCHAR,
  wind_dir VARCHAR,
  wind_kph VARCHAR,
  wind_mph VARCHAR,
  windchill_c VARCHAR,
  windchill_f VARCHAR,
  DW_CREATETS TIMESTAMP_LTZ(9)
); 

--select * from SF_REFINED_PRD.DW_R_ANALYTICS.RA_WEATHERFORECAST_FLAT;

-- flat stream
CREATE OR REPLACE STREAM SF_REFINED_PRD.DW_APPL.RA_WEATHERFORECAST_FLAT_STREAM ON table SF_REFINED_PRD.DW_R_ANALYTICS.RA_WEATHERFORECAST_FLAT;

--select * from SF_REFINED_PRD.DW_APPL.RA_WEATHERFORECAST_FLAT_STREAM;

-- R Task

CREATE OR REPLACE TASK SF_REFINED_PRD.DW_APPL.RA_WEATHERFORECAST_R_TASK
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CRON 0 1 * * * America/Los_Angeles'
  WHEN
    SYSTEM$STREAM_HAS_DATA('SF_REFINED_PRD.DW_APPL.RA_WeatherForecast_R_STREAM')
    AS
  CALL SF_REFINED_PRD.DW_APPL.SP_RA_WEATHERFORECAST_TO_FLAT_LOAD();



-- ALTER TASK SF_REFINED_PRD.DW_APPL.RA_WEATHERFORECAST_R_TASK resume;

-- select *
--   from table(information_schema.task_history(
--     scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
--     result_limit => 10,
--     task_name=>'RA_WEATHERFORECAST_R_TASK'));