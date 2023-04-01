CREATE OR REPLACE TABLE SF_CONFIRMED_PRD.DW_C_ANALYTICS.WEATHER_FORECAST(
  country VARCHAR NOT NULL,
  region VARCHAR NOT NULL,
  forecast_date date NOT NULL,
  time TIMESTAMP_LTZ(9) NOT NULL,
  latitude NUMBER(12,2),
  longitude NUMBER(12,2),
  localtime_epoch VARCHAR,
  timezone_id VARCHAR,
  is_moon_up NUMBER(12,2),
  is_sun_up NUMBER(12,2),
  moon_illumination NUMBER(12,2),
  moon_phase VARCHAR,
  moonrise VARCHAR,
  moonset VARCHAR,
  sunrise VARCHAR,
  sunset VARCHAR,
  chance_of_rain NUMBER(12,2),
  chance_of_snow NUMBER(12,2),
  cloud NUMBER(12,2),
  dewpoint_c NUMBER(12,2),
  dewpoint_f NUMBER(12,2),
  feelslike_c NUMBER(12,2),
  feelslike_f NUMBER(12,2),
  gust_kph NUMBER(12,2),
  gust_mph NUMBER(12,2),
  heatindex_c NUMBER(12,2),
  heatindex_f NUMBER(12,2),
  humidity NUMBER(12,2),
  is_day NUMBER(12,2),
  precip_in NUMBER(12,2),
  precip_mm NUMBER(12,2),
  pressure_in NUMBER(12,2),
  pressure_mb NUMBER(12,2),
  temp_c NUMBER(12,2),
  temp_f NUMBER(12,2),
  time_epoch VARCHAR,
  uv NUMBER(12,2),
  vis_km NUMBER(12,2),
  vis_miles NUMBER(12,2),
  will_it_rain NUMBER(12,2),
  will_it_snow NUMBER(12,2),
  wind_degree NUMBER(12,2),
  wind_dir VARCHAR,
  wind_kph NUMBER(12,2),
  wind_mph NUMBER(12,2),
  windchill_c NUMBER(12,2),
  windchill_f NUMBER(12,2),
  DW_CREATE_TS TIMESTAMP_LTZ(9),
  DW_LAST_UPDATE_TS	TIMESTAMP_LTZ(9),
  DW_First_Effective_Dt	DATE NOT NULL,
  DW_Last_Effective_Dt	DATE NOT NULL,
  DW_SOURCE_CREATE_NM VARCHAR,
  DW_LOGICAL_DELETE_IND boolean,
  DW_CURRENT_VERSION_IND boolean
);

ALTER TABLE SF_CONFIRMED_PRD.DW_C_ANALYTICS.WEATHER_FORECAST
 ADD PRIMARY KEY (country, region, forecast_date, DW_First_Effective_Dt, DW_Last_Effective_Dt);

CREATE OR REPLACE TASK SF_CONFIRMED_PRD.DW_APPL.RA_WEATHERFORECAST_C_TASK
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 20 1 * * * America/Los_Angeles'
WHEN
    SYSTEM$STREAM_HAS_DATA('SF_REFINED_PRD.DW_APPL.RA_WEATHERFORECAST_FLAT_STREAM')
AS
    CALL SF_CONFIRMED_PRD.DW_APPL.SP_RA_WEATHERFORECAST_FLAT_TO_BIM_LOAD_WEATHER_FORECAST();



-- ALTER TASK SF_CONFIRMED_PRD.DW_APPL.RA_WEATHERFORECAST_C_TASK resume;

-- select *
--   from table(information_schema.task_history(
--     scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
--     result_limit => 10,
--     task_name=>'RA_WEATHERFORECAST_C_TASK'));