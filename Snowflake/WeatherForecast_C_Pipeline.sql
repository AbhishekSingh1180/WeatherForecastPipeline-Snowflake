CREATE OR REPLACE PROCEDURE SF_CONFIRMED_PRD.DW_APPL.SP_RA_WEATHERFORECAST_FLAT_TO_BIM_LOAD_WEATHER_FORECAST()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

// Global Variable Declaration
var cnf_db = "SF_CONFIRMED_PRD";
var dw_prd_schema = "DW_C_ANALYTICS";
var cnf_wrk_schema = "DW_C_STAGE";
var appl_schema = "DW_APPL";
var ref_db = "SF_REFINED_PRD";

var src_tbl = ref_db + "." + appl_schema + ".RA_WEATHERFORECAST_FLAT_STREAM";
var src_rerun_tbl = cnf_db + "." + cnf_wrk_schema + ".RA_WEATHERFORECAST_FLAT_Rerun";
var src_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".RA_WEATHERFORECAST_FLAT_WRK";
var src_tmp_wrk_tbl = cnf_db + "." + cnf_wrk_schema + ".RA_WEATHERFORECAST_FLAT_TMP_WRK";
var tgt_tbl = cnf_db +"."+ dw_prd_schema +".WEATHER_FORECAST";


var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM `+ src_tbl +` where 1=2 `;

try {
    snowflake.execute ({sqlText: sql_crt_rerun_tbl});
    }
catch (err)  {
    throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
    }

    
// persist stream data in work table for the current transaction, includes data from previous failed run

var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as 
                            SELECT * FROM `+ src_tbl +` where METADATA$ACTION = 'INSERT'
                            UNION ALL 
                            SELECT * FROM `+ src_rerun_tbl+``;
try {
    snowflake.execute ({sqlText: sql_crt_src_wrk_tbl  });
    }
catch (err)  {
    throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
    }


// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE `+ src_rerun_tbl + ` `;
try {
    snowflake.execute({sqlText: sql_empty_rerun_tbl  });
    }
catch (err)  {
    throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
    }

// query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl+` AS
                         SELECT * FROM `+ src_wrk_tbl+``;

	
var tmp_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_tmp_wrk_tbl +` AS
                    SELECT 
                        country,
                        region,
                        DATE(time) AS forecast_date,
                        TO_TIMESTAMP_LTZ(time) AS time,
                        latitude::NUMBER(12,2) AS LATITUDE,
                        longitude::NUMBER(12,2) AS LONGITUDE,
                        localtime_epoch,
                        timezone_id,
                        is_moon_up::NUMBER(12,2) AS IS_MOON_UP,
                        is_sun_up::NUMBER(12,2) AS IS_SUN_UP,
                        moon_illumination::NUMBER(12,2) AS MOON_ILLUMINATION,
                        moon_phase,
                        moonrise,
                        moonset,
                        sunrise,
                        sunset,
                        chance_of_rain::NUMBER(12,2) AS CHANCE_OF_RAIN,
                        chance_of_snow::NUMBER(12,2) AS CHANCE_OF_SNOW,
                        cloud::NUMBER(12,2) AS CLOUD,
                        dewpoint_c::NUMBER(12,2) AS DEWPOINT_C,
                        dewpoint_f::NUMBER(12,2) AS DEWPOINT_F,
                        feelslike_c::NUMBER(12,2) AS FEELSLIKE_C,
                        feelslike_f::NUMBER(12,2) AS FEELSLIKE_F,
                        gust_kph::NUMBER(12,2) AS GUST_KPH,
                        gust_mph::NUMBER(12,2) AS GUST_MPH,
                        heatindex_c::NUMBER(12,2) AS HEATINDEX_C,
                        heatindex_f::NUMBER(12,2) AS HEATINDEX_F,
                        humidity::NUMBER(12,2) AS HUMIDITY,
                        is_day::NUMBER(12,2) AS IS_DAY,
                        precip_in::NUMBER(12,2) AS PRECIP_IN,
                        precip_mm::NUMBER(12,2) AS PRECIP_MM,
                        pressure_in::NUMBER(12,2) AS PRESSURE_IN,
                        pressure_mb::NUMBER(12,2) AS PRESSURE_MB,
                        temp_c::NUMBER(12,2) AS TEMP_C,
                        temp_f::NUMBER(12,2) AS TEMP_F,
                        time_epoch,
                        uv::NUMBER(12,2) AS UV,
                        vis_km::NUMBER(12,2) AS VIS_KM,
                        vis_miles::NUMBER(12,2) AS VIS_MILES,
                        will_it_rain::NUMBER(12,2) AS WILL_IT_RAIN,
                        will_it_snow::NUMBER(12,2) AS WILL_IT_SNOW,
                        wind_degree::NUMBER(12,2) AS WIND_DEGREE,
                        wind_dir,
                        wind_kph::NUMBER(12,2) AS WIND_KPH,
                        wind_mph::NUMBER(12,2) AS WIND_MPH,
                        windchill_c::NUMBER(12,2) AS WINDCHILL_C,
                        windchill_f::NUMBER(12,2) AS WINDCHILL_F,
                        CURRENT_TIMESTAMP AS dw_create_ts,
                        NULL AS dw_last_update_ts,
                        CURRENT_DATE AS dw_first_effective_dt,
                        '9999-12-31' AS dw_last_effective_dt,
                        filename AS dw_source_create_nm,
                        0 AS dw_logical_delete_ind,
                        1 AS dw_current_version_ind
                    FROM ` + src_wrk_tbl + `
                    WHERE country IS NOT NULL 
                      AND region IS NOT NULL 
                      AND local_time IS NOT NULL 
                      AND time IS NOT NULL 
                    ORDER BY country, region, forecast_date, time desc `;

   try {
        snowflake.execute({sqlText: tmp_wrk_tbl});
    }
    catch (err)  { 
    snowflake.execute ({sqlText: sql_ins_rerun_tbl});
    throw "Creation of tmp_wrk_tbl table  Failed with error: " + err;   //throw error message.
    }


// SCD type 2 
// Deactivate old record

var sql_updates = `update ` + tgt_tbl + 
                   ` tgt set dw_last_update_ts = current_timestamp,
                        dw_last_effective_dt = current_date,
                        dw_current_version_ind = 0,
                        dw_logical_delete_ind = 1
                    from ( select distinct country, region from ` + src_tmp_wrk_tbl + `) tmp
                    WHERE tgt.dw_last_effective_dt = '9999-12-31'
                    and tgt.dw_current_version_ind = 1
                    and tgt.country = tmp.country
                    and tgt.region = tmp.region`;
                    
// Processing Inserts
var sql_inserts = `insert INTO ` + tgt_tbl + `
                   SELECT * FROM `+ src_tmp_wrk_tbl +``;
                   
var sql_begin = "BEGIN"
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"

try {
       snowflake.execute ({sqlText: sql_begin});
       snowflake.execute ({sqlText: sql_updates});              
       snowflake.execute ({sqlText: sql_inserts});
       snowflake.execute ({sqlText: sql_commit});    
    }
catch (err) {
    snowflake.execute ({sqlText: sql_rollback});
    snowflake.execute ({sqlText: sql_ins_rerun_tbl});
    throw "Loading of "  + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}

return "Done"
$$;

call  SF_CONFIRMED_PRD.DW_APPL.SP_RA_WEATHERFORECAST_FLAT_TO_BIM_LOAD_WEATHER_FORECAST();

SELECT * FROM SF_CONFIRMED_PRD.DW_C_STAGE.RA_WEATHERFORECAST_FLAT_TMP_WRK;

select * from SF_CONFIRMED_PRD.DW_C_ANALYTICS.WEATHER_FORECAST;