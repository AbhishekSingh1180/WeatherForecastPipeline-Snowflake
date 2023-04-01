CREATE OR REPLACE PROCEDURE SF_REFINED_PRD.DW_APPL.SP_RA_WEATHERFORECAST_TO_FLAT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Global Variable Declaration
    
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "SF_REFINED_PRD";
    var ref_schema = "DW_R_ANALYTICS";
	var appl_schema = " DW_APPL"
    var src_tbl = ref_db + "." + appl_schema + ".RA_WeatherForecast_R_STREAM";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".RA_WeatherForecast_R_wrk";
	var src_rerun_tbl = ref_db + "." + wrk_schema + ".RA_WeatherForecast_R_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".RA_WEATHERFORECAST_FLAT";

// check if rerun queue table exists otherwise create it

	var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0
    AS SELECT * FROM `+ src_tbl +`  where 1=2;`;
	try {
      snowflake.execute ({sqlText: sql_crt_rerun_tbl});
    }
    catch (err)  {
      throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
    }

	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +`  as
								select * from `+ src_tbl +` where METADATA$ACTION = 'INSERT'
								UNION ALL
								select * from `+ src_rerun_tbl;
    try {
        snowflake.execute({sqlText: sql_crt_src_wrk_tbl  });
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

	// Empty the rerun queue table
    
	var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl +` `;
    
	try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
      }
    catch (err) {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
      }

      
	// query to load rerun queue table when encountered a failure
    var sql_ins_rerun_tbl = `CREATE or REPLACE table   `+ src_rerun_tbl+` as SELECT * FROM `+ src_wrk_tbl +``;

    var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`
                                with LVL_1_FLATTEN as
                                (
                                    select
                                        tbl.filename as filename
                                        ,tbl.src_json as src_json
                                        ,wf.value as value
                                        ,wf.seq as seq
                                        ,wf.key as key
                                    from `+ src_wrk_tbl +` tbl
                                    ,lateral flatten(tbl.src_json) wf
                                    where wf.key <> 'current'
                                ),
                                LOCATION_FLATTEN AS
                                (
                                    SELECT 
                                        filename,
                                        'Weather API' source,
                                        loc.seq as seq,
                                        loc.value:country::string as country,
                                        loc.value:region::string as region,
                                        loc.value:lat::string as latitude,
                                        loc.value:lon::string as longitude,
                                        loc.value:localtime::string as local_time,
                                        loc.value:localtime_epoch::string as localtime_epoch,
                                        loc.value:tz_id::string as timezone_id
                                        FROM LVL_1_FLATTEN loc WHERE loc.key = 'location'
                                ),
                                FORECASTDAY_FLATTEN AS
                                (
                                    SELECT
                                        fc.seq as seq,
                                        'astro' as key,
                                        fcd.value:astro data
                                    FROM LVL_1_FLATTEN fc, lateral flatten(fc.value:forecastday) fcd WHERE fc.key = 'forecast'  
                                    union all
                                    SELECT
                                        fc.seq as seq,
                                        'hourly' as key,
                                        fcd.value:hour data
                                    FROM LVL_1_FLATTEN fc, lateral flatten(fc.value:forecastday) fcd WHERE fc.key = 'forecast'
                                ),
                                ASTRO_FLATTEN AS
                                (
                                    SELECT 
                                        ast.seq as seq,
                                        ast.data:is_moon_up::string as is_moon_up,
                                        ast.data:is_sun_up::string as is_sun_up,
                                        ast.data:moon_illumination::string as moon_illumination,
                                        ast.data:moon_phase::string as moon_phase,
                                        ast.data:moonrise::string as moonrise,
                                        ast.data:moonset::string as moonset,
                                        ast.data:sunrise::string as sunrise,
                                        ast.data:sunset::string as sunset
                                    FROM FORECASTDAY_FLATTEN ast WHERE ast.key = 'astro'
                                ),
                                HOURLY_FLATTEN AS
                                (
                                    SELECT 
                                        hourly.seq,
                                        hr.value:chance_of_rain::string as chance_of_rain,
                                        hr.value:chance_of_snow::string as chance_of_snow,
                                        hr.value:cloud::string as cloud,
                                        hr.value:dewpoint_c::string as dewpoint_c,
                                        hr.value:dewpoint_f::string as dewpoint_f,
                                        hr.value:feelslike_c::string as feelslike_c,
                                        hr.value:feelslike_f::string as feelslike_f,
                                        hr.value:gust_kph::string as gust_kph,
                                        hr.value:gust_mph::string as gust_mph,
                                        hr.value:heatindex_c::string as heatindex_c,
                                        hr.value:heatindex_f::string as heatindex_f,
                                        hr.value:humidity::string as humidity,
                                        hr.value:is_day::string as is_day,
                                        hr.value:precip_in::string as precip_in,
                                        hr.value:precip_mm::string as precip_mm,
                                        hr.value:pressure_in::string as pressure_in,
                                        hr.value:pressure_mb::string as pressure_mb,
                                        hr.value:temp_c::string as temp_c,
                                        hr.value:temp_f::string as temp_f,
                                        hr.value:time::string as time,
                                        hr.value:time_epoch::string as time_epoch,
                                        hr.value:uv::string as uv,
                                        hr.value:vis_km::string as vis_km,
                                        hr.value:vis_miles::string as vis_miles,
                                        hr.value:will_it_rain::string as will_it_rain,
                                        hr.value:will_it_snow::string as will_it_snow,
                                        hr.value:wind_degree::string as wind_degree,
                                        hr.value:wind_dir::string as wind_dir,
                                        hr.value:wind_kph::string as wind_kph,
                                        hr.value:wind_mph::string as wind_mph,
                                        hr.value:windchill_c::string as windchill_c,
                                        hr.value:windchill_f::string as windchill_f
                                    FROM FORECASTDAY_FLATTEN hourly,
                                    lateral flatten(data) hr
                                    WHERE hourly.key = 'hourly'
                                ),
                                FLATTEN_DATA AS
                                (   
                                    select * from location_flatten loc
                                    left join astro_flatten astr on loc.seq = astr.seq
                                    left join hourly_flatten hr on loc.seq = hr.seq
                                )
                                SELECT
                                    filename,
                                    source,
                                    country, 
                                    region, 
                                    latitude, 
                                    longitude, 
                                    local_time, 
                                    localtime_epoch, 
                                    timezone_id, 
                                    is_moon_up, 
                                    is_sun_up, 
                                    moon_illumination, 
                                    moon_phase, 
                                    moonrise, 
                                    moonset, 
                                    sunrise, 
                                    sunset, 
                                    chance_of_rain, 
                                    chance_of_snow, 
                                    cloud, 
                                    dewpoint_c, 
                                    dewpoint_f, 
                                    feelslike_c, 
                                    feelslike_f, 
                                    gust_kph, 
                                    gust_mph, 
                                    heatindex_c, 
                                    heatindex_f, 
                                    humidity, 
                                    is_day, 
                                    precip_in, 
                                    precip_mm, 
                                    pressure_in, 
                                    pressure_mb, 
                                    temp_c, 
                                    temp_f, 
                                    time, 
                                    time_epoch, 
                                    uv, 
                                    vis_km, 
                                    vis_miles, 
                                    will_it_rain, 
                                    will_it_snow, 
                                    wind_degree, 
                                    wind_dir, 
                                    wind_kph, 
                                    wind_mph, 
                                    windchill_c, 
                                    windchill_f,
                                    current_timestamp dw_createts
                                FROM FLATTEN_DATA;`

	try {
            snowflake.execute ({sqlText: insert_into_flat_dml});
        }
    catch (err)  {
		    snowflake.execute ({sqlText: sql_ins_rerun_tbl});
            throw "Loading of table "+ tgt_flat_tbl +" Failed with error: " + err;   // Return a error message.
        }

	$$;