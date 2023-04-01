# WeatherForecastPipeline-Snowflake
Weather Forecast Pipeline using snowflake and AWS (Lambda / S3 / Cloudwatch)

# Pipeline Flow

![WF_PIPE](https://user-images.githubusercontent.com/42813544/229279423-ab666e1d-1762-40f4-8f64-b90ccc975871.png)


1. Lambda Function triggered by event Bridge fetching Data from WeatherAPI

![lambda](https://user-images.githubusercontent.com/42813544/229279546-84fea54c-9c7c-4b1c-961a-f2daeef6d351.jpg)

2. Json Data loads to S3 incrementally everyday

![s3](https://user-images.githubusercontent.com/42813544/229279635-1e8eebf8-ddaf-4d00-8a23-28b3cc0effc1.jpg)

3. Event notification informs Snowpipe via an SQS queue when files are ready to load.

![event notification snowflake](https://user-images.githubusercontent.com/42813544/229279871-2faff570-793d-4432-8e59-5f76afc1e9eb.jpg)

4. Snowpipe Auto ingest the data into varaint table using copy into from external stage.
  
  Stage :
  
  ![stage](https://user-images.githubusercontent.com/42813544/229279689-b08dc936-6a08-43b8-a7ba-12b4214010bc.jpg)
  
  Variant table:
  
  ![variant table](https://user-images.githubusercontent.com/42813544/229279730-af3dda20-bfef-48e1-a922-b9acbf5b64e4.jpg)

5. Next varaint stream will capture the new inserted data into varaint table and trigger the task that runs the store procedure to load data to Flat table. This layer is where all data is integrated and stored in flatten format

![Flat](https://user-images.githubusercontent.com/42813544/229279805-5d4726ac-8a44-448e-9dd8-09d8b5bdecac.jpg)

6. Again Flat stream will capture the new inserted data into flat table and trigger the task that runs the store procedure to load data to Target table along with proper data types and business validation and checks.

![CONF](https://user-images.githubusercontent.com/42813544/229279978-35243672-a553-4648-8159-2c103b2e1c84.jpg)

Note : 
  Target table has SCD type 2 where only latest forecast data will be active.
  
  ![CONF SCD2](https://user-images.githubusercontent.com/42813544/229280032-43373777-b6bb-4503-a640-6b5b73e61daf.jpg)
