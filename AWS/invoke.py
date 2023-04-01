#import datetime
import json
import datetime
import requests
import boto3

def get_data(event, context):

    try:
    #Use AWS Secrets for now using config file
        cred = {}
        with open('config.json') as f:
            config = json.load(f)
            for key in config:
                cred[key] = config[key]
        
        url = cred['url']
        
        # get tomorrow's date
        forecast_date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
       
        querystring = {"q":"India","days":"1","dt":"{}".format(forecast_date)}

        headers = {
            "X-RapidAPI-Key": cred['X-RapidAPI-Key'],
            "X-RapidAPI-Host": cred['X-RapidAPI-Host']
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        # convert response to JSON
        json_response = json.loads(response.text)

        print(json_response)

        # print(response.text)
        # store this response in a json file in S3 with filename as forecast_details_{forecast_date}.json
        s3 = boto3.resource('s3')
        s3.Bucket(cred['S3-bucket']).put_object(Key='forecast_details_{}.json'.format(forecast_date), Body=json.dumps(json_response), ContentType='application/json')
     
        return {
            'statusCode': 200,
            'body': json.dumps({'Status': 'Success', 'Message': '{} forecast details stored in S3 bucket successfully'.format(forecast_date)})
        }
    
    except Exception as e: 
        return {
            'statusCode': 500,
            'body': json.dumps({'Status': 'Failure', 'Message': '{}'.format(e)})
        }
    
get_data('','')