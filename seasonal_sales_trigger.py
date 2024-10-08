import json
import random
import boto3
import logging
from datetime import datetime, timedelta
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('EventsPromotions')
    lambda_client = boto3.client('lambda')

    # Fetch all events from DynamoDB table
    try:
        response = table.scan()
        events = response['Items']
        logger.info(f"Fetched {len(events)} events from EventsPromotions table.")
    except Exception as e:
        logger.error(f"Error fetching events from DynamoDB: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    # Randomly select a date
    if random.random() < 0.5:
        # Generate a random date
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        selected_date = random_date.strftime('%d-%m-%Y')
        logger.info(f"Randomly selected date: {selected_date}")
    else:
        # Select a specific date from events
        selected_event = random.choice(events)
        selected_date = selected_event['StartDate']
        logger.info(f"Selected date from event: {selected_date}")

    # Check if any event is active on the selected date
    active_event = None
    selected_date_obj = datetime.strptime(selected_date, '%d-%m-%Y')
    for event in events:
        start_date_obj = datetime.strptime(event['StartDate'], '%d-%m-%Y')
        end_date_obj = datetime.strptime(event['EndDate'], '%d-%m-%Y')
        if start_date_obj <= selected_date_obj <= end_date_obj:
            active_event = event
            logger.info(f"Active event found: {event['EventName']} (EventID: {event['EventID']})")
            break

    response_payload = {
        'SelectedDate': selected_date,
        'ActiveEvent': active_event
    }

    if active_event:
        # Invoke the second Lambda function
        try:
            response = lambda_client.invoke(
                FunctionName='seasonalsales',
                InvocationType='RequestResponse',
                Payload=json.dumps(response_payload, cls=DecimalEncoder)
            )
            response_body = json.loads(response['Payload'].read())
            logger.info(f"Second Lambda function response: {response_body}")
            response_payload['UpdatedProducts'] = response_body
        except Exception as e:
            logger.error(f"Error invoking second Lambda function: {str(e)}")
