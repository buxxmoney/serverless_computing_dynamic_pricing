import json
import boto3
from decimal import Decimal, Context, setcontext, Inexact, Rounded
import random
import logging

# Initialize a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
dynamodb = boto3.resource('dynamodb')
eventbridge = boto3.client('events')

# Define the DynamoDB table name as a string
COMPETITOR_TABLE = 'Competitor'

# Set the decimal context to avoid Inexact and Rounded errors
context = Context(prec=10, rounding=None, traps=[Inexact, Rounded])
setcontext(context)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    try:
        # Log the start of the function
        logger.info(f"Starting update function for table {COMPETITOR_TABLE}")

        # Get the DynamoDB table
        table = dynamodb.Table(COMPETITOR_TABLE)

        # Fetch all items from the table
        response = table.scan()
        items = response['Items']

        if items:
            # Select a random item
            random_item = random.choice(items)
            competitor_id = random_item['CompetitorID']
            product_id = random_item['ProductID']
            new_competitor_price = Decimal(str(round(random.uniform(10.0, 100.0), 2)))

            # Update the item in DynamoDB
            table.update_item(
                Key={'CompetitorID': competitor_id, 'ProductID': product_id},
                UpdateExpression='SET CompetitorPrice = :val1',
                ExpressionAttributeValues={':val1': new_competitor_price}
            )

            # Log the updated item details
            logger.info(f"Updated CompetitorID: {competitor_id} with NewCompetitorPrice: {new_competitor_price}")

            # Prepare the event details
            updated_item = {
                'CompetitorID': competitor_id,
                'ProductID': product_id,
                'NewCompetitorPrice': str(new_competitor_price)
            }

            # Send event to EventBridge
            response = eventbridge.put_events(
                Entries=[
                    {
                        'Source': 'my.lambda.competitorprice',
                        'DetailType': 'CompetitorPriceUpdate',
                        'Detail': json.dumps(updated_item, default=decimal_default),
                        'EventBusName': 'default'
                    }
                ]
            )

            # Log the EventBridge response
            logger.info(f"EventBridge response: {response}")

            return {
                'statusCode': 200,
                'body': json.dumps(updated_item, default=decimal_default)
            }

        else:
            logger.warning("No items found in the DynamoDB table")
            return {
                'statusCode': 404,
                'body': json.dumps("No items found in the DynamoDB table")
            }

    except Exception as e:
        logger.error(f"Error processing the request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Internal server error: {e}")
        }
