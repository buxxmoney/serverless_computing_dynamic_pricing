import json
import boto3
from decimal import Decimal
import random
import logging

# Initialize a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Define the DynamoDB table name as a string
TABLE_NAME = 'Products'

def lambda_handler(event, context):
    try:
        # Log the start of the function
        logger.info(f"Starting lambda function for table {TABLE_NAME}")

        # Get the DynamoDB table
        table = dynamodb.Table(TABLE_NAME)

        # Fetch all items from the table
        response = table.scan()
        items = response['Items']

        if items:
            # Select a random item
            random_item = random.choice(items)

            # Simulate new random demand and stock values
            new_demand = random.randint(1, 100)
            new_stock = random.randint(1, 500)

            # Update the item in DynamoDB
            table.update_item(
                Key={'ProductID': random_item['ProductID']},
                UpdateExpression='SET Demand = :val1, Stock = :val2',
                ExpressionAttributeValues={
                    ':val1': Decimal(new_demand),
                    ':val2': Decimal(new_stock)
                }
            )

            # Log the updated item details
            logger.info(f"Updated item {random_item['ProductID']} with Demand: {new_demand} and Stock: {new_stock}")

            # Return the updated item details
            updated_item = {
                'ProductID': random_item['ProductID'],
                'NewDemand': new_demand,
                'NewStock': new_stock
            }
            return {
                'statusCode': 200,
                'body': json.dumps(updated_item)
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
