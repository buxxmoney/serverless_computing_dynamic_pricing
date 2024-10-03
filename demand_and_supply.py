import json
import boto3
from decimal import Decimal
import logging

# Initialize a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Define the DynamoDB table names
PRODUCTS_TABLE_NAME = 'Products'
CURRENT_PRICE_TABLE_NAME = 'CurrentPrice'

def lambda_handler(event, context):
  try:
    # Log the start of the function
    logger.info(f"Starting price update function for table
    {PRODUCTS_TABLE_NAME} and {CURRENT_PRICE_TABLE_NAME}")

    # Get the DynamoDB tables
    products_table = dynamodb.Table(PRODUCTS_TABLE_NAME)
    current_price_table = dynamodb.Table(CURRENT_PRICE_TABLE_NAME)

    # Define the demand coefficient (example value, change as needed)
    demand_coefficient = Decimal('0.05')

    # Iterate over each record in the event
    for record in event['Records']:
      logger.info(f"Processing record: {record}")
      if record['eventName'] == 'MODIFY':
        new_image = record['dynamodb']['NewImage']
      # Fetch the necessary attributes
      product_id = new_image['ProductID']['S']
      base_price = Decimal(new_image['BasePrice']['N'])
      new_demand = Decimal(new_image['Demand']['N'])
      new_stock = Decimal(new_image['Stock']['N'])
  
      # Calculate the new CurrentPrice
      new_current_price = base_price * (Decimal(1) + demand_coefficient * (new_demand / new_stock))
  
      # Update the CurrentPrice in DynamoDB
      current_price_table.update_item(
        Key={'ProductID': product_id}, UpdateExpression='SET CurrentPrice = :val1',
        ExpressionAttributeValues={':val1': new_current_price}
      )
  
      # Log the updated price
      logger.info(f"Updated CurrentPrice table for item
        {product_id} with CurrentPrice: {new_current_price}")

    return {
      'statusCode': 200,
      'body': json.dumps("Current prices updated successfully") 
    }
  except Exception as e:
      logger.error(f"Error processing the request: {e}")
    return {
      'statusCode': 500,
      'body': json.dumps(f"Internal server error: {e}")
    }
