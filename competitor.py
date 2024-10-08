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

# Define the DynamoDB table names as strings
PRODUCTS_TABLE = 'CurrentPrice'

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    try:
        logger.info("Starting current price update function")
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract the updated item details from the event
            detail = event.get('detail', {})

        if not detail:
            logger.error("No detail found in the event")
            return {
                'statusCode': 400,
                'body': json.dumps("No detail found in the event")
            }

        competitor_id = detail.get('CompetitorID')
        product_id = detail.get('ProductID')
        new_competitor_price =
        Decimal(detail.get('NewCompetitorPrice', '0'))

        logger.info(f"Processing CompetitorID: {competitor_id},
        ProductID: {product_id}, NewCompetitorPrice:
        {new_competitor_price}")

           # Fetch the product details from the Products table
        product_table = dynamodb.Table(PRODUCTS_TABLE)
        product_response = product_table.get_item(Key={'ProductID':product_id})

        if 'Item' in product_response:
            product = product_response['Item']
            current_price = product.get('CurrentPrice', Decimal('0'))

            # Calculate the new current price
            new_price = new_competitor_price + Decimal(random.choice([-1, 1]))

            logger.info(f"Calculated NewCurrentPrice: {new_price} for ProductID: {product_id}")

            # Update the product's current price in the Products table with conditional write
            try:
                update_response = product_table.update_item(
                Key={'ProductID': product_id},
                UpdateExpression='SET CurrentPrice = :new_price',
                ConditionExpression='CurrentPrice = :current_price',
                ExpressionAttributeValues={':new_price': new_price,':current_price': current_price
                },
                ReturnValues='UPDATED_NEW'
                )

                logger.info(f"UpdateResponse: {update_response}")

                # Log the actual update response attributes
                updated_attributes = update_response.get('Attributes', {})
                logger.info(f"Updated Attributes:{updated_attributes}")

                if 'CurrentPrice' in updated_attributes and updated_attributes['CurrentPrice'] == new_price:
                    logger.info(f"Successfully updated CurrentPrice to {new_price} for ProductID: {product_id}")
                else:
                    logger.error(f"Failed to update CurrentPrice for ProductID: {product_id}")

                return {
                    'statusCode': 200,
                    'body': json.dumps({'ProductID': product_id,
                    'NewCurrentPrice': str(new_price)}, default=decimal_default)
                }
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                logger.warning(f"Conditional check failed for ProductID: {product_id}. The current price might have been updated by another process.")
                return {
                    'statusCode': 409,
                    'body': json.dumps(f"Conditional check failed for ProductID: {product_id}. The current price might have been updated by another process.")
                }
        else:
            logger.warning(f"Product not found for ProductID:{product_id}")
            return {
                'statusCode': 404,
                'body': json.dumps(f"Product not found for ProductID: {product_id}")
            }
    except Exception as e:
        logger.error(f"Error processing the request: {e}")
        return {
        'statusCode': 500,
        'body': json.dumps(f"Internal server error: {e}")
        }
