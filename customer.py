import json
import boto3
import logging
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource with specified region
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# Define the loyalty level coefficients
LOYALTY_COEFFICIENTS = {
    'Bronze': Decimal('0.02'),
    'Silver': Decimal('0.05'),
    'Gold': Decimal('0.10'),
    'Platinum': Decimal('0.15')
}

# Define the loyalty level thresholds
LOYALTY_THRESHOLDS = [
    (500, 'Platinum'),
    (250, 'Gold'),
    (100, 'Silver'),
    (0, 'Bronze')
]

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def calculate_new_price(base_price, loyalty_level):
    coefficient = LOYALTY_COEFFICIENTS.get(loyalty_level, Decimal('0.02'))  # Default to Bronze if not found
    return base_price * (Decimal('1.0') + coefficient)

def determine_loyalty_level(total_spent):
    for threshold, level in LOYALTY_THRESHOLDS:
        if total_spent >= threshold:
            return level
    return 'Bronze'  # Default to Bronze if no threshold matches

def lambda_handler(event, context):
    try:
        logger.info("Starting lambda function to process CustomerProductSelection stream event")
        
        # Log the event for debugging purposes
        logger.info(f"Event: {json.dumps(event, default=decimal_default)}")
        
        if 'Records' not in event or not event['Records']:
            logger.error("Event does not contain 'Records' key or 'Records' is empty")
            return {
                'statusCode': 400,
                'body': json.dumps("Event does not contain 'Records' key or 'Records' is empty")
            }
        
        for record in event['Records']:
            logger.info(f"Processing record: {record}")
            
            if record['eventName'] == 'INSERT':
                new_image = record['dynamodb']['NewImage']
                customer_id = new_image['CustomerID']['S']
                product_id = new_image['ProductID']['S']
                logger.info(f"CustomerID: {customer_id}, ProductID: {product_id}")
                
                # Get the base price from the Products table
                product_table = dynamodb.Table('Products')
                product_response = product_table.get_item(Key={'ProductID': product_id})
                
                if 'Item' not in product_response:
                    logger.error(f"Product with ID {product_id} not found")
                    continue
                
                product = product_response['Item']
                base_price = Decimal(product['BasePrice'])
                logger.info(f"Base price of product {product_id}: {base_price}")
                
                # Get the customer's loyalty level and total spent
                customer_table = dynamodb.Table('Customer')
                customer_response = customer_table.get_item(Key={'CustomerID': customer_id})
                
                if 'Item' not in customer_response:
                    logger.error(f"Customer with ID {customer_id} not found")
                    continue
                
                customer = customer_response['Item']
                loyalty_level = customer['LoyaltyLevel']
                total_spent = Decimal(customer['TotalSpent'])
                logger.info(f"Customer {customer_id} - LoyaltyLevel: {loyalty_level}, TotalSpent: {total_spent}")
                
                # Calculate the new current price
                current_price = calculate_new_price(base_price, loyalty_level)
                logger.info(f"Calculated current price: {current_price}")
                
                # Update the customer's total spent
                new_total_spent = total_spent + current_price
                logger.info(f"New total spent for customer {customer_id}: {new_total_spent}")
                
                # Determine if the loyalty level should be updated
                new_loyalty_level = determine_loyalty_level(new_total_spent)
                logger.info(f"New loyalty level for customer {customer_id}: {new_loyalty_level}")
                
                # Update the customer's record in the Customer table
                customer_table.update_item(
                    Key={'CustomerID': customer_id},
                    UpdateExpression="SET TotalSpent = :totalSpent, LoyaltyLevel = :loyaltyLevel",
                    ExpressionAttributeValues={
                        ':totalSpent': new_total_spent,
                        ':loyaltyLevel': new_loyalty_level
                    }
                )
                
                logger.info(f"Updated customer {customer_id}: TotalSpent = {new_total_spent}, LoyaltyLevel = {new_loyalty_level}")
        
        logger.info("Finished processing all records")
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed event')
        }
    except Exception as e:
        logger.error(f"Error processing CustomerProductSelection stream event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing CustomerProductSelection stream event')
        }
