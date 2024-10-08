import json
import random
import boto3
import logging
from decimal import Decimal
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource with specified region
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')  # Change to your region
TABLE_NAME = 'PurchaseHistory'

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def generate_selection_id():
    timestamp = int(time.time() * 1000)  # Current time in milliseconds
    random_number = random.randint(1000, 9999)  # Random 4-digit number
    selection_id = str(timestamp)[-6:] + str(random_number)  # Combine to make a 10-digit number
    return selection_id

def lambda_handler(event, context):
    try:
        logger.info(f"Starting lambda function for table {TABLE_NAME}")

        # Access the PurchaseHistory table
        table = dynamodb.Table(TABLE_NAME)

        # Select random product from Products table
        product_table = dynamodb.Table('Products')
        product_items = product_table.scan()['Items']
        product_ids = [item['ProductID'] for item in product_items]
        random_product_id = random.choice(product_ids)

        # Select a random customerID from the Customers table
        customer_table = dynamodb.Table('Customer')
        customer_items = customer_table.scan()['Items']
        customer_ids = [item['CustomerID'] for item in customer_items]
        random_customer_id = random.choice(customer_ids)

        # Generate a new selection ID and add the record to CustomerProductSelection table
        selection_table = dynamodb.Table('CustomerProductSelection')
        selection_id = generate_selection_id()
        selection_table.put_item(
            Item={
                'SelectionID': selection_id,
                'CustomerID': random_customer_id,
                'ProductID': random_product_id
            }
        )

        logger.info(f"Changing {random_customer_id} and {random_product_id} and calculating new customer price...")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'selectionID': selection_id,
                'randomProductID': random_product_id,
                'randomCustomerID': random_customer_id,
                'message': 'The Customer lambda function has been updated successfully. A new customer has been calculated.'
            }, default=decimal_default)
        }

    except Exception as e:
        logger.error(f"Error updating PurchaseHistory: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error updating PurchaseHistory')
        }
