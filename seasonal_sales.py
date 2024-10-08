import json
import boto3
import logging
from decimal import Decimal, Context, ROUND_HALF_EVEN

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    # Extract event details from the input
    event_details = event
    selected_date = event_details['SelectedDate']
    active_event = event_details['ActiveEvent']

    if not active_event:
        logger.info(f"No active event on the selected date: {selected_date}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'No active event on the selected date.'
            })
        }

    affected_products = active_event['AffectedProducts'].split(',')
    discount_rate = float(active_event['DiscountRate'])
    logger.info(f"Processing {len(affected_products)} affected products with a discount rate of {discount_rate}.")

    dynamodb = boto3.resource('dynamodb')
    products_table = dynamodb.Table('Products')
    current_price_table = dynamodb.Table('CurrentPrice')
    updated_products = []

    # Define the context for Decimal to handle precision issues
    context = Context(prec=10, rounding=ROUND_HALF_EVEN)

    for product_id in affected_products:
        # Fetch the product details
        response = products_table.get_item(Key={'ProductID': product_id})
        product = response.get('Item')

        if not product:
            logger.warning(f"ProductID {product_id} not found in Products table.")
            continue

        base_price = context.create_decimal(product['BasePrice'])
        new_current_price = base_price * (1 - context.create_decimal(discount_rate))

        # Update the CurrentPrice table
        current_price_table.update_item(
            Key={'ProductID': product_id},
            UpdateExpression="set CurrentPrice = :c",
            ExpressionAttributeValues={':c': str(new_current_price)}
        )

        logger.info(f"Updated ProductID {product_id} from BasePrice {base_price} to NewCurrentPrice {new_current_price}.")

        # Append the product update details to the list
        updated_products.append({
            'ProductID': product_id,
            'BasePrice': float(base_price),
            'NewCurrentPrice': float(new_current_price),
            'DiscountRate': discount_rate
        })

    # Return the details of updated products
    return {
        'statusCode': 200,
        'body': json.dumps(updated_products, cls=DecimalEncoder)
    }
