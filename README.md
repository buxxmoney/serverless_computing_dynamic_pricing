# Dynamic Pricing System


The Dynamic Pricing System is designed to adjust product prices in real-time based on demand,
supply, competitor prices, customer loyalty, and seasonal events. This system leverages AWS
serverless technologies for scalability and cost-effectiveness.

# Architecture:

AWS Lambda: Executes functions in response to events.
AWS DynamoDB: Stores product, pricing, and customer data.
AWS Kinesis: Processes real-time data streams.
AWS EventBridge: Manages event routing.
AWS CloudWatch: Provides logging and monitoring.
AWS S3: Stores audit logs.
DynamoDB Tables
Products: Stores product details.
CurrentPrice: Holds dynamically adjusted prices.
Customer: Contains customer information.
EventsPromotions: Stores promotional event details.
Tickets: Manages event ticket information.
Lambda Functions
Demand and Supply Trigger: Updates product demand and stock levels.
Competitor Price Trigger: Adjusts competitor prices.
Customer Loyalty Trigger: Updates customer loyalty status.
Seasonal Sales Trigger: Applies promotional discounts.
Tickets Price Trigger: Adjusts ticket prices based on booking time
