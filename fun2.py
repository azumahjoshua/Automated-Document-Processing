import json
import boto3
import os

# Initialize SNS and DynamoDB clients
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # Iterate over DynamoDB Stream records
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            # New item inserted into DynamoDB
            new_item = record['dynamodb']['NewImage']
            
            # Extract the relevant fields from the new item
            document_key = new_item['DocumentKey']['S']
            extracted_data = new_item['ExtractedData']['M']
            
            # Log extracted data for debugging
            print(f"Processing new CV data for document: {document_key}")
            print(f"Extracted Data: {json.dumps(extracted_data, indent=2)}")
            
            # Optionally send the extracted data to an SNS topic (for notification)
            send_notification(document_key, extracted_data)

            # Further processing can be done here, such as data enrichment or integration

    return {'statusCode': 200, 'body': 'DynamoDB stream processed'}

def send_notification(document_key, extracted_data):
    """
    Send a notification to an SNS topic about the new CV data.
    """
    # Environment variable that holds the SNS Topic ARN
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    
    # Create a message for the SNS notification
    message = {
        "Document": document_key,
        "Name": extracted_data.get('Name', {}).get('S', 'N/A'),
        "Email": extracted_data.get('Email', {}).get('S', 'N/A'),
        "WorkExperience": extracted_data.get('WorkExperience', {}).get('S', 'N/A'),
        "Education": extracted_data.get('Education', {}).get('S', 'N/A'),
        "Skills": extracted_data.get('Skills', {}).get('S', 'N/A'),
        "Certifications": extracted_data.get('Certifications', {}).get('S', 'N/A'),
        "LinkedIn": extracted_data.get('LinkedIn', {}).get('S', 'N/A'),
        "GitHub": extracted_data.get('GitHub', {}).get('S', 'N/A')
    }
    
    # Send the notification via SNS
    response = sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"New CV Processed: {document_key}",
        Message=json.dumps(message, indent=2)
    )
    
    print(f"Notification sent to SNS for document: {document_key}")
