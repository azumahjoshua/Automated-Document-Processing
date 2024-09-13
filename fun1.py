import json
import boto3
import os
import  uuid

sqs = boto3.client('sqs')
region_name = os.environ['REGION_NAME']
s3 = boto3.client('s3', region_name='us-west-2')
textract = boto3.client('textract', region_name='us-west-2')
dynamodb = boto3.resource('dynamodb')

# DynamoDB table name is set as environment variable
table_name = os.environ['DYNAMODB_TABLE']
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    for record in event['Records']:
        print("Processing record...")
        
        # Add logging to inspect the SQS message
        print(f"Raw SQS message body: {record.get('body', 'No body found')}")
        
        try:
            # Process SQS message body as a JSON object
            message_body = json.loads(record['body'])
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {str(e)}")
            continue
        
        if 'Records' in message_body and len(message_body['Records']) > 0:
            s3_event = message_body['Records'][0]
            bucket_name = s3_event['s3']['bucket']['name']
            object_key = s3_event['s3']['object']['key']
            
            print(f"Processing document from S3 bucket: {bucket_name}, key: {object_key}")
            
            try:
                textract_response = textract.detect_document_text(
                    Document={'S3Object': {'Bucket': bucket_name, 'Name': object_key}}
                )
                extracted_data = extract_data_from_textract(textract_response)
                
                save_data_to_dynamodb(object_key, extracted_data)
                
                print(f"Document processed and data stored in DynamoDB for: {object_key}")
            except Exception as e:
                print(f"Error processing document: {str(e)}")
        else:
            print(f"Invalid message format: {message_body}")

    return {'statusCode': 200, 'body': 'Document processed'}

def extract_data_from_textract(response):
    extracted_data = {
        'Name': '',
        'Email': '',
        'Phone': '',
        'WorkExperience': [],
        'Education': [],
        'Skills': '',
        'Certifications': '',
        'LinkedIn': '',
        'GitHub': ''
    }

    blocks = response.get('Blocks', [])
    current_section = None
    work_experience = []
    education = []
    skills = []
    certifications = []
    
    for block in blocks:
        if block['BlockType'] == 'LINE':
            text = block['Text']

            if "experience" in text.lower():
                current_section = 'WorkExperience'
            elif "education" in text.lower():
                current_section = 'Education'
            elif "skills" in text.lower():
                current_section = 'Skills'
            elif "certifications" in text.lower():
                current_section = 'Certifications'
            elif "linkedin" in text.lower():
                extracted_data['LinkedIn'] = extract_value_after_key(text, "linkedin")
            elif "github" in text.lower():
                extracted_data['GitHub'] = extract_value_after_key(text, "github")
            elif "name" in text.lower() or "email" in text.lower() or "phone" in text.lower():
                if "name" in text.lower():
                    extracted_data['Name'] = extract_value_after_key(text, "name")
                if "email" in text.lower():
                    extracted_data['Email'] = extract_value_after_key(text, "email")
                if "phone" in text.lower():
                    extracted_data['Phone'] = extract_value_after_key(text, "phone")

            if current_section == 'WorkExperience':
                work_experience.append(text)
            elif current_section == 'Education':
                education.append(text)
            elif current_section == 'Skills':
                skills.append(text)
            elif current_section == 'Certifications':
                certifications.append(text)

    extracted_data['WorkExperience'] = process_work_experience(work_experience)
    extracted_data['Education'] = process_education(education)
    extracted_data['Skills'] = ", ".join(skills)
    extracted_data['Certifications'] = ", ".join(certifications)

    return extracted_data

def extract_value_after_key(text, key):
    try:
        return text.split(f'{key}:')[1].strip()
    except IndexError:
        return ''

def process_work_experience(work_experience_lines):
    experience_list = []
    current_experience = {}

    for line in work_experience_lines:
        if "company" in line.lower():
            current_experience['Company'] = line
        elif "title" in line.lower():
            current_experience['JobTitle'] = line
        elif "date" in line.lower():
            current_experience['Dates'] = line
        else:
            current_experience.setdefault('Description', []).append(line)

        if 'Company' in current_experience and 'JobTitle' in current_experience and 'Dates' in current_experience:
            experience_list.append(current_experience)
            current_experience = {}

    return experience_list

def process_education(education_lines):
    education_list = []
    current_education = {}

    for line in education_lines:
        if "degree" in line.lower() or "bachelor" in line.lower() or "master" in line.lower():
            current_education['Degree'] = line
        elif "institution" in line.lower() or "university" in line.lower():
            current_education['Institution'] = line
        elif "graduation" in line.lower() or "date" in line.lower():
            current_education['GraduationDate'] = line

        if 'Degree' in current_education and 'Institution' in current_education and 'GraduationDate' in current_education:
            education_list.append(current_education)
            current_education = {}

    return education_list

def save_data_to_dynamodb(document_key, extracted_data):
    try:
        table.put_item(
            Item={
                'documentID': str(uuid.uuid4()),
                'DocumentKey': document_key,
                'ExtractedData': extracted_data,
            }
        )
    except Exception as e:
        print(f"Error saving data to DynamoDB: {str(e)}")
