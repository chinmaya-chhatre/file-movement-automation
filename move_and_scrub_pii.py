import boto3
import csv
import os
from io import StringIO
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize the S3 client
s3 = boto3.client('s3')

# Define source and destination buckets
SOURCE_BUCKET = 'prod2-source-bucket'  # Replace with your source bucket name
DESTINATION_BUCKET = 'prod2-destination-bucket'  # Replace with your destination bucket name

def scrub_sensitive_data(file_content):
    """
    Scrubs sensitive data (email, phone, and address columns) in the CSV file content.
    Args:
        file_content (str): The content of the CSV file as a string.
    Returns:
        str: The scrubbed CSV content.
    """
    # Load CSV content into memory
    csv_reader = csv.reader(StringIO(file_content))
    scrubbed_csv = StringIO()
    csv_writer = csv.writer(scrubbed_csv)

    # Process the header row
    headers = next(csv_reader)
    csv_writer.writerow(headers)

    # Define dummy data
    dummy_email = "dummy_email@example.com"
    dummy_phone = "123-456-7890"
    dummy_address = "Dummy Address"

    # Process each row and scrub sensitive fields
    for row in csv_reader:
        # Replace sensitive columns with dummy data
        row[2] = dummy_email  # Email column
        row[3] = dummy_phone  # Phone column
        row[5] = dummy_address  # Address column
        row[6] = "Dummy City"  # City column
        row[7] = "Dummy State"  # State column
        row[8] = "00000"  # Zip column
        row[9] = "Dummy Country"  # Country column

        csv_writer.writerow(row)

    # Return the scrubbed CSV content
    scrubbed_csv.seek(0)
    return scrubbed_csv.getvalue()

def move_and_scrub_file(file_key):
    """
    Copies a CSV file from the source bucket to the destination bucket, scrubbing PII in transit.
    Args:
        file_key (str): The key of the file in the source bucket.
    """
    try:
        # Step 1: Download the file from the source bucket
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')

        # Step 2: Scrub sensitive data
        scrubbed_content = scrub_sensitive_data(file_content)

        # Step 3: Save the scrubbed content to a temporary file
        temp_file_name = f"scrubbed_{file_key}"
        with open(temp_file_name, "w") as temp_file:
            temp_file.write(scrubbed_content)

        # Step 4: Upload the scrubbed file to the destination bucket
        s3.upload_file(temp_file_name, DESTINATION_BUCKET, file_key)
        print(f"File '{file_key}' has been scrubbed and moved to the destination bucket.")

        # Step 5: Clean up the temporary file
        os.remove(temp_file_name)
    except Exception as e:
        print(f"Error processing file '{file_key}': {e}")

if __name__ == '__main__':
    try:
        # Step 1: List all files in the source bucket
        response = s3.list_objects_v2(Bucket=SOURCE_BUCKET)
        if 'Contents' in response:
            files = [file['Key'] for file in response['Contents']]
            print(f"Found files in source bucket: {files}")

            # Step 2: Process each file
            for file_key in files:
                if file_key.endswith('.csv'):  # Process only CSV files
                    print(f"Processing file: {file_key}")
                    move_and_scrub_file(file_key)
                else:
                    print(f"Skipping non-CSV file: {file_key}")
        else:
            print("No files found in the source bucket.")
    except (NoCredentialsError, PartialCredentialsError) as cred_err:
        print(f"Credentials error: {cred_err}")
    except Exception as general_err:
        print(f"An unexpected error occurred: {general_err}")
