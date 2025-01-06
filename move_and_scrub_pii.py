import boto3
import csv
import os
from io import StringIO
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize the S3 client using default AWS credentials or IAM Role
s3 = boto3.client('s3')

# Define the source and destination bucket names
SOURCE_BUCKET = 'prod2-source-bucket'  # Bucket containing the original files
DESTINATION_BUCKET = 'prod2-destination-bucket'  # Bucket to receive scrubbed files

def scrub_sensitive_data(file_content):
    """
    Scrubs sensitive data (PII) from the CSV file content.
    Specifically replaces Email, Phone, and Address-related fields with dummy data.
    Args:
        file_content (str): The content of the CSV file as a string.
    Returns:
        str: The scrubbed CSV content with sensitive data replaced.
    """
    # Create CSV reader and writer objects for in-memory processing
    csv_reader = csv.reader(StringIO(file_content))
    scrubbed_csv = StringIO()  # In-memory storage for the scrubbed data
    csv_writer = csv.writer(scrubbed_csv)

    # Extract and write the header row (assumes first row contains headers)
    headers = next(csv_reader)
    csv_writer.writerow(headers)

    # Define the dummy replacement data
    dummy_email = "dummy_email@example.com"
    dummy_phone = "123-456-7890"
    dummy_address = "Dummy Address"

    # Process each row in the CSV file
    for row in csv_reader:
        # Replace sensitive columns with dummy data
        row[2] = dummy_email  # Email column
        row[3] = dummy_phone  # Phone column
        row[5] = dummy_address  # Address column
        row[6] = "Dummy City"  # City column
        row[7] = "Dummy State"  # State column
        row[8] = "00000"  # Zip column
        row[9] = "Dummy Country"  # Country column

        # Write the scrubbed row to the in-memory CSV writer
        csv_writer.writerow(row)

    # Return the entire scrubbed CSV content as a string
    scrubbed_csv.seek(0)  # Reset pointer to the start of the stream
    return scrubbed_csv.getvalue()

def move_and_scrub_file(file_key):
    """
    Processes a specific CSV file by scrubbing PII and moving it to the destination bucket.
    Args:
        file_key (str): The key (name) of the file in the source bucket.
    """
    try:
        # Step 1: Retrieve the file from the source bucket
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')  # Decode file content to text

        # Step 2: Scrub sensitive data from the file content
        scrubbed_content = scrub_sensitive_data(file_content)

        # Step 3: Write the scrubbed content to a temporary file
        temp_file_name = f"scrubbed_{file_key}"  # Temporary file name for local storage
        with open(temp_file_name, "w") as temp_file:
            temp_file.write(scrubbed_content)

        # Step 4: Upload the scrubbed file to the destination bucket
        s3.upload_file(temp_file_name, DESTINATION_BUCKET, file_key)
        print(f"File '{file_key}' has been scrubbed and uploaded to the destination bucket.")

        # Step 5: Delete the temporary file to clean up local storage
        os.remove(temp_file_name)
    except Exception as e:
        # Handle any exceptions that occur during processing
        print(f"Error processing file '{file_key}': {e}")

if __name__ == '__main__':
    try:
        # Step 1: List all files in the source bucket
        response = s3.list_objects_v2(Bucket=SOURCE_BUCKET)
        if 'Contents' in response:  # Check if the bucket contains any files
            files = [file['Key'] for file in response['Contents']]  # Extract file keys
            print(f"Found files in source bucket: {files}")

            # Step 2: Process each file in the bucket
            for file_key in files:
                if file_key.endswith('.csv'):  # Only process files with .csv extension
                    print(f"Processing file: {file_key}")
                    move_and_scrub_file(file_key)
                else:
                    print(f"Skipping non-CSV file: {file_key}")
        else:
            print("No files found in the source bucket.")
    except (NoCredentialsError, PartialCredentialsError) as cred_err:
        # Handle cases where AWS credentials are missing or incomplete
        print(f"Credentials error: {cred_err}")
    except Exception as general_err:
        # Handle any other unexpected errors
        print(f"An unexpected error occurred: {general_err}")
