# File Movement Automation

This project demonstrates how to securely scrub sensitive data from files stored in an AWS S3 bucket before moving them to another bucket. The script ensures that Personally Identifiable Information (PII) like emails, phone numbers, and addresses are replaced with dummy data in transit, while leaving the original files in the source bucket untouched.

---

## Features

- Scrubs PII (email, phone number, address columns) from CSV files.
- Maintains original files in the source bucket.
- Moves scrubbed files to a destination bucket.
- Operates on-demand using Python and AWS SDK (`boto3`).

---

## Requirements

1. Python 3.x
2. AWS CLI installed and configured
3. IAM Role or AWS credentials with permissions for:
   - S3 List (`s3:ListBucket`)
   - S3 Read (`s3:GetObject`)
   - S3 Write (`s3:PutObject`)

---

## Setup Instructions

1. **Create S3 Buckets**:
   - Create two S3 buckets:
     - `prod2-source-bucket` (source bucket)
     - `prod2-destination-bucket` (destination bucket)

2. **Upload the Dummy CSV File**:
   - The `dummy_data.csv` file provided in the repository contains sample data.
   - Upload it to the source bucket:
     ```bash
     aws s3 cp dummy_data.csv s3://prod2-source-bucket/
     ```

3. **Run the Script**:
   - Execute the script to scrub PII and move the file:
     ```bash
     python3 move_and_scrub_pii.py
     ```

4. **Verify the Results**:
   - Check that the scrubbed file is in the destination bucket:
     ```bash
     aws s3 ls s3://prod2-destination-bucket/
     ```

---

## How It Works

- **Source Bucket**: Contains original files.
- **Destination Bucket**: Receives scrubbed files with dummy data.
- **Scrubbing Logic**: Replaces PII fields with:
  - Email: `dummy_email@example.com`
  - Phone: `123-456-7890`
  - Address: `Dummy Address`

---

## File Structure

```plaintext
file-movement-automation/
│
├── move_and_scrub_pii.py   # Main script for scrubbing and moving files
├── dummy_data.csv          # Sample CSV file
├── README.md               # Project documentation
