import sys
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = 'AKIA5SGRKANTPXS4RWKA'
SECRET_KEY = 'srmT7TLMIIk5i+8yJ/rbzA5alRVXkI3+09VuC/##'
BUCKET_NAME = 'excel-file-uploads-aniket'  # Your bucket

def upload_to_s3(file_path, s3_file_name):
    try:
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

        s3.upload_file(file_path, BUCKET_NAME, s3_file_name)
        print(f"Upload Successful: {s3_file_name} to bucket {BUCKET_NAME}")

    except FileNotFoundError:
        print("The file was not found. Check your FILE_PATH.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Something went wrong: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload_excel_to_s3.py <file_path> [s3_file_name]")
        sys.exit(1)

    file_path = sys.argv[1]
    # If S3 filename not provided, use the same as file name
    s3_file_name = sys.argv[2] if len(sys.argv) > 2 else file_path.split("\\")[-1]

    upload_to_s3(file_path, s3_file_name)