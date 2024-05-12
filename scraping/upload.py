import os
import boto3

# Initialize the S3
s3 = boto3.resource('s3',
                    aws_access_key_id='ASIA4HHT4VBDOBM7OAN6',#secrets.AWS_ACCESS_KEY,
                    aws_secret_access_key='CJjLtgCaqAYEd39VDPHn95Vt61Ns/e3bYKdZNREK',#secrets.AWS_SECRET_KEY,
                    aws_session_token='IQoJb3JpZ2luX2VjEPf//////////wEaCXVzLXdlc3QtMiJHMEUCIC78+9DLwCoQJbJoT5IdCEGt4ZW/AQwPzm9OaYB71uZoAiEA6bJ9vKTsDeWFlxBqPs4299QbrpukzjO8Y8e5JISzv/kqsAIIYBABGgw4NDAxNzc1OTIzOTAiDLRSbk7T/+Oc0QiFRCqNAvxRyDaaDPCYg981Cbs/iyJdVQXqpUEeT3ekyZ4kAqJOKyPgiIZKGDpofx9ixRpGYkgKnPq3eEVeuF9STfOMDQSYjbB6e7z6zZLB0VLrnY2LIibNgZeOo7Ey/1TEUHwPVYwW6Csvofs8q1KaVSHRXoZ9Rdyk+IHs/2sTFUtPi1N+JVBI9KZaJQ4sLHOKjqsjH0Lncnm6mQuIba2/of0dsU5YOJ5n5J/YprVtiGaE57r5V4PFi2sqyxkC1CClWgy9JitHaI31HDUoVYJjrTpYQD5sEoTMBuaNt2GJmOb7eWjp91HQGHdllhaP2K+aWJGoe6EFtJZ2Y+0efLhOTTvvi9oemlRYdoggxLbM3aPyMIy4g7IGOp0BC3hFziE5aGEdRGO9KgWlN3zRVjRotk+h8Jghqo40A0kP8KhagnQ6ATaWcWtILceztSV0G0BAe0WrsRjXZRhWwnS/tMcKVWodR+O3NWk6uwRmnHCokaKaTpU1NycGBWrh+fdSobvnzNkzD6jXaxpVouM7BXAQH1zJc/tEwEHPDcQ/j51XVYRkvzcUEC3OtDqZxgUg98Lg/GhVdMtPHA==',#secrets.AWS_SESSION_TOKEN
                    )
s3_client = boto3.client('s3',
                    aws_access_key_id='ASIA4HHT4VBDOBM7OAN6',#secrets.AWS_ACCESS_KEY,
                    aws_secret_access_key='CJjLtgCaqAYEd39VDPHn95Vt61Ns/e3bYKdZNREK',#secrets.AWS_SECRET_KEY,
                    aws_session_token='IQoJb3JpZ2luX2VjEPf//////////wEaCXVzLXdlc3QtMiJHMEUCIC78+9DLwCoQJbJoT5IdCEGt4ZW/AQwPzm9OaYB71uZoAiEA6bJ9vKTsDeWFlxBqPs4299QbrpukzjO8Y8e5JISzv/kqsAIIYBABGgw4NDAxNzc1OTIzOTAiDLRSbk7T/+Oc0QiFRCqNAvxRyDaaDPCYg981Cbs/iyJdVQXqpUEeT3ekyZ4kAqJOKyPgiIZKGDpofx9ixRpGYkgKnPq3eEVeuF9STfOMDQSYjbB6e7z6zZLB0VLrnY2LIibNgZeOo7Ey/1TEUHwPVYwW6Csvofs8q1KaVSHRXoZ9Rdyk+IHs/2sTFUtPi1N+JVBI9KZaJQ4sLHOKjqsjH0Lncnm6mQuIba2/of0dsU5YOJ5n5J/YprVtiGaE57r5V4PFi2sqyxkC1CClWgy9JitHaI31HDUoVYJjrTpYQD5sEoTMBuaNt2GJmOb7eWjp91HQGHdllhaP2K+aWJGoe6EFtJZ2Y+0efLhOTTvvi9oemlRYdoggxLbM3aPyMIy4g7IGOp0BC3hFziE5aGEdRGO9KgWlN3zRVjRotk+h8Jghqo40A0kP8KhagnQ6ATaWcWtILceztSV0G0BAe0WrsRjXZRhWwnS/tMcKVWodR+O3NWk6uwRmnHCokaKaTpU1NycGBWrh+fdSobvnzNkzD6jXaxpVouM7BXAQH1zJc/tEwEHPDcQ/j51XVYRkvzcUEC3OtDqZxgUg98Lg/GhVdMtPHA==',#secrets.AWS_SESSION_TOKEN
                    )

def upload_files_to_s3(bucket_name, folder_path):
    file_count = 0  # counter
    # loop through all the files in the folder
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        # check if the file is a file (not a folder)
        if os.path.isfile(file_path):
            try:
                # upload the file to S3
                s3_client.upload_file(file_path, bucket_name, file_name)
                file_count += 1  # renew the counter
                if file_count % 1000 == 0:
                    print(f"uploaded {file_count} files to {bucket_name}")
            except Exception as e:
                print(f"uploading {file_name} fail: {e}")

# use the function to upload files to S3
bucket_name = 'patent-bucket-raw'
folder_path = 'raw'

upload_files_to_s3(bucket_name, folder_path)