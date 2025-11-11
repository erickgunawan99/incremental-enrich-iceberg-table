import os
import boto3
import asyncio

region = 'us-east-1'
s3_path = 's3a://transaction/raw/'
#s3_path = 's3://XXX5/raw/'
path_without_scheme = s3_path.split("://", 1)[-1]
parts = path_without_scheme.split("/", 1) #1 is the number of split so it doesnt split the last /
        #parts[0] is XX5 the bucket and parts[1] is the prefix
manifest_key = f"pending/manifest.pending"  
s3_client = boto3.client('s3', endpoint_url='http://minio:9000', 
                            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"), 
                            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
                            region_name = region,
                            aws_session_token=None, 
                            config=boto3.session.Config(signature_version='s3v4'), 
                            verify=False)  

def create_manifest(max_files):
     

     paginator = s3_client.get_paginator("list_objects_v2") #paginator to get 
     pages = paginator.paginate(Bucket=parts[0], Prefix=parts[1]) #bucket name and prefix (raw or smtg)

     list_files = []
     for page in pages:
          for obj in page.get('Contents', []):
              list_files.append(f"s3a://{parts[0]}/{obj['Key']}")
              if (len(list_files) >= max_files):
                   break
          if (len(list_files) >= max_files):
                break
          
     if not list_files:
          return None
     
        # \n.join = new line join, f1,f2,f3 = 
        # f1
        # f2
        # f3
     manifest_content = '\n'.join(list_files)  
     
    # Write manifest file to S3
     s3_client.put_object(Bucket=parts[0], Key=manifest_key, Body=manifest_content)
     print(f"Manifest file created: s3a://{parts[0]}/{manifest_key}")
     return f"s3a://{parts[0]}/{manifest_key}"

async def archive_each(file_path):
     old_key = file_path.split('/', 3)[-1]
     new_key = f"archive/{os.path.basename(old_key)}"
     s3_client.copy_object(
          Bucket=parts[0],
          CopySource={'Bucket': parts[0], 'Key': old_key},
          Key=new_key
     )
     s3_client.delete_object(Bucket=parts[0], Key=old_key)
 
async def archive_processed_files():
     response = s3_client.get_object(Bucket=parts[0], Key=manifest_key)
     content = response['Body'].read().decode('utf-8')

     file_paths = [path.strip() for path in content.split('\n') if path.strip()] 
      #if path.strip() return false if it only contains whitespace
     tasks = [archive_each(file_path=file_path) for file_path in file_paths]
     gather_tasks = await asyncio.gather(*tasks)
     return gather_tasks

def archive_callable():
     asyncio.run(archive_processed_files())
     print("files have been archived")

async def quarantine_each(file_path):
     old_key = file_path.split('/', 3)[-1]
     new_key = os.path.basename(old_key)
     s3_client.copy_object(
          Bucket="quarantine",
          CopySource={'Bucket': parts[0], 'Key': old_key},
          Key=new_key
     )
     s3_client.delete_object(Bucket=parts[0], Key=old_key)

async def quarantine_processed_files():
     response = s3_client.get_object(Bucket=parts[0], Key=manifest_key)
     content = response['Body'].read().decode('utf-8')

     file_paths = [path.strip() for path in content.split('\n') if path.strip()] 
      #if path.strip() return false if it only contains whitespace
     tasks = [quarantine_each(file_path=file_path) for file_path in file_paths]
     gather_tasks = await asyncio.gather(*tasks)
     return gather_tasks

def quarantine_callable():
     asyncio.run(quarantine_processed_files())
     print("files have been quarantined")

def delete_manifest():
     s3_client.delete_object(Bucket=parts[0], Key=manifest_key) 
     print("manifest file deleted")
     #split to get the mmanifest key out of the path
