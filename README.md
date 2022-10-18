# google_data_transfer_utility  
  
Google Cloud script to load continuously files onto a certain bucket in order to populate it for the Data Transfer procedure.  
  
# copy_bucket.py  
  
Python script used to create or run a storage transfer job on Google Cloud in order to copy buckets.  

## Setup  
  
Run the script using the various options available:  
- --project-id: Project-id of the project to use (**required**)  
- --job-name: The name of an existing job or of a job to create. If not specified during the *create* operation, a random name will be generated  (**required only for *run* operation**)  
- --description: The description of the job  
- --source-bucket: The source bucket name to use for a new job (**required only for the *create* operation**)  
- --sink-bucket: The sink (destination) bucket name to use for a new job (**required only for the *create* operation**)   
- --operation: The operation to do [create|run] (**ALWAYS required**)  
  
**Example of a script input:**  
```
python3 copy_bucket.py --project-id iungo-prevendita --source-bucket cron_test_bucket --sink-bucket cron_test_bucket_dst --operation create
```
