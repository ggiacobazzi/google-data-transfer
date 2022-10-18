import argparse, random, string, os, google.auth, logging as log
from google.cloud import storage_transfer
from google.api_core.exceptions import NotFound, AlreadyExists


def create_one_time_transfer(project_id: str, source_bucket: str,
                             sink_bucket: str, job_name=None, description='Placeholder description'):

    client = storage_transfer.StorageTransferServiceClient()

    log.info(f"Creating job: {job_name}")
    
    path = f"{source_bucket}/"
    
    transfer_job_request = storage_transfer.CreateTransferJobRequest({
        'transfer_job': {
            'name': job_name,
            'description': description,
            'project_id': project_id,
            'status': storage_transfer.TransferJob.Status.ENABLED,
            'transfer_spec': {
                'gcs_data_source': {
                    'bucket_name': source_bucket,
                },
                'gcs_data_sink': {
                    'bucket_name': sink_bucket,
                    'path': path,
                },
                'transfer_options': {
                    'overwrite_when': 'DIFFERENT',
                    'metadata_options': {
                        'acl': 'ACL_DESTINATION_BUCKET_DEFAULT',
                        'storage_class':
                        'STORAGE_CLASS_DESTINATION_BUCKET_DEFAULT',
                        'temporary_hold': 'TEMPORARY_HOLD_PRESERVE',
                        'kms_key': 'KMS_KEY_DESTINATION_BUCKET_DEFAULT',
                        'time_created': 'TIME_CREATED_SKIP'
                    }
                }
            }
        }
    })

    transfer_job = client.create_transfer_job(transfer_job_request)
    client.run_transfer_job({
        'job_name': transfer_job.name,
        'project_id': transfer_job.project_id
    }) 

    log.info(f'Created and ran transfer job: {transfer_job.name}')


def run_existing_transfer(project_id: str, job_name: str):

    client = storage_transfer.StorageTransferServiceClient()

    log.info(f"Running job: {job_name}")

    client.run_transfer_job({
        'job_name': job_name,
        'project_id': project_id
    })

    log.info("Waiting for operation to complete...")
    log.info(f'Executed job: {job_name}')


def run_transfer(project_id: str, job_name: str, source_bucket: str, sink_bucket: str, description='Placeholder description'):

    if job_name is None:
        random_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
        job_name = 'transferJobs/' + random_str
    elif not job_name.startswith("transferJobs"):
        job_name = "transferJobs/" + job_name
    
    client = storage_transfer.StorageTransferServiceClient()

    try:
        response = client.get_transfer_job({
            'job_name': job_name,
            'project_id': project_id,
        })
        if response.status.__eq__("Status.DISABLED"):
            log.info("Updating job")
            client.update_transfer_job({
                "job_name": job_name,
                "project_id": project_id,
                "transfer_job": {
                    "status": storage_transfer.TransferJob.Status.ENABLED
                }
            })
        else:
            log.info('No update needed')
        run_existing_transfer(project_id, job_name)
    except NotFound:
        log.info("Creating job")
        create_one_time_transfer(project_id, source_bucket, sink_bucket, job_name, description)


if __name__ == '__main__':
    log.basicConfig(filename='copy_bucket.log', level=log.DEBUG)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--project-id',
        help='The ID of the Google Cloud Platform Project that owns the job')
    parser.add_argument(
        '--job-name',
        help='The name of the job')
    parser.add_argument(
        '--description',
        help='A useful description for your transfer job')
    parser.add_argument(
        '--source-bucket',
        help='Google Cloud source bucket name')
    parser.add_argument(
        '--sink-bucket',
        help='Google Cloud Storage destination bucket name')
    parser.add_argument(
        '--operation',
        help='Operation to do [create/run]')
    args = parser.parse_args()
    log.info(args)

    credentials, project_id = google.auth.default()

    if hasattr(credentials, "service_account_email"):
      print(credentials.service_account_email)
    else:
        print("WARNING: no service account credential. User account credential?")
    

    if args.operation is not None:
        if  args.operation.__eq__('create'):
            create_one_time_transfer(args.project_id, args.source_bucket, args.sink_bucket, job_name=args.job_name, description=args.description)
        elif args.operation.__eq__('run'):
            run_existing_transfer(args.project_id, args.job_name)
    else:
        run_transfer(args.project_id, args.job_name, args.source_bucket, args.sink_bucket, description=args.description)
    print("Done")