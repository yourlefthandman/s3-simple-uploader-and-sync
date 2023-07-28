from typing import List

from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler

from s3_uploader import S3Uploader

app = Flask(__name__)

SYNC_INTERVAL_SECONDS = 10
periodic_s3_uploaders: List[S3Uploader] = []


@app.post('/upload')
def upload_glob_to_s3():
    req = request.json
    upload_id: str = req.get('upload_id')
    root_folder: str = req.get('source_folder')
    dst_bucket_name: str = req.get('destination')
    glob_pattern: str = req.get('pattern')

    # Validate inputs
    inputs_to_validate = [upload_id, root_folder, dst_bucket_name, glob_pattern]
    input_types = {type(i) for i in inputs_to_validate}
    if input_types != {str}:
        return {'error': 'Invalid input'}, 400

    s3_uploader = S3Uploader(root_folder, dst_bucket_name, glob_pattern)
    success_count, failure_count = s3_uploader.sync_local_and_remote_files()

    return {'message': 'Finished uploading files', 'upload_id': upload_id,
            'file_upload_success_count': success_count,
            'file_upload_failures_count': failure_count, 'total_files': success_count + failure_count}, 200


@app.post('/sync')
def sync_glob_to_s3():
    req = request.json
    upload_id: str = req.get('upload_id')
    root_folder: str = req.get('source_folder')
    dst_bucket_name: str = req.get('destination')
    glob_pattern: str = req.get('pattern')

    # Validate inputs
    inputs_to_validate = [upload_id, root_folder, dst_bucket_name, glob_pattern]
    input_types = {type(i) for i in inputs_to_validate}
    if input_types != {str}:
        return {'error': 'Invalid input'}, 400

    s3_uploader = S3Uploader(root_folder, dst_bucket_name, glob_pattern)
    periodic_s3_uploaders.append(s3_uploader)

    return {'message': 'Registered sync', 'upload_id': upload_id}, 200


def run_s3_uploaders_sync():
    for s3_uploader in periodic_s3_uploaders[:]:
        success_count, failure_count = s3_uploader.sync_local_and_remote_files()
        print(f'Finished sync: success_count: {success_count}, failure_count: {failure_count} for ({s3_uploader})')


if __name__ == '__main__':
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(run_s3_uploaders_sync, 'interval', seconds=SYNC_INTERVAL_SECONDS)
    scheduler.start()

    app.run(debug=False)
