import datetime
import glob
import hashlib
import logging
import os
from typing import Tuple, List

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError


def upload_file_to_s3(relative_file_path: str, root_dir: str, bucket_name: str, s3_client: BaseClient) -> bool:
    full_file_path = os.path.join(root_dir, relative_file_path)

    try:
        object_name = relative_file_path.replace(os.path.sep, '/')
        s3_client.upload_file(full_file_path, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def sha256_local_file(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


def sha256_s3_file(bucket_name: str, object_name: str, s3_client: BaseClient) -> str:
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    return hashlib.sha256(s3_object['Body'].read()).hexdigest()


def compare_local_and_remote_file_hash(relative_file_path: str, root_dir: str, bucket_name: str,
                                       s3_client: BaseClient) -> bool:
    full_file_path = os.path.join(root_dir, relative_file_path)

    local_file_hash = sha256_local_file(full_file_path)
    object_name = relative_file_path.replace(os.path.sep, '/')
    s3_file_hash = sha256_s3_file(bucket_name, object_name, s3_client)

    return local_file_hash == s3_file_hash


class S3Uploader:
    def __init__(self, root_folder: str, dst_bucket_name: str, glob_pattern: str):
        self._root_folder = root_folder
        self._dst_bucket_name = dst_bucket_name
        self._glob_pattern = glob_pattern
        self._last_upload = None

    def __str__(self):
        return f'S3Uploader - root_folder:"{self._root_folder}", dst_bucket_name:"{self._dst_bucket_name}", ' \
               f'glob_pattern:"{self._glob_pattern}"'

    def _get_files_to_upload(self) -> List:
        start_upload_time = datetime.datetime.now()

        glob_result = glob.glob(self._glob_pattern, root_dir=self._root_folder, recursive=True)
        relative_path_to_full_path = {path: os.path.join(self._root_folder, path) for path in glob_result}

        # Filter only files
        glob_result_files = [relative_path for relative_path in glob_result if
                             os.path.isfile(relative_path_to_full_path[relative_path])]

        # If we have already uploaded files, only upload files which were later modified / created
        if self._last_upload is not None:
            last_upload_timestamp = self._last_upload.timestamp()
            glob_result_files = [relative_path for relative_path in glob_result_files if
                                 os.path.getmtime(relative_path_to_full_path[relative_path]) > last_upload_timestamp]

        self._last_upload = start_upload_time
        return glob_result_files

    def _upload_files_to_remote(self, file_paths: List[str], s3_client: BaseClient) -> Tuple[int, int]:
        success_counter = 0
        failure_counter = 0

        for file_path in file_paths:
            upload_success = upload_file_to_s3(file_path, self._root_folder, self._dst_bucket_name, s3_client)
            if not upload_success:
                failure_counter += 1
                continue

            validation_success = compare_local_and_remote_file_hash(file_path, self._root_folder, self._dst_bucket_name,
                                                                    s3_client)
            if not validation_success:
                failure_counter += 1
                continue

            success_counter += 1

        return success_counter, failure_counter

    def sync_local_and_remote_files(self) -> Tuple[int, int]:
        total_success_counter = 0
        total_failure_counter = 0
        s3_client: BaseClient = boto3.client('s3')

        files_to_upload = self._get_files_to_upload()

        # While there are files which were not yet uploaded
        while files_to_upload:
            success_counter, failure_counter = self._upload_files_to_remote(file_paths=files_to_upload,
                                                                            s3_client=s3_client)
            total_success_counter += success_counter
            total_failure_counter += failure_counter

            files_to_upload = self._get_files_to_upload()

        return total_success_counter, total_failure_counter
