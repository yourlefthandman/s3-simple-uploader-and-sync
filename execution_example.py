import requests

BASE_URL = 'http://127.0.0.1:5000'
UPLOAD_URL = BASE_URL + '/upload'
SYNC_URL = BASE_URL + '/sync'
SOURCE_FOLDER = 'D:\\source'


def format_response(response):
    return response.status_code, response.text.replace('\n', '')


# Add multiple values
request_json = {"upload_id": '1',
                "source_folder": SOURCE_FOLDER,
                'destination': 'ultima-ex-bucket',
                'pattern': '**\\a*.txt'}
print(f'Result:', format_response(requests.post(UPLOAD_URL, json=request_json)))

# Add multiple values
request_json = {"upload_id": '2',
                "source_folder": SOURCE_FOLDER,
                'destination': 'ultima-ex-bucket',
                'pattern': '**\\b*.txt'}
print(f'Result:', format_response(requests.post(SYNC_URL, json=request_json)))


# Add multiple values
request_json = {"upload_id": '3',
                "source_folder": SOURCE_FOLDER,
                'destination': 'ultima-ex-bucket',
                'pattern': '**\\c*.txt'}
print(f'Result:', format_response(requests.post(SYNC_URL, json=request_json)))
