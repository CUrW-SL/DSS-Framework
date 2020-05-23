from google.cloud import storage


def upload_file_to_bucket(key_file, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    client = storage.Client.from_service_account_json(key_file)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


if __name__ == "__main__":
    upload_file_to_bucket(
        key_file='/home/hasitha/uwcc-admin.json',
        bucket_name='wrf_nfs',
        source_file_name='/home/hasitha/PycharmProjects/DSS-Framework/output/station_rain.csv',
        destination_blob_name='results/station_rain.csv'
    )
