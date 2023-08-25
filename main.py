def fast_copy_to_redshift(file_name, delimiter):
    from manual_file_upload.s3_upload import upload_to_s3
    from manual_file_upload.copy_file import copy_file_to_redshift

    upload_to_s3(file_name)
    copy_file_to_redshift(file_name, file_name, delimiter)


fast_copy_to_redshift("MBKI_2023_08_04", ',')






