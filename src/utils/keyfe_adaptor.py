import json
import os
import urllib
from base64 import b64decode
from urllib import request

import boto3
from botocore.client import Config


# get parameter values
username = ''
password = ''
xapikey = '3sIW0vMvw21ATIuG6lduh22iAmpLsNpV2Hlq6HPp'

class KeyfeAdaptor:
    s3_local_resource = boto3.resource(
        's3'
    )

    # get parameter values
    username = 'dhuris'
    password = 'Coldmonk09#'
    xapikey = '3sIW0vMvw21ATIuG6lduh22iAmpLsNpV2Hlq6HPp'
    # ENCRYPTED_password = 'Coldmonk09#' #os.environ['password']
    # ENCRYPTED_xapikey = '3sIW0vMvw21ATIuG6lduh22iAmpLsNpV2Hlq6HPp' #os.environ['xapikey']

    # Decrypt code should run once and variables stored outside of the function
    # handler so that these are decrypted once per container
    # password = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_password))['Plaintext']
    # xapikey = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_xapikey))['Plaintext']


    # get remote credentials and create resource
    def remote_s3_resource(username, password, xapikey):
        req_headers = {"content-type": "application/json", "x-api-key": xapikey}
        url = 'https://vhk52gm1z1.execute-api.us-east-1.amazonaws.com/prod/service'
        input_fields = {"username": username, "password": password, "datatype": "keyfe"}
        params = json.dumps(input_fields).encode('utf8')

        req = request.Request(url, headers=req_headers)
        resp = request.urlopen(req, params).read().decode('utf-8')

        resp_json_str = json.loads(resp)
        resp_json_obj = json.loads(resp_json_str)
        resp_body_obj = json.loads(resp_json_obj['body'])

        access_key_id = resp_body_obj['accessKeyId']
        secret_access_key = resp_body_obj['secretAccessKey']
        session_token = resp_body_obj['sessionToken']

        print("Successfully obtained remote credentials")

        s3_remote_resource = boto3.resource(
        's3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        config=Config(signature_version='s3v4')
        )
        return s3_remote_resource

    # get scival key from usage bucket and key
    def getScivalKey(bucketKey, usageType, keyPrefix):
        # get usage type and file name and build scival key string
        wordList = bucketKey.split("/")

        fileNameIndex = len(wordList) - 1
        fileName = wordList[fileNameIndex]

        fileSplit = fileName.split(".")
        newFileName = fileSplit[0] + '.csv.gz'

        scivalKey = keyPrefix + newFileName
        return (scivalKey)


    def lambda_handler(event, context):
        print("Received event: " + json.dumps(event, indent=2))
        usageMetadataSer = bucketName = event['Records'][0]['Sns']['Message']
        usageMetadata = json.loads(usageMetadataSer)
        bucketName = usageMetadata['keyfeFilesAvailable']['bucket']
        bucketKeyTmp = usageMetadata['keyfeFilesAvailable']['name']

        bucketKey1 = urllib.unquote_plus(bucketKeyTmp).decode('utf8')
        bucketKey = bucketKey1[:-1]
        try:

            print(usageMetadata)
            print(bucketName)
            print(bucketKey)

            # get usage type
            wordList = bucketKey.split("/")
            usageType = wordList[0]

            # get scival key prefix depending on usage type
            newkeyPrefix = ''
            if usageType == 'sd':
                print("SD")
                newkeyPrefix = 'usage/sd/staging/'
            else:
                if usageType == 'sc':
                    print("scopus")
                    newkeyPrefix = 'usage/scopus/staging/'
                else:
                    print("Error in usage type")

            s3_remote_resource = remote_s3_resource()

            fileList = s3_remote_resource.meta.client.list_objects_v2(Bucket=bucketName, Prefix=bucketKey)
            print(fileList['Contents'])
            # handle multiple files
            for item in fileList['Contents']:
                usagekeyName = item['Key']
                scivalKeyName = KeyfeAdaptor.getScivalKey(usagekeyName, usageType, newkeyPrefix)
                # dowload file to local file system
                download_result = s3_remote_resource.meta.client.download_file(bucketName, usagekeyName, '/tmp/tmpfile')
                # upload file to scival s3 bucket
                upload_result = s3_local_resource.meta.client.upload_file('/tmp/tmpfile', 'scivaldata', scivalKeyName)
                print(usagekeyName)
                print(scivalKeyName)

            return ("success")

        except Exception as e:
            print(e)
            raise e


    def get_data(usageType='SD'):
        
        # usageMetadataSer = 'keyfe'
        # usageMetadata = json.loads(usageMetadataSer)
        # bucketName = usageMetadata['keyfeFilesAvailable']['bucket']
        # bucketKeyTmp = usageMetadata['keyfeFilesAvailable']['name']

        # bucketKey1 = urllib.unquote_plus(bucketKeyTmp).decode('utf8')
        # bucketKey = bucketKey1[:-1]
        try:

            #print(usageMetadata)
            #print(bucketName)
            #print(bucketKey)
            bucketName = 'keyfe'

            # get scival key prefix depending on usage type
            newkeyPrefix = ''
            if usageType == 'sd':
                print("SD")
                newkeyPrefix = 'usage/sd/staging/'
            else:
                if usageType == 'sc':
                    print("scopus")
                    newkeyPrefix = 'usage/scopus/staging/'
                else:
                    print("Error in usage type")

            s3_remote_resource = KeyfeAdaptor.remote_s3_resource(username, password, xapikey)

            fileList = s3_remote_resource.meta.client.list_objects_v2(Bucket=bucketName, Prefix=bucketKey)
            
            # handle multiple files
            for item in fileList['Contents']:
                usagekeyName = item['Key']
                scivalKeyName = KeyfeAdaptor.getScivalKey(usagekeyName, usageType, newkeyPrefix)
                # dowload file to local file system
                download_result = s3_remote_resource.meta.client.download_file(bucketName, usagekeyName, '../data/tmp/tmpfile')
                # upload file to scival s3 bucket
                #upload_result = s3_local_resource.meta.client.upload_file('/tmp/tmpfile', 'scivaldata', scivalKeyName)
                print(usagekeyName)
                print(scivalKeyName)

            return ("success")

        except Exception as e:
            print(e)
            raise e

        
