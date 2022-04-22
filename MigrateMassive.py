import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests
import json
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

rowsPer = 20000 # adjust these values to whatever works best code will pull 5 batches of 20,000 (5X20,000 = 100,000 +1 for good luck)
maxRows = 100001 # Google API can only handle so many uploads to a bucket so if this is small you will hit your quota fast. 

#Gets highest ID from google so it know where to continue pulling from 
def getMaxId():
    global x
    global credentials
    global key_path
    key_path = <path to client_secrets.json>

    credentials = Credentials.from_service_account_file(
         key_path,
         scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
         credentials=credentials,
         project=credentials.project_id
    )
    query = """SELECT MAX(id) maxid  FROM `<google BigQuery MassiveTable here>`"""
    df = client.query(query).to_dataframe()
    x = df.iloc[0,0]


def download_file(i):
    startID = str(i)
    apiUrl = 'https://<API endpoint>'
    apiKey = '<API Key>'
    headers = {"X-Key": apiKey, "Content-Type" : "application/json"}
    query = """SELECT top {} *
    from <MassiveTable>
    WHERE id >""".format(str(rowsPer))

    var = query + startID
    r = requests.post(apiUrl, data='"' + var + '"', headers=headers, stream=True)
    if r.text == '[]':
        print('Data Is Up to Date.')
        quit()
    res = r.json()
    
    #take json response from API and turn it into .csv
    file1 = "...Table\\Files\\dict_to_json_textfile" + str(i) + ".json"
    file2 = "...Table\\Files\\FileName_" + str(i) + ".csv"
    with open(file1, 'w') as fout:
        json_dumps_str = json.dumps(res, indent=4)
        print(json_dumps_str, file=fout)

    with open(file1) as json_file:
        data = json.load(json_file)
    data_file = open(file2, 'w', newline='')
    csv_writer = csv.writer(data_file, delimiter = ",")
    count = 0
    for d in data:
        if count == 0:
            # Writing headers of CSV file
            header = d.keys()
            csv_writer.writerow(header)
            count += 1
        # Writing data of CSV file
        csv_writer.writerow(d.values())
    data_file.close()
    os.remove(file1)

def uploadToCloudStorage(i):
    file2 = "...Table\\Files\\FileName_" + str(i) + ".csv"
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.get_bucket('<YOUR BUCKET>')
    blob = bucket.blob("FileName_" + str(i) + ".csv")
    blob.upload_from_filename(file2)
    #print("File" + str(i) + "uploaded")
    os.remove(file2)

def appendBQTable(): # takes batches of files and appends them to master file in Google BigQuery
    storage_client = storage.Client.from_service_account_json(key_path)
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id
    )
    table_ref = client.dataset('<YOUR BUCKET>').table('<YOUR MassiveTable>')

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    for i in range(int(x),x+maxRows,rowsPer):
        uri = "gs://<Your Bucket>/FileName_" + str(i) + ".csv"
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request
        load_job.result()  # Waits for table load to complete.
        destination_table = client.get_table(table_ref)

        bucket = storage_client.bucket('<Your Bucket>')
        blob = bucket.blob("FileName_" + str(i) + ".csv")
        try:
            blob.delete()
        except:
            pass


  #MultiThreading to make pulling and uploading much faster
def runnerDownload():
	threads= []
	with ThreadPoolExecutor(max_workers=20) as executor:
		for i in range(int(x),x + maxRows,rowsPer):
			threads.append(executor.submit(download_file, i))

def runnerUpload():
	threads= []
	with ThreadPoolExecutor(max_workers=10) as executor:
		for i in range(int(x),x + maxRows,rowsPer):
			threads.append(executor.submit(uploadToCloudStorage, i))


while True:
    try:
        getMaxId()
        print('Max ID is: ' + str(x) + ' Starting next batch......')
        runnerDownload()
        print('File Batch Downloaded')
        runnerUpload()
        print('Batch Files Uploaded')
        appendBQTable()
        print('Batch Data Appended')
    except IndexError as e: #when there are no new records that have a max ID greater than the last one pulled, throw an error and break
        print(e)
        break
