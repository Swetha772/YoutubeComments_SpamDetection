**Youtube Comments Spam Detection using Python**

**Overview**
Many people have been using youtube as a platfor to share their content online and people are posting comments under videos to express their opinions. However thera are so many comments that contain hyperlinks, unwanted advertisements which are classified as spam comments. In this project I am using naive bayes classification to detect spam comments by fetching the popular videos of different video categories using youtube Data API.

**For Data Processing and Data storage I am using**
-> Cloud Bigquery
-> Dataflow
-> Apache Beam SDK
-> Python

**For Data Visualization I am using**
-> Power BI 

**Steps**
**Connect to Yotube Data API** 
Enable Youtube Data API in Google Cloud Console. Generate API Key and save it. Pass this API Key in python code to fetch data from Youtube Data API

**Install Below Packages**
-> In Google cloud shell , install Apache Beam SDK.
-> While running jobs in Dataflow, below packages to be installed:
   pandas_gbq
   fsspec
   gcsfs

**Feature Extraction using Training Dataset**
-> I have downloaded the training dataset from kaggle
https://www.kaggle.com/datasets/fatihblg/youtube-spam-collection/code?datasetId=3309142&sortBy=dateRun&tab=profile

->For naive bayes classification, build the corpus using training dataset by incluing below features.
  Spam Keyword: Using spam comments of training dataset, words with highest frequency are considered as spam keywords.
  Hyperlink: Comments with hyperlink that might lead to spam websites, unwanted advertisements
  Duplicate: Multiple comments from same Author and content
  Comment Length: Length of spam comments are relatively high to length of non spam comments.

**ETL Pipeline in Google Cloud Dataflow**
-> Fetch the video categories from Youtube Data API, store this in csv file in google cloud storage bucket.
-> Grant all the required permissions through roles to service account through googl cloud IAM and Admin.
-> Write code that build ETL pipeline using Apache Beam SDK Python.
   Input for the pipeline: videocategories in google cloud storage bucket
-> Submit the job to google cloud dataflow from cloud shell:
   python main.py --project=rare-hub-399208 --job_name=spamdetection-job3 --region=us-east1 --runner=DataflowRunner --temp_location=gs://rare-hub-399208/test --staging_location=gs://rare-hub-399208/test --service_account_email=129031517274-compute@developer.gserviceaccount.com --requirements_file=requirements.txt --setup_file=./setup.py

-> While pipeline is running, for each video category, it fetches the top 10k comments
-> Then Spam detection will be done using naive bayes classification algorithm.
   comments are classified as either 0 or 1.
   0 - spam
   1 - non spam
-> Finally write to bigquery using pandas_gbq

**Data Visualization**
-> Power BI dashboard is built by integrating power BI with Bigquery.
-> Below is the link for the final dashboard:
   https://app.powerbi.com/groups/me/reports/f5b12a8e-1602-4bb5-acdb-8280b6f5ef94/ReportSection?experience=power-bi

   ![image](https://github.com/Swetha772/YoutubeComments_SpamDetection/assets/66777487/f04b59c3-f7b3-4f6b-b5ae-8002278ba270)

