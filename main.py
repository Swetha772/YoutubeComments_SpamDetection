import argparse
import logging
import re
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd


import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='rare-hub-399208-699e26cfe3c5.json'

class DataIngestion:
    ''' to fetch top 10 popular videos from youtube based on the input category '''
    def getPopularVideos(self,CategoryId,CategoryName):
        import pandas as pd
        import pandas_gbq
        from googleapiclient.discovery import build
        ''' Youtube API credentials '''
        api_key='AIzaSyBNbIyPHViaexn-DFJnO3oE-hHUt8OVEMo'
        api_service_name = 'youtube'
        api_service_version = 'v3'
        youtubeapi = build(serviceName = api_service_name,
                        version = api_service_version,
                        developerKey = api_key)


        popularvideos = pd.DataFrame(columns=['videoCategory','videoId','videoTitle','channelId','channelTitle'])
        request = youtubeapi.videos().list(part="snippet",
                                       chart="mostPopular",
                                       regionCode="in",
                                       videoCategoryId=CategoryId,
                                       maxResults=10
                                       )
        response=request.execute()
        for r in response['items']:
            res = dict({'videoCategory':CategoryName,
                        'videoId':r['id'],
                        'videoTitle':r['snippet']['title'],
                        'channelId':r['snippet']['channelId'],
                        'channelTitle':r['snippet']['channelTitle']
                       })
            popularvideos=popularvideos.append(res,ignore_index=True)
        return popularvideos
    
    ''' Function to fetch video comments for each videos of video category and perform spam detection '''
    def getVideoComments(self,videoCategoryId,videoCategoryName):
        import pandas as pd
        import pandas_gbq
        from utils.feature_extraction import FeatureExtraction
        from utils.naive_bayes import spam_detection
        from googleapiclient.discovery import build
        ''' Youtube API credentials '''
        api_key='AIzaSyBNbIyPHViaexn-DFJnO3oE-hHUt8OVEMo'
        api_service_name = 'youtube'
        api_service_version = 'v3'
        youtubeapi = build(serviceName = api_service_name,
                        version = api_service_version,
                        developerKey = api_key)

        print('---fetching the videos---')
        popularvideos=self.getPopularVideos(videoCategoryId,videoCategoryName)
        print('---fetching the videos end---')
        for i in range(len(popularvideos['videoId'])):
            videocomments_final = pd.DataFrame(columns=['videoCategory','videoId','videoTitle','channelTitle','Author','Comments'])
            print('video ',popularvideos.iat[i,1])
            print('videoTitle ',popularvideos.iat[i,2])
            video_Category = popularvideos.iat[i,0]
            video_ID = popularvideos.iat[i,1]
            video_Title = popularvideos.iat[i,2]
            channel_Title = popularvideos.iat[i,4]
            nextPageToken=''
            videocomments = pd.DataFrame(columns=['videoCategory','videoId','videoTitle','channelTitle','Author','Comments'])
            print('--start fetching the comments---')
            while(True):
                request = youtubeapi.commentThreads().list(part='snippet,replies',
                                                            videoId=video_ID,
                                                            textFormat='plainText',
                                                            pageToken=nextPageToken)
                response = request.execute()
                for r in response['items']:
                    comments=r['snippet']['topLevelComment']['snippet']['textDisplay']
                    author = r['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    res = dict({'videocategory':video_Category,
                                    'videoId':video_ID,
                                    'videoTitle':video_Title,
                                    'channelTitle':channel_Title,
                                    'Author':author,
                                    'Comments':comments})
                    videocomments = videocomments.append(res,ignore_index=True)
                    if('replies' in r.keys()):
                        for rs in r['replies']['comments']:
                            comments=rs['snippet']['textDisplay']
                            author=rs['snippet']['authorDisplayName']
                            res = dict({'videocategory':video_Category,
                                            'videoId':video_ID,
                                            'videoTitle':video_Title,
                                            'channelTitle':channel_Title,
                                            'Author':author,                                     
                                            'Comments':comments})
                            videocomments = videocomments.append(res,ignore_index=True)
                if(videocomments.shape[0]>10000):
                    print('fetching comments end')
                    break
                elif('nextPageToken' in response and len(response['items'])>0):
                    nextPageToken = response['nextPageToken']
                else:
                    print('fething comments end')
                    break
            print('---end fetching the comments ---')
            videocomments_final = pd.concat([videocomments_final,videocomments],axis=0)
            ''' calling the naive bayes function '''
            print('--starting the spam detection--')
            spam_res = spam_detection(videocomments)
            print('--spam detection ending--')
            ''' writing to bigquery '''
            spam_res=spam_res.astype(str)
            pandas_gbq.to_gbq(spam_res,"youtubedata_analytics.spamdetection", project_id="rare-hub-399208",if_exists="append")
                

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://rare-hub-399208/input/videocategories.csv')
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='youtubedata_analytics.spamdetection')
                        
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    # DataIngestion is a class we built in this script to hold the logic for transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()
    # Initiate the pipeline using the pipeline arguments passed in from thecommand line. 

    #pipeline to read videocategories, fetch popular videos and commments, perform spam detection and write resulst to GCS bucket
    p1 = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p1
     | 'Read videocategories from GCS'     >> beam.io.ReadFromText('gs://rare-hub-399208/input/videocategories.csv',skip_header_lines=1)
     | 'Fetch comments to perform spam detection and write to BigQuery' >> beam.Map(lambda s: data_ingestion.getVideoComments(str.split(s,',')[0],str.split(s,',')[1]))
     )
    p1.run().wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()