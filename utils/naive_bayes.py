from utils.feature_extraction import FeatureExtraction
import pandas as pd
import numpy as np


''' fetching corpus and spam key words from Feature Extraction module '''
corpus = FeatureExtraction.corpus
spam_keywords = list(FeatureExtraction.spam_keywords)

''' number of rows and columns '''
rows = corpus.shape[0]
cols = corpus.shape[1]

''' seperating spam and non spam comments '''
spam_df = corpus.loc[corpus['Class']==1]
nonspam_df = corpus.loc[corpus['Class']==0]
 
 
''' number of spam and non-spam comments '''
spam_num = spam_df.shape[0]
nonspam_num = nonspam_df.shape[0]


'''probability of spam and non-spam '''
p_spam = spam_num/rows
p_nonspam = nonspam_num/rows

print(p_spam,p_nonspam)

''' fetching column names in corpus '''
features = []
for c in corpus.columns.values:
    if c not in ['CommentID','Comment','Class']:
        features.append(c)

''' creating 2D numpy array for storing conditional probability '''
shape = (len(features),2)
arr = np.empty(shape)


''' for each feature calculting the conditional probability '''
for i in range(shape[0]):
    print(features[i])
    w = features[i]
    probability_spam = (spam_df.loc[spam_df[w]==1].shape[0]/spam_num)
    probability_nonspam = (nonspam_df.loc[nonspam_df[w]==1].shape[0]/nonspam_num)
    arr[i][0] = probability_nonspam
    arr[i][1] = probability_spam
    
    

for i in range(shape[0]):
    print(arr[i][0],arr[i][1])

res_df=pd.DataFrame(columns=['VideoCategory','VideoId','VideoTitle','ChannelTitle','Author','Comment','Class','SpamFeature'])


'''
videocomments = pd.read_csv('C:/Users/ADMIN/Desktop/YouTube Data API Project/Training Dataset/videocomments.csv', encoding = "unicode_escape")
'''

def spam_detection(video_comments):
    global res_df
    ''' Adding author+comment to check duplicate comments '''
    duplicate_comments=[]
    for i in range(video_comments.shape[0]):
        duplicate_comments.append(str(video_comments.iat[i,4])+str(video_comments.iat[i,5]).lower())
        
    ''' for each comment of a video '''
    for i in range(video_comments.shape[0]):
        ''' initializing the conditional probability variables, features and res dictionary '''
        cp_spam=p_spam
        cp_nonspam = p_nonspam
        features=dict()
        res=dict()
        res['VideoCategory']=video_comments.iat[i,6]
        res['VideoId']=video_comments.iat[i,1]
        res['VideoTitle']=str.replace(video_comments.iat[i,2],","," ")
        res['ChannelTitle']=str.replace(video_comments.iat[i,3],","," ")
        res['Author']=str.replace(video_comments.iat[i,4],","," ")
        res['Comment']=str.replace(video_comments.iat[i,5],","," ")
        res['Class']=0
        res['SpamFeature']=''
        
        comment = str(video_comments.iat[i,5]).lower()
        comment = comment.replace(':','')
        
        ''' deciding the feature spam keyword for each comment '''
        words = comment.split(' ')
        for w in spam_keywords:
            if w in words:
                features['spam keyword']=1
                break
        if 'spam keyword' not in list(features.keys()):
            features['spam keyword']=0
            
        ''' deciding feature for hyperlink for each comment '''
        if 'http' in comment or 'https' in comment:
            features['Hyperlink']=1
        elif  ('.com' in comment and '/' in comment) or ('.org' in comment and '/' in comment):
            features['Hyperlink']=1
        elif len([c for c in words if c.endswith('.com')==True or c.endswith('.org')==True]) >= 1:
            features['Hyperlink']=1
        else:
            features['Hyperlink']=0
        
        ''' deciding the feature comment length '''
        if(len(comment)>=200):
            features['Comment Length']=1
        else:
            features['Comment Length']=0
            
        ''' deciding the feature for Duplicate Comments '''
        if(duplicate_comments.count(str(res['Author'])+str(res['Comment']).lower())>1):
            features['Duplicate']=1
        else:
            features['Duplicate']=0
        
        ''' calculating conditional probability for spam and non spam based on features '''
        if(features['spam keyword']==1):
            cp_spam = cp_spam*arr[0][1]
            cp_nonspam = cp_nonspam*arr[0][0]
            res['SpamFeature']=res['SpamFeature']+'spamkeyword'
        
        if(features['Hyperlink']==1):
            cp_spam = cp_spam*arr[1][1]
            cp_nonspam = cp_nonspam*arr[1][0]
            res['SpamFeature']=res['SpamFeature']+' hyperlink'
        
        if(features['Duplicate']==1):
            cp_spam = cp_spam*arr[2][1]
            cp_nonspam = cp_nonspam*arr[2][0]
            res['SpamFeature']=res['SpamFeature']+' duplicate'
        
        if(features['Comment Length']==1):
            cp_spam = cp_spam*arr[3][1]
            cp_nonspam = cp_nonspam*arr[3][0]
            res['SpamFeature']=res['SpamFeature']+' commentlength'
            
        
        ''' comparing probability for spam and nonspam '''
        if(1 in list(features.values())):
            if(cp_spam > cp_nonspam):
                res['Class']=1
        
        res_df=res_df.append(res,ignore_index=True)
    print('length of res_df',res_df.shape[0])
    return res_df