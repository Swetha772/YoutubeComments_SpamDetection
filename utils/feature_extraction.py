import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')
from collections import Counter

''' ['COMMENT_ID', 'AUTHOR', 'DATE', 'CONTENT', 'CLASS'], dtype='object') '''
class FeatureExtraction:
    
    ''' Loading csv file into a dataframe '''
    training_dataset = pd.read_csv('gs://rare-hub-399208/input/TrainingDataset/youtubeSpamCollection.csv', encoding = "unicode_escape")
    
    ''' Adding duplicate author+comments into a list '''
    duplicate_comments = []
    for i in range(training_dataset.shape[0]):
        duplicate_comments.append(training_dataset.iat[i,1]+training_dataset.iat[i,3].lower())
        
    
    ''' filtering records with spam comments to get words with high frequency '''
    spam_comments = training_dataset.loc[training_dataset['CLASS']==1]
    nonspam_comments = training_dataset.loc[training_dataset['CLASS']==0]
    
    
    ''' fixing the threshold for comment length to detect spam comments '''
    spam_len = []
    for i in range(spam_comments.shape[0]):
        spam_len.append(len(spam_comments.iat[i,3]))
    arr = np.array(spam_len)
    avg_len = int(np.average(arr))
    
    ''' breaking each comment into words '''
    words = []
    for i in range(spam_comments.shape[0]):
        words.extend(nltk.word_tokenize(spam_comments.iat[i,3].lower()))
    
    
    ''' removing stopwords and single letter words from the list of words '''
    stop_words = stopwords.words('english')
    clean_words = []
    for w in words:
        if w not in stop_words and len(w)>2 and w.isalpha() and w not in ['quot','amp','http','https']:
            clean_words.append(w)


    ''' calculaing the occurence of each word '''
    words_count = Counter(clean_words)
    words_frequency=pd.DataFrame(columns=['word','frequency'])
    for i in words_count:
        if i in ['song','music','video','like','new','people','would','give']:
            continue
        res = dict({'word':i, 'frequency':words_count[i]})
        words_frequency=words_frequency._append(res,ignore_index=True)
        
    ''' sorting the frequency dataframe based on the frequency of each word desc order'''        
    words_frequency = words_frequency.sort_values('frequency',ascending=False)
    
    ''' fetching the top 25 words with highest frequency '''
    top_wordsfrequency = pd.DataFrame(columns=['word','frequency'])
    for i in range(19):
        res = dict({'word':words_frequency.iat[i,0], 'frequency':words_frequency.iat[i,1]})
        top_wordsfrequency = top_wordsfrequency._append(res,ignore_index=True)
    
    spam_keywords = top_wordsfrequency['word']
    ''' building corpus for training dataset with top word with highest frequency '''
    corpus_cols = ['CommentID','Comment','Spam Keyword','Hyperlink','Duplicate Comment','Comment_Length','Class']

    corpus = pd.DataFrame(columns=corpus_cols)
    
    ''' for each comment in training dataset '''
    for i in range(training_dataset.shape[0]):
        res = dict()
        res['CommentID']=training_dataset.iat[i,0]
        res['Comment']=training_dataset.iat[i,3]
        
        '''storing comment into variable '''
        comment = training_dataset.iat[i,3].lower()
        comment = comment.replace(':','')
        comment_list=comment.split(' ')
        
        ''' for each word with top frequency if spam keyword present then 1 '''
        for j in range(top_wordsfrequency.shape[0]):
            search_word = top_wordsfrequency.iat[j,0]
            if(search_word in comment_list):
                res['Spam Keyword']=1
                break
        
        ''' If no spam keyword present in comment then 0 '''
        if('Spam Keyword' not in res.keys()):
            res['Spam Keyword']=0

        ''' Hyperlink feature for spam '''
        if 'http' in comment or 'https' in comment:
            res['Hyperlink']=1
        elif  ('.com' in comment and '/' in comment) or ('.org' in comment and '/' in comment):
            res['Hyperlink']=1
        elif len([c for c in comment_list if c.endswith('.com')==True or c.endswith('.org')==True]) >= 1:
            res['Hyperlink']=1
        else:
            res['Hyperlink']=0
        
        ''' checking if author is duplicated across comments '''
        if(duplicate_comments.count(training_dataset.iat[i,1]+training_dataset.iat[i,3].lower())>1):
            res['Duplicate Comment']=1
        else:
            res['Duplicate Comment']=0
        
        ''' checking the length of the comments based on the avg length '''
        if(len(training_dataset.iat[i,3])>=avg_len):
            res['Comment_Length']=1
        else:
            res['Comment_Length']=0
        
        res['Class']=training_dataset.iat[i,4]
        corpus = corpus._append(res,ignore_index=True)

    ''' printing shape of corpus df '''
    print('shape of corpus')
    print(corpus.shape[0],corpus.shape[1])
    
          
            
            
        
    
    
    
   