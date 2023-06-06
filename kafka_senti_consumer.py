# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 10:53:51 2023

@author: krishna
"""

# Import Libraries
from textblob import TextBlob
import sys
import tweepy
import matplotlib
matplotlib.use('Agg')  # Set the backend to 'Agg'
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import nltk
import re
import string
import json
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import SnowballStemmer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from kafka import KafkaConsumer
import json, time
import ConfigParser

config_parser = ConfigParser.ConfigParserUtility(os.path.join(os.getcwd(), "config.txt"))
config_parser_dict = config_parser.parse_configfile()


topic_name = config_parser_dict['topic_name'].strip()
tweets_batch_size = 100

def percentage(part,whole):
    return 100 * float(part)/float(whole) 

class SentiConsumer():
    
    def __init__(self):
        '''
        Default constructor to create
        kafka consumer instance

        Returns
        -------
        None.

        '''
        # create consumer config
        self.consumer = KafkaConsumer( topic_name,
                                      bootstrap_servers=[f'{config_parser_dict["kafka_server_ip"].strip()}:9092'],
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      auto_commit_interval_ms =  5000,
                                      fetch_max_bytes = 128,
                                      max_poll_records = 100,
                                     )
        
    def create_wordcloud(self, text, sentiment):
        '''
        Func to create word cloud
        and save as png files in flask static image resources 
        Parameters
        ----------
        text : DataFrame
            List of words.
        sentiment : string
            Type of sentiment e.g. positive, negative, neutral words.

        Returns
        -------
        None.

        '''
        #mask = np.array(Image.open("cloud.png"))
        stopwords = set(STOPWORDS)
        wc = WordCloud(background_color="white",
                       #mask = mask,
                       max_words=3000,
                       stopwords=stopwords,
                       width = 1000, height = 200,
                       repeat=True)
        wc.generate(str(text))
        os.remove(os.path.join(config_parser_dict["static_dir"].strip(), f"wordcloud_{sentiment}.png"))
        wc.to_file(os.path.join(config_parser_dict["static_dir"].strip(), f"wordcloud_{sentiment}.png"))
        print("Word Cloud Saved Successfully")
        
        #path="wc.png"
        #display(Image.open(path))

    def sentiment_analysis(self, tweets):
        '''
        Func to perform sentiment analysis
        on tweets

        Parameters
        ----------
        tweets : list
            list of random tweets.

        Returns
        -------
        None.

        '''
        positive  = 0
        negative = 0
        neutral = 0
        polarity = 0
        tweet_list = []
        neutral_list = []
        negative_list = []
        positive_list = []

        for tweet in tweets:

            #print(tweet.text)
            tweet_list.append(tweet)
            analysis = TextBlob(tweet)
            score = SentimentIntensityAnalyzer().polarity_scores(tweet)
            neg = score['neg']
            neu = score['neu']
            pos = score['pos']
            comp = score['compound']
            polarity += analysis.sentiment.polarity

            if neg > pos:
                negative_list.append(tweet)
                negative += 1

            elif pos > neg:
                positive_list.append(tweet)
                positive += 1

            elif pos == neg:
                neutral_list.append(tweet)
                neutral += 1

        positive = percentage(positive, tweets_batch_size)
        negative = percentage(negative, tweets_batch_size)
        neutral = percentage(neutral, tweets_batch_size)
        polarity = percentage(polarity, tweets_batch_size)
        positive = format(positive, '.1f')
        negative = format(negative, '.1f')
        neutral = format(neutral, '.1f')
        
        #Number of Tweets (Total, Positive, Negative, Neutral)
        tweet_list = pd.DataFrame(tweet_list)
        neutral_list = pd.DataFrame(neutral_list)
        negative_list = pd.DataFrame(negative_list)
        positive_list = pd.DataFrame(positive_list)
        print("total number: ",len(tweet_list))
        print("positive number: ",len(positive_list))
        print("negative number: ", len(negative_list))
        print("neutral number: ",len(neutral_list))
        
        print("Total no of tweets processed {}".format(len(tweets)))
        
        #Creating PieCart
        labels = ['Positive ['+str(positive)+'%]' , 'Neutral ['+str(neutral)+'%]','Negative ['+str(negative)+'%]']
        sizes = [positive, neutral, negative]
        colors = ['yellowgreen', 'blue','red']
        patches, texts = plt.pie(sizes,colors=colors, startangle=90)
        plt.style.use('default')
        plt.legend(labels)
        plt.title("Sentiment Analysis Result")
        plt.axis('equal')
        #plt.show()
        # save pie chart
        os.remove(os.path.join(config_parser_dict["static_dir"].strip(),'pieplot.png'))
        plt.savefig(os.path.join(config_parser_dict["static_dir"].strip(),'pieplot.png'), transparent=True)
        # Clear the plot
        plt.clf()
        
        # create word cloud
        print("Word cloud for positive words")
        self.create_wordcloud(positive_list, "pos")
        print("Word cloud for negative words")
        self.create_wordcloud(negative_list, "neg")
        print("Word cloud for neutral words")
        self.create_wordcloud(neutral_list, "neu")
    
    def process(self):
        
        batch_messages = []
        batch_size = 0
        
        for message in self.consumer:
            tweet = message.value
            batch_messages.append(str(tweet))
            batch_size += 1
            
            if batch_size>=tweets_batch_size:
                # perform sentiment analysis on batch messages
                print(f"New iteration........................................................... {batch_size} {tweets_batch_size}")
                print(batch_messages[-1])
                self.sentiment_analysis(batch_messages)
                batch_size = 0 # reset batch
                batch_messages = []
                break
                
def consumer():
    
    senti_consumer = SentiConsumer()
    senti_consumer.process()
    senti_consumer.consumer.close()
    
    
# driver code
if __name__=='__main__':
    args = sys.argv[1:]
    if args[0]=='consumer':
        consumer()
    else:
        print("Invalid argument...")
