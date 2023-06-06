# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 10:51:50 2023

@author: krishna
"""

from tweepy.streaming import Stream
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import random
import os, sys, datetime, time
import ConfigParser


config_parser = ConfigParser.ConfigParserUtility(os.path.join(os.getcwd(), "config.txt"))
config_parser_dict = config_parser.parse_configfile()

"""Code to stimulate realtime twitter api data"""

class TwitterAPIStimulator():
    
    def __init__(self, topic_name):
        '''
        default constructor 
        creates kafka producer client object
        loads list of offline tweets
        uses kaggle dataset

        Parameters
        ----------
        topic_name : string
            name of the kafka topic; mentioned in config file.

        Returns
        -------
        None.

        '''
        
        self.producer = KafkaProducer(bootstrap_servers=f'{config_parser_dict["kafka_server_ip"].strip()}:9092') #Same port as your Kafka server
        self.topic_name = topic_name
        
        # set up Kafka admin client
        self.admin_client = KafkaAdminClient(bootstrap_servers=[f'{config_parser_dict["kafka_server_ip"].strip()}:9092'],
                                             client_id='admin'
                                            )
        # create kafka topic
        self.kafka_create_topic()
        
        self.offline_tweets = open("offlineTweets.csv", 'r').readlines()
        print(f"No of tweets loaded: {len(self.offline_tweets)}")
    
    def random_tweets(self):
        '''
        generates 100 random tweets
        returns: list -> list of tweets

        Returns
        -------
        random_tweets : list
            100 random tweets.

        '''
        
        stime = time.time()
        random_tweets = []
        counter = 0
        while counter<=100:
            index = random.randint(0, len(self.offline_tweets))
            random_tweets.append(self.offline_tweets[index])
            self.offline_tweets.pop(index)
            counter += 1
        print(f"Total time to generate 100 random tweets : {time.time()-stime}")
        
        return random_tweets
    
    def kafka_delete_topic(self):
        '''
        func to delete a topic using admin api

        Returns
        -------
        None.

        '''
            
        # Create the topic deletion request
        delete_request = self.admin_client.delete_topics(topics=[self.topic_name])
        time.sleep(30)
                
        # Verify that the topic has been deleted
        if self.topic_name not in self.admin_client.list_topics():
            print(f"Topic {self.topic_name} has been successfully deleted.")
        else:
            print(f"Failed to delete topic {self.topic_name}.")
            
            
    
    def kafka_create_topic(self):
        '''
        func to create topic
        using kafka admin client api

        Returns
        -------
        None.

        '''
        
        topic_list = self.admin_client.list_topics()
        # Filter out internal topics that start with "__" ; -> example filters any consumer offsets
        if self.topic_name not in [t for t in topic_list if not t.startswith('__')]:
            print(f"Creating a new topic {self.topic_name}")
            # create new topic configuration
            partitions = 3
            replication_factor = 1
            new_topic = NewTopic(name=self.topic_name, 
                                 num_partitions=partitions, 
                                 replication_factor=replication_factor)

            # create new topic
            self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        else:
            print(f"{self.topic_name} already exists....")

    
    def kafka_twitter_producer(self):
        '''
        Func to publish data streams
        to kafka cluster

        Returns
        -------
        None.

        '''
        
        stime = time.time()
        for line in self.random_tweets():
            # producer with callbacks
            self.producer.send( self.topic_name, 
                               str.encode("".join(line.split("NO_QUERY")[-1].split(",")[2:]) )).add_callback(
                                lambda x: print('Message delivered to partition {} with offset {}'.format(
                                    x.partition, x.offset))).add_errback(
                                        lambda excp: print (f"Failed to send message: {excp}"))
            self.producer.flush()
            
        print(f"Total time to publish 100 random tweets : {time.time()-stime}")
        
def producer():
    
    twitteroffline = TwitterAPIStimulator(topic_name = config_parser_dict['topic_name'].strip())
    
    twitteroffline.kafka_twitter_producer() # generating 100 tweets randomly
    
    twitteroffline.producer.close()

    twitteroffline.admin_client.close()
 
# driver code
if __name__=='__main__':
    args = sys.argv[1:]
    if args[0]=='producer':
        producer()
    else:
        print("Invalid argument...")
        

#twitteroffline.kafka_delete_topic() # delete a topic