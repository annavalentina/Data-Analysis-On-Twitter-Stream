# -*- coding: utf-8 -*-
from __future__ import division
import tweepy
import json
from pymongo import MongoClient

##INSERT KEYS AND TOKENS##
consumer_key=
consumer_secret=
access_token=
access_token_secret=


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

try:
    redirect_url = auth.get_authorization_url()
    print 'SUCCESS'
except tweepy.TweepError:
    print 'Error! Failed to get request token.'

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

client = MongoClient('localhost', 27017)
db = client['client']

collection = db["tweets"]
#####PART 1#####
#Gets the top trends in the usa 
topTrends=api.trends_place(23424977)
#how to get the top 5 trends off the list: topTrends
i = 0
topTrendsList=[None]*5
while i < 5:
    print topTrends[0]['trends'][i]['name']
    print ''
    #Appends trend in a top-5 list
    topTrendsList[i]=topTrends[0]['trends'][i]['name']
    collection.insert({"name":topTrendsList[i]})
    i += 1




#####PART 2#####
class StreamListener(tweepy.StreamListener):    

    def on_connect(self):
        self.num_tweets = 0#Number of tweets collected
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
 
    def on_error(self, status_code):
        # On error - if an error occurs, displays the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
 
    def on_data(self, data):
        #Saves data to database
        try:
          
            # Takes JSON from Twitter
            datajson = json.loads(data)
            
            if (not datajson['retweeted']) and ('RT @' not in datajson['text']):#If it isn't a retweet                       
                collection = db["collection"+`i`]#Creates a collection
                
                if self.num_tweets < 1500:#If less than 1500 tweets are collected
                  collection.insert(datajson)#Inserts tweet to database
                  self.num_tweets += 1
                  return True
                else:
                  self.num_tweets=0#All tweets collected
                  return False
            
        except Exception as e:
            print(e)


#Searches for tweets for every topic(from the top-5 list)
i=0
listener = StreamListener(api) 
streamer = tweepy.Stream(auth=auth, listener=listener)
for topic in topTrendsList:#For every topic
    streamer.filter(languages=["en"],track=[topic])
    i+=1      
    
    