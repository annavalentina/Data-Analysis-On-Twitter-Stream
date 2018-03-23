from __future__ import division
import json
from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
import re
import numpy as np
import matplotlib.pyplot as plt
import requests
from itertools import *
from pylab import *
import sys
from scipy.stats import norm

#####PART 4#####
#Function to print the plots of the most frequent terms    
def printPlots(word_counter,topic,str):
    words=[]#Holds the words
    freq=[]#Holds the frequency of the words
    word_counter3=sorted(word_counter,key=word_counter.get,reverse=True)[:50] #Sorts the dict by the frequency and takes the 10 top items
    for j,word in enumerate(word_counter3):
          words.append(word)
          freq.append(word_counter[word])
    #Sets the plot attributes
    plt.figure("50 Most Frequent", figsize=(9, 13))
    plt.barh(range(len(word_counter3)), freq, align='center')
    plt.yticks(range(len(word_counter3)),words)       
    plt.suptitle('50 Most Frequent Terms of '+topic+str)
    plotTitle="50 Most Frequent Terms of "+topic+str+".png"
    plt.savefig(plotTitle)
    plt.show()
 
#####PART 4#####    
#Function to print the polarity pie    
def polarityPie(i):
        collection = db["collection"+`i`]#Gets a collection (topic)
        cursor = collection.find({})#Gets the tweets in that topic
        negative=0
        positive=0
        neutral=0
        sum=0#Counter for the tweets
        #For each tweet in collection
        for document in cursor:
            if(document["empty_text"]=="false"):#If there is text
                sum+=1
                if (document["label"]=="neutral"):
                    neutral+=1
                elif (document["label"]=="pos"):
                    positive+=1
                else:
                    negative+=1
        #Finds average of each label
        neutral=neutral/sum
        positive=positive/sum
        negative=negative/sum
        
        #Sets plot's attributes
        labels = 'Positive','Negative','Neutral'
        sizes = [positive, negative, neutral]
        colors = ['yellowgreen', 'gold', 'lightskyblue']
        
        plt.pie(sizes, labels=labels, colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=90)
        plt.axis('equal')
        plt.title("Polarity Pie of "+ topic)
        plotTitle="Polarity Pie of "+topic+".png"
        plt.savefig(plotTitle)
        plt.show()


#####PART 4#####
#Function to print the zipf plot
def plotZipf(word_counter,topic):
   
    counts = array(word_counter.values())#Holds the frequencies of the words
    tokens = word_counter.keys()#Holds the words

	#Sets plot's arguments
    ranks = arange(1, len(counts)+1)
    indices = argsort(-counts)
    frequencies = counts[indices]
    loglog(ranks, frequencies, marker=".")
    title("Zipf plot for "+topic)
    xlabel("Frequency rank of token")
    ylabel("Absolute frequency of token")
    grid(True)
    for n in list(logspace(-0.5, log10(len(counts)-1), 20).astype(int)):
        dummy = text(ranks[n], frequencies[n], " " + tokens[indices[n]], 
                 verticalalignment="bottom",
                 horizontalalignment="left")
    
    plotTitle="Zipf plot for "+topic+".png"
    plt.savefig(plotTitle)
    show()


#####PART 4#####
#Function that sends a tweet to the sentiment API
def sentimentAnalysis(str,document):
    if str:#If it isn't empty
        collection .update_one({'_id' :document["_id"]}, {'$set': {'empty_text': "false"}})#Sets the empty_text slot as FALSE (this is useful in the polarity pie function)
        r = requests.post("http://text-processing.com/api/sentiment/", data={'text': str})#Sends text to API
        if(r.status_code!=503):#If it returns a result
            sentiment = json.loads(r.text)#Gets the json 
            #Updates the database
            collection.update_one({'_id': document["_id"]}, {
                    '$set': {'label': sentiment["label"], 'positive_probability': sentiment["probability"]["pos"],
                             'negative_probability': sentiment["probability"]["neg"],
                             'neutral_probability': sentiment["probability"]["neutral"]}})  # adds sentiment to the mongo document
        else:
            print "Error 503: Over the daily limit. Try again later."
            sys.exit()
    else:
        collection .update_one({'_id' :document["_id"]}, {'$set': {'empty_text': "true"}})#Sets the empty_text slot as TRUE




#####PART 5#####          
#Function to analyse users
def userAnalysis(db,i,topic):
    
   ratios=[]#List to hold the ratios between friends and followers
   collectionName=db["collection"+`i`]#Gets a collection(topic)
   textFile= "collection"+`i`+".txt"
   text_file = open(textFile, "w")#Creates or opens a file
   #For each user (user_id) it finds the average of negative, positive, neutral probability and the number of friends and followers
   result=collectionName.aggregate(
    [
     {
                     "$group": {
                     "_id":"$user.id_str",
                      "avgNeg":{"$avg":"$negative_probability"},
                      "avgPos":{"$avg":"$positive_probability"},
                      "avgNeut":{"$avg":"$neutral_probability"},
                      "friends":{"$push":"$user.friends_count"},
                      "followers":{"$push":"$user.followers_count"}
                      }         
                     }                                      
                ]
        )
    
   #For each user 
   for userId in result:
       #Finds the final sentiment
        if (userId["avgPos"]>userId["avgNeg"]) and (userId["avgPos"]>userId["avgNeut"]):
                         label="Positive"
        elif (userId["avgNeg"]>userId["avgPos"]) and (userId["avgNeg"]>userId["avgNeut"]):
                          label="Negative"
        elif(userId["avgNeut"]>userId["avgPos"]) and (userId["avgNeut"]>userId["avgNeg"]):
                          label="Neutral"
        else:
                          label="Undefined"
        #Writes sentiment to file
        text_file.write("User: "+userId["_id"]+" Label: "+label+"\n")
        #Finds followers/friends ratio
        if(userId["friends"][0]!=0):
                ratio=userId["followers"][0]/userId["friends"][0]
        else:
                ratio=0
        ratios.append(ratio)#Appends ratio to list
        collectionName.update_many({"user.id_str":userId["_id"]}, {"$set": {'final_label':label, 'ratio':ratio} })#Updates the database
    
   #Prints the CDF diagram  
   sortedRatio = np.sort(ratios)
   c = norm.cdf(sortedRatio)
   plt.plot(c)
   plt.suptitle('Cumulative Distribution Function of '+topic)
   plotTitle="Cumulative Distribution Function of "+topic+".png"
   plt.savefig(plotTitle)
   plt.show()
   text_file.close()
   
       
#Creates the database client    
client = MongoClient('localhost', 27017)
db = client['client']
collection = db["tweets"]#This collection holds the topics

topTrendsList=[None]*5#List to hold the topics
i=0
cursor = collection.find({})#Gets the topics and puts them in the list
for document in cursor:
    topTrendsList[i]=document["name"]
    i+=1
  
#####PART 3#####
nltk.download('stopwords')#Downloads the stopwords
stopwords = set(stopwords.words('english'))
i=0
word_counter = {}#Holds the words of a tweet
word_counter2 = {}#Holds the words of a tweet after the processing
final_tweet=[]#Holds all the 'clean' words from a tweet

for topic in topTrendsList:#For each topic
        clean_topic = re.sub('\W|\d|_', '', topic)#Removes symbols from the topic name
        lower_topic = clean_topic.lower()#Normalizes the text of the topic
        hashtag = nltk.word_tokenize(lower_topic)#Tokenizes the text of the topic
        
        collection = db["collection"+`i`]#Gets a colection
        cursor = collection.find({})#Gets the tweets in that collection
        
        for document in cursor:#For each tweet

            raw_tweet = re.sub(r"https\S+", "", document["text"])#Removes urls
            raw_tweet = re.sub(r"http\S+", "", document["text"])#Removes urls
            tokenized_tweet = nltk.word_tokenize(raw_tweet)#Tokenizes the tweet
            
            for word in tokenized_tweet:
                clean_tweet = re.sub('\W|\d|_', '', word)#Excludes numbers and symbols
                if clean_tweet:  #If it isn't empty
                    
                    lower_tweet = clean_tweet.lower()#Normalization of the tweet
                    
                    if lower_tweet not in word_counter : #If 'word' is seen for the first time it adds it to the list and sets value to 1
                          word_counter[lower_tweet] = 1
                    else:
                          word_counter[lower_tweet] += 1 #If 'word' is already in the list it increases the value by 1
                   
                    if (lower_tweet not in stopwords) and (lower_tweet not in hashtag):#Removes stopwords and the topic
                        final_tweet.append(lower_tweet)#Adds word to a list
                        
                        
            for word in final_tweet:
                 if word not in word_counter2 :#If 'word' is seen for the first time it adds it to the list and sets value to 1
                          word_counter2[word] = 1
                 else:
                          word_counter2[word] += 1#If 'word' is already in the list it increases the value by 1
           
            str1 = ' '.join(final_tweet)#Rejoins the clean text into a single string seperated by spaces
            final_tweet[:]=[]#Empties list
            collection .update_one({'_id' :document["_id"]}, {'$set': {'clean_text': str1}})#Adds the clean text to the database
            
            #####PART 4#####
            if not (("label") in document):#If tweets isn't already analyzed
                sentimentAnalysis(str1,document)#Calls function to analyse the sentiment of the tweet
        printPlots(word_counter,topic," before stopwords removal")#Plots most frequent terms before stopwords removal
        plotZipf(word_counter,topic)#Plots zipf
        printPlots(word_counter2,topic," after stopwords removal")#Plots most frequent terms after stopwords removal
        polarityPie(i)#Plots polarity pie
        
        #####PART 5#####
        userAnalysis(db,i,topic)#Analyzes the users
        word_counter.clear()
        word_counter2.clear()
        i+=1
        
        