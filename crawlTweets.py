#!/usr/bin/python
from twitter import *
import dml
import csv
import time
import json
import pymongo

# The coordinate of center of Amman City # We use this to get radius of
latitude = 31.947
longitude = 35.925
max_range = 100
num_results = 29000
# outfile = "output.csv"
data = []
# create twitter API object
twitter = Twitter(auth=OAuth(dml.auth['services']['Access']['Access_token'],
                             dml.auth['services']['Access']['Access_token_secret'],
                             dml.auth['services']['Consumer']['API_key'],
                             dml.auth['services']['Consumer']['API_secret_key']))

with open('tweets_amman.json', 'w') as outfile:
    result_count = 0
    last_id = None
    while result_count < num_results:
        # perform a search based on latitude and longitude
        query = twitter.search.tweets(q="", geocode="%f,%f,%dkm" % (latitude, longitude, max_range),
                                        num_results=100, max_id=last_id, count=100)
        for result in query["statuses"]:
            # print(result)
            data.append(result)
            result_count += 1
            print(result_count)
            last_id = result["id"]
        time.sleep(5.5)
    json.dump(data, outfile, indent=4)
print("Got %d results" % result_count)
