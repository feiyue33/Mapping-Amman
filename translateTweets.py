import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from google.cloud import translate
import re
import time
import os
import random

# Google Cloud
# To get the credential:
# 1. Create or select a project.
# 2. Enable the Cloud Translation API for that project.
# 3. Create a service account.
# 4. Download a private key as JSON.
credential_path = "../auth.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


def remove_emoji(string):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)


class translateTweets(dml.Algorithm):
    contributor = 'gaotian'
    reads = ['emmaliu_gaotian_xli33_yuyangl.tweets']
    writes = ['emmaliu_gaotian_xli33_yuyangl.tweets_translated']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')

        # To run the code in trial mode, we only get the twitter data that already translated by us. Because
        # if you want to run the code without the trial mode, you have to apply a Google account and pay some money
        # to use Google translate API and spend about 2 hours to run the entire code.
        if trial:
            url = 'http://datamechanics.io/data/tweets_translated.json'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropCollection("tweets_translated")
            repo.createCollection("tweets_translated")
            repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].insert_many(r)
            repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].metadata({'complete': True})
            print(repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].metadata())
            repo.logout()

        # run the code in non-trial mode. Use Google API to translate the tweets
        else:
            # Get Tweets data without objectId
            tweetsData = repo.emmaliu_gaotian_xli33_yuyangl.tweets.find({}, {'_id': False}, no_cursor_timeout=True).batch_size(30)
            dataTranslated = []

            # translate text and user's location to English
            translate_client = translate.Client()
            i = 0
            for item in tweetsData:
                # print(item['user']['location'])
                # print(translator.translate(remove_emoji(item['user']['location'])).text)
                try:
                    if item['user']['location']:
                        item['user']['location'] = translate_client.translate(remove_emoji(item['user']['location']),
                                                                              target_language='en')['translatedText']
                    if item['text']:
                        item['text'] = translate_client.translate(remove_emoji(item['text']), target_language='en')['translatedText']
                except:
                    break
                i += 1
                print(i)
                dataTranslated.append(item)
                sleep = random.choice([.4, .45, .5, .55, .6, .65, .7])
                time.sleep(sleep)
                # if i >= 10:
                #     break

            tweetsData.close()

            with open("tweets_translated.json", 'w') as outfile:
                 json.dump(dataTranslated, outfile, indent=4)

            # store results into database
            repo.dropCollection("tweets_translated")
            repo.createCollection("tweets_translated")

            for i in dataTranslated:
                # print(i)
                repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].insert(i)
            repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].metadata({'complete': True})
            print(repo['emmaliu_gaotian_xli33_yuyangl.tweets_translated'].metadata())

            repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/emmaliu_gaotian_xli33_yuyangl')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/emmaliu_gaotian_xli33_yuyangl')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', '')
        this_script = doc.agent('alg:emmaliu_gaotian_xli33_yuyangl#translateTweets',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#tweets',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        translate_tweets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(translate_tweets, this_script)
        doc.usage(translate_tweets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )
        tweetsTranslated = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#get_tweets',
                                  {prov.model.PROV_LABEL: 'tweets translated by Google Translator', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(tweetsTranslated, this_script)
        doc.wasGeneratedBy(tweetsTranslated, translate_tweets, endTime)
        doc.wasDerivedFrom(tweetsTranslated, resource, translate_tweets, translate_tweets, translate_tweets)

        repo.logout()

        return doc


# translateTweets.execute(True)
# doc = translateTweets.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof