from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import matplotlib.pyplot as plt
import random


class sentimentAnalysis(dml.Algorithm):

    contributor = 'gaotian_xli33'
    reads = ['emmaliu_gaotian_xli33_yuyangl.tweets_translated']
    writes = []

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')

        # Get Tweets data
        tweetsData = repo.emmaliu_gaotian_xli33_yuyangl.tweets_translated.find()
        sentences = []
        compoundScore = []
        sentiment = []
        analyzer = SentimentIntensityAnalyzer()

        # sentiment analysis
        for item in tweetsData:
            sentences.append(item['text'])

        for sentence in sentences:
            vs = analyzer.polarity_scores(sentence)
            # print("{:-<65} {}".format(sentence, str(vs)))
            compoundScore.append(vs['compound'])
            if vs['compound'] >= 0:
                sentiment.append(1)
            else:
                sentiment.append(0)

        # sampling: random sample 200 tweets from 5000 tweets
        sample = []
        sampleNum = random.sample(range(len(compoundScore)), 200)
        for i in sampleNum:
            sample.append(compoundScore[i])
            # print(i)

        x = range(len(sample))
        # compoundScore.sort()

        # draw plot
        plt.scatter(x, sample, c=sample, cmap='coolwarm', alpha=1)
        # plt.plot(x, compoundScore,  alpha=1)
        plt.show()

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
        this_script = doc.agent('alg:emmaliu_gaotian_xli33_yuyangl#sentimentAnalysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#tweets_translated',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        sentiment_analysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(sentiment_analysis, this_script)
        doc.usage(sentiment_analysis, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )

        repo.logout()

        return doc

# sentimentAnalysis.execute()
# doc = sentimentAnalysis.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof