import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt


def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled


def avg(x): # Average
    return sum(x)/len(x)


def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))


def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)


def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))


def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 500):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
        # print(k)
    return len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)


class computeCorrelation(dml.Algorithm):

    contributor = 'gaotian_xli33'
    reads = ['emmaliu_gaotian_xli33_yuyangl.tweets']
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
        tweetsData = repo.emmaliu_gaotian_xli33_yuyangl.tweets.find()

        followers_num = []
        list_num = []
        i = 0
        for item in tweetsData:
            followers_num.append(item['user']['followers_count'])
            list_num.append(item['user']['listed_count'])
            i += 1
            if i >= 200:
                break

        print('correlation coefficient: ' + str(corr(followers_num, list_num)))
        print('p-value: ' + str(p(followers_num, list_num)))

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
        this_script = doc.agent('alg:emmaliu_gaotian_xli33_yuyangl#computeCorrelation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#tweets',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        compute_correlation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(compute_correlation, this_script)
        doc.usage(compute_correlation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )

        repo.logout()

        return doc


# computeCorrelation.execute()
# doc = computeCorrelation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof