import dml
import prov.model
import datetime
import uuid
import matplotlib.pyplot as plt
import matplotlib as mpl
import random
import numpy as np


def product(R, S):
    return [(t, u) for t in R for u in S]


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]


def dist(p, q):
    (x1, y1) = p
    (x2, y2) = q
    return (x1 - x2) ** 2 + (y1 - y2) ** 2


def plus(args):
    p = [0, 0]
    for (x, y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)


def scale(p, c):
    (x, y) = p
    return (x / c, y / c)


def select_init_center(k, points):
    centroids = []
    # select first center
    firstCenter = random.choice(points)
    # print(firstCenter)
    centroids.append(firstCenter)
    # select other centers
    for i in range(0, k - 1):
        weights = [distance_point_to_closest_center(points[x], centroids) for x in range(len(points))]
        total = sum(weights)
        # normalize to 0-1
        weights = [x / total for x in weights]

        num = random.random()
        total = 0
        x = -1
        while total < num:
            x += 1
            total += weights[x]
        centroids.append(points[x])
    # centers = [points[r] for r in centroids]
    return centroids


def distance_point_to_closest_center(x, center):
    result = dist(x, center[0])
    for centroid in center[1:]:
        distance = dist(x, centroid)
        if distance < result:
            result = distance
    return result


class placeClustering(dml.Algorithm):

    contributor = 'gaotian_xli33'
    reads = ['emmaliu_gaotian_xli33_yuyangl.tweets']
    writes = []

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        # @staticmethod
        def kmeans(k, points):
            # M = [(1, 1), (10, 10)]
            # P = [(1, 2), (2,  1), (1, 3),  (10, 12), (13, 14), (13, 9), (11, 11)]
            P = points
            # use K-means++ algorithm to init k points as centers
            M = select_init_center(k, P)
            # print(M)
            MP = []
            OLD = []
            map = {}
            result = []
            while OLD != M:
                OLD = M
                MPD = [(m, p, dist(m, p)) for (m, p) in product(M, P)]
                PDs = [(p, dist(m, p)) for (m, p, d) in MPD]
                PD = aggregate(PDs, min)
                MP = [(m, p) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p == p2 and d == d2]
                MT = aggregate(MP, plus)
                M1 = [(m, 1) for (m, _) in MP]
                MC = aggregate(M1, sum)
                M = [scale(t, c) for ((m, t), (m2, c)) in product(MT, MC) if m == m2]
                # print(sorted(M))

            cluster = 0
            for m in M:
                map[m] = cluster
                cluster += 1
            # print(map)

            # assign points to cluster
            for mp in MP:
                m, p = mp
                result.append((p, map[m]))
            # print(result)
            return result

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')

        # Get Tweets data
        tweetsData = repo.emmaliu_gaotian_xli33_yuyangl.tweets.find()

        latitude = []
        longitude = []
        coordinates = []
        count = 0
        for item in tweetsData:
            if item['geo']:
                latitude.append(item['geo']['coordinates'][0])
                longitude.append(item['geo']['coordinates'][1])
                coordinates.append((item['geo']['coordinates'][0], item['geo']['coordinates'][1]))
                count += 1
                # print(item['geo'])
                # print(item['user']['name'])
        # print(count)

        # plt.scatter(latitude, longitude, alpha=1)
        # plt.show()

        # run k-means++ algorithm
        points_with_cluster = kmeans(3, coordinates)

        x1 = []
        y1 = []
        x2 = []
        y2 = []
        x3 = []
        y3 = []
        for item in points_with_cluster:
            (x, y), cluster = item
            if cluster == 0:
                x1.append(x)
                y1.append(y)
            elif cluster == 1:
                x2.append(x)
                y2.append(y)
            else:
                x3.append(x)
                y3.append(y)
        area = (15 * np.random.rand(50)) ** 2  # 0 to 15 point radii
        p1 = plt.scatter(x1, y1, color='steelblue', label='cluster 1', s=area, alpha=0.5)
        p2 = plt.scatter(x2, y2, color='darkred', label='cluster 2', s=area, alpha=0.5)
        p3 = plt.scatter(x3, y3, color='olivedrab', label='cluster 3', s=area, alpha=0.5)
        plt.xlabel("latitude")
        plt.ylabel("longitude")
        plt.title('k-means++ algo with k=3')
        plt.legend(loc='upper right')
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
        this_script = doc.agent('alg:emmaliu_gaotian_xli33_yuyangl#placeClustering',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#tweets',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        place_clustering = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(place_clustering, this_script)
        doc.usage(place_clustering, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )

        repo.logout()

        return doc


# placeClustering.kmeans()
# placeClustering.execute()
# doc = placeClustering.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof