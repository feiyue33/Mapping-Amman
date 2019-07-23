import dml
import prov.model
import datetime
import uuid
import folium
from folium.plugins import HeatMap
import os
import re
import time
from google.cloud import translate
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
import tempfile
import subprocess

# # Google Cloud
# # To get the credential:
# # 1. Create or select a project.
# # 2. Enable the Cloud Translation API for that project.
# # 3. Create a service account.
# # 4. Download a private key as JSON.
# credential_path = "../auth.json"
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
# translate_client = translate.Client()
analyzer = SentimentIntensityAnalyzer()

PORT = 5002
HOST = '127.0.0.1'
SERVER_ADDRESS = '{host}:{port}'.format(host=HOST, port=PORT)
FULL_SERVER_ADDRESS = 'http://' + SERVER_ADDRESS


def TemproraryHttpServer(page_content_type, raw_data):
    """
    A simpe, temprorary http web server on the pure Python 3.
    It has features for processing pages with a XML or HTML content.
    """

    class HTTPServerRequestHandler(BaseHTTPRequestHandler):
        """
        An handler of request for the server, hosting XML-pages.
        """

        def do_GET(self):
            """Handle GET requests"""

            # response from page
            self.send_response(200)

            # set up headers for pages
            content_type = 'text/{0}'.format(page_content_type)
            self.send_header('Content-type', content_type)
            self.end_headers()

            # writing data on a page
            self.wfile.write(bytes(raw_data, encoding='utf'))

            return

    if page_content_type not in ['html', 'xml']:
        raise ValueError('This server can serve only HTML or XML pages.')

    page_content_type = page_content_type

    # kill a process, hosted on a localhost:PORT
    subprocess.call(['fuser', '-k', '{0}/tcp'.format(PORT)])

    # Started creating a temprorary http server.
    httpd = HTTPServer((HOST, PORT), HTTPServerRequestHandler)

    # run a temprorary http server
    httpd.serve_forever()


def run_html_server(html_data=None):

    if html_data is None:
        html_data = """
        <!DOCTYPE html>
        <html>
        <head>
        <title>Page Title</title>
        </head>
        <body>
        <h1>This is a Heading</h1>
        <p>This is a paragraph.</p>
        </body>
        </html>
        """

    # open in a browser URL and see a result
    webbrowser.open(FULL_SERVER_ADDRESS)

    # run server
    TemproraryHttpServer('html', html_data)


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


class visualization(dml.Algorithm):

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

        # In order to run the code quickly, we should use trial mode
        if trial:
            with open('emmaliu_gaotian_xli33_yuyangl/sentiment_map.html') as f:
                folium_map_html = f.read()
            run_html_server(folium_map_html)
        else:
            # Google Cloud
            # To get the credential:
            # 1. Create or select a project.
            # 2. Enable the Cloud Translation API for that project.
            # 3. Create a service account.
            # 4. Download a private key as JSON.
            credential_path = "../auth.json"
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
            translate_client = translate.Client()

            # Get Tweets data
            tweetsData = repo.emmaliu_gaotian_xli33_yuyangl.tweets.find()

            # create heatmap
            heat_map = folium.Map(location=[31.947, 35.925])
            # create sentiment map
            sentiment_map = folium.Map(location=[31.947, 35.925])

            positiveFG = folium.FeatureGroup(name='Positive')
            negativeFG= folium.FeatureGroup(name='Negative')
            neutralFG = folium.FeatureGroup(name='Neutral')
            heatmapFG = folium.FeatureGroup(name='Heat Map')
            coordinates = []
            count = 0
            for item in tweetsData:
                if item['geo']:
                    coordinates.append((item['geo']['coordinates'][0], item['geo']['coordinates'][1]))
                    translated_text = translate_client.translate(remove_emoji(item['text']), target_language='en')['translatedText']
                    vs = analyzer.polarity_scores(translated_text)
                    # print("{:-<65} {}".format(sentence, str(vs)))
                    if vs['compound'] < 0:
                        color = 'blue'
                        icon = 'thumbs-down'
                        featureGroup = negativeFG
                    elif vs['compound'] > 0:
                        color = 'red'
                        icon = 'thumbs-up'
                        featureGroup = positiveFG
                    else:
                        color = 'orange'
                        icon = ''
                        featureGroup = neutralFG
                    folium.Marker(
                        location=[item['geo']['coordinates'][0], item['geo']['coordinates'][1]],
                        popup=item['text'],
                        icon=folium.Icon(color=color, icon=icon)
                    ).add_to(featureGroup)
                    count += 1
                    print(count)
                    time.sleep(.6)

            HeatMap(coordinates).add_to(heatmapFG)
            # heat_map.save('heat_map.html')

            positiveFG.add_to(sentiment_map)
            negativeFG.add_to(sentiment_map)
            neutralFG.add_to(sentiment_map)
            heatmapFG.add_to(sentiment_map)
            folium.LayerControl().add_to(sentiment_map)
            sentiment_map.save('sentiment_map.html')

            tmp = tempfile.NamedTemporaryFile()
            sentiment_map.save(tmp.name)
            with open('sentiment_map.html') as f:
                folium_map_html = f.read()

            run_html_server(folium_map_html)

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
        visualization = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(visualization, this_script)
        doc.usage(visualization, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )

        repo.logout()

        return doc


# visualization.execute(trial=True)
# doc = visualization.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
