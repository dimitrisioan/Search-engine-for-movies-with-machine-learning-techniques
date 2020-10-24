
import json
import codecs
from bottle import run, get
from elasticsearch import Elasticsearch

DOMAIN = "localhost"
ELASTIC_PORT = 9200
BOTTLE_PORT = 8000

INDEX = 'movies'
DOC_TYPE = 'movie'

try:
    # concatenate a string for the Elasticsearch connection
    domain_str = DOMAIN + ":" + str(ELASTIC_PORT)
    # declare a client instance of the Python Elasticsearch library
    client = Elasticsearch( domain_str )
    # info() method raises error if domain or conn is invalid
    print (json.dumps( Elasticsearch.info(client), indent=4 ), "\n")
except Exception as err:
    print ("Elasticsearch() ERROR:", err, "\n")
    # client is set to none if connection is invalid
    client = None

def get_elasticsearch_data(client, query={}, page=0):
    # make API call if client is not None
    if client != None:
        # get 10 of the Elasticsearch documents from index
        docs = client.search(
            from_ = page, # for pagination
            index = INDEX,
            body = {
                'size' : 10,
                'query': {
                    # pass query paramater
                    'match_all' : query
                }
        })

        # get just the doc "hits"
        doc= docs["hits"]["hits"]

        # print the list of docs
        print ("index:", INDEX, "has", len(doc), "num of docs.")

    # return all of the doc dict
    return doc

# gef a function that will return HTML string for frontend
def html_elasticsearch():

    html_file = codecs.open("index.html", 'r')
    html = html_file.read()

    # get all of the Elasticsearch indices, field names, & documents
    elastic_data = get_elasticsearch_data(client)

    # if there's no client then show on frontend
    if client != None:
        pass
        # print ("html_elasticsearch() client:", client)

        # iterate over the index names
        # for index, val in elastic_data.items():
            # pass

#             # create a new HTML table from the index name
#             html += '<br><h3>Index name: ' + str(index) + '</h3>'
#             html += '<table id="' + str(index) + '" class="table table-responsive">'

#             # grab the "fields" list attribute created in get_elasticsearch_data()
#             fields = source_data = elastic_data[index]["fields"]
#             print ("\n\nfields:", fields)

#             # new table row
#             html += '\n<tr>'

#             # enumerate() over the index fields
#             for num, field in enumerate(fields):
#                 html += '<th>' + str(field) + '</th>'

#             # close the table row for the Elasticsearch index fields
#             html += '\n</tr>'

#             # get all of the docs in the Elasticsearch index
#             all_docs = elastic_data[index]["docs"]
#             print ("\nall_docs type:", type(all_docs))

#             # enumerate() over the list of docs
#             for num, doc in enumerate(all_docs):
#                 print ("\ndoc:", doc)

#                 # new row for each doc
#                 html += '<tr>\n'

#                 # iterate over the _source dict for the doc
#                 for f, val in doc["_source"].items():
#                     html += '<td>' + str(val) + '</td>'
               
#                 html += '</tr>'

#             # close the table tag for the Elasticsearch index
#             html += '</table><br>'
    elif client == None:
        html += '<h3 style="color:red">Warning: Elasticsearch cluster is not running on'
        html += ' port: ' + str(ELASTIC_PORT) + '</h3>'
    elif elastic_data == {}:
        html += '<h3>Elasticsearch did not return index information</h3>'

    # return the HTML string
    return html

@get('/')
def elastic_app():
    # call the func to return HTML to framework
    return html_elasticsearch()

# # pass a port for the framework's server
run(
    host = DOMAIN,
    port = BOTTLE_PORT,
    debug = True
)