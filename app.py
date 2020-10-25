import json
import codecs
import pandas as pd
from bottle import route, get, post, template, static_file, request, run
from elasticsearch import Elasticsearch

DOMAIN = "localhost"
ELASTIC_PORT = 9200
BOTTLE_PORT = 8000

INDEX = 'movies'
DOC_TYPE = 'movie'
RESULTS_PER_PAGE = 10

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
            from_ = RESULTS_PER_PAGE * page, # for pagination
            index = INDEX,
            body = {
                'size' : RESULTS_PER_PAGE,
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
def html_elasticsearch(view_page=0):

    html_file = codecs.open("index.html", 'r')
    html = html_file.read()

    # get all of the Elasticsearch indices, field names, & documents
    elastic_data = get_elasticsearch_data(client, page=view_page)
    dataFrame = pd.DataFrame(elastic_data)

    # if there's no client then show on frontend
    if client != None:
        pass

        html += '<div class="my_container">'
        html += '<br>'
        html += '<div class="page-header text-center">'
        html +=     '<h1>Search for a movie</h1>'
        html += '</div>'
        html += '<span style="font-size: 1.2em;"><strong>Index name:</strong>' + str(dataFrame['_index'].iloc[0]) + '</span>'
        html += '<p style="font-size: 1.2em;"><strong>Doc Type:</strong>' + str(dataFrame['_type'].iloc[0]) + '</p>'
        html += '<table id="result_table" class="table table-striped table-hover">'
        html +=     '<thead>'
        html +=         '<tr>'
        html +=             '<th scope="col">#</th>'
        html +=             '<th scope="col">Id</th>'
        html +=             '<th scope="col">Score</th>'
        html +=             '<th scope="col">Movie Id</th>'
        html +=             '<th scope="col">Title</th>'
        html +=             '<th scope="col">Genres</th>'
        html +=         '</tr>'
        html +=     '</thead>'
        html +=     '<tbody>'

        for row in dataFrame.itertuples():
            index = row.Index
            id = row._3
            score = row._4
            movie = row._5

            # new table row
            html += '<tr>'

            # enumerate() over the index fields
            html += '<th scope="row">' + str(index) + '</th>'
            html += '<td>' + str(id) + '</td>'
            html += '<td>' + str(score) + '</td>'
            html += '<td>' + str(movie.get('movieId')) + '</td>'
            html += '<td>' + str(movie.get('title')) + '</td>'
            html += '<td>' + str(movie.get('genres')) + '</td>'

            # close the table row for the Elasticsearch index fields
            html += '</tr>'

        # close the table tag for the Elasticsearch index
        html += '</body><br>'
        html += '</table><br>'

        html += '<nav aria-label="...">'
        html +=     '<ul class="pagination pagination-lg justify-content-center">'
        html +=       '<li class="page-item disabled">'
        html +=         '<a class="page-link" style="cursor: pointer;" onclick="javasrcript:previousPage()">Previous</a>'
        html +=       '</li>'
        html +=       '<li class="page-item">'
        html +=         '<a class="page-link" style="cursor: pointer;" onclick="javasrcript:nextPage()">Next</a>'
        html +=       '</li>'
        html +=     '</ul>'
        html += '</nav>'
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

@route('/page/<num>')
def select_page(num=0):
    # call the func to return HTML to framework
    page = int(num)
    return html_elasticsearch(page)
    
# pass a port for the framework's server
run(
    host = DOMAIN,
    port = BOTTLE_PORT,
    debug = True
)