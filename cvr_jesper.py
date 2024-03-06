from elasticsearch import Elasticsearch, RequestsHttpConnection

# IMPORTANT: elasticsearch==7.13.4 og IKKE nyere
# conda install -c conda-forge elasticsearch==7.13.4
# pip install elasticsearch==7.13.4


### Initialize elasticsearch
def create_es_connection():
    es = Elasticsearch(
        ["http://distribution.virk.dk/cvr-permanent"],
        port=80,
        http_auth=(
            "Implement_Consulting_Group_CVR_I_SKYEN",
            "5db2f428-b1a8-4243-96d0-83a5c11e3f6c",
        ),
        connection_class=RequestsHttpConnection,
        use_ssl=False,
        verify_cert=False,
        timeout=100,
    )
    return es


### Query functions
def search_cvr(cvr):
    query = {"match": {"Vrvirksomhed.cvrNummer": {"query": cvr}}}
    return query


def search_vname(navn):
    query = {"match": {"Vrvirksomhed.navne.navn": {"query": navn}}}
    return query


def search_dname(navn):
    query = {
        "match": {"Vrvirksomhed.deltagerRelation.deltager.navne.navn": {"query": navn}}
    }
    return query


### Body function
def create_body(cvr=False, vname=False, dname=False):
    ## The clause (query) must appear in matching documents. These clauses must match, like logical AND.
    must = []
    must_not = []

    ## At least one of these clauses must match, like logical OR.
    should = []

    ## Creating queries
    if cvr:
        cvr = search_cvr(cvr)
        should.append(cvr)

    if dname:
        dname = search_dname(dname)
        should.append(dname)

    if vname:
        vname = search_dname(vname)
        should.append(vname)

    ## Collecting queries in the body
    body = {"query": {"bool": {"should": should, "must": must, "must_not": must_not}}}

    return body


es = create_es_connection()

response = es.search(
    size=1, explain=False, body=create_body(cvr="32767788")  # how many matches you want
)

print(response["hits"]["hits"][0]["_source"]["Vrvirksomhed"])
