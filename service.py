from elasticsearch import Elasticsearch
import json
from query import basic_search, agg_q

# from rules import process
with open("config.json", "r") as f0:
    config = json.load(f0)

client = Elasticsearch(HOST=config["elasticsearch"]["host"], PORT=config["elasticsearch"]["port"],
                       http_auth=(config["elasticsearch"]["username"], config["elasticsearch"]["password"]),
                       scheme="https",
                       ca_certs="elasticsearch-8.6.0-windows-x86_64/elasticsearch-8.6.0/config/certs/http_ca.crt")

INDEX = config["elasticsearch"]["index"]


def search(query):
    # result = client. (index=INDEX,body=standard_analyzer(query))
    # keywords = result ['tokens']['token']
    # print(keywords)

    # query_body= process(query)
    query_body = basic_search(query)
    print('Making Basic Search ')
    res = client.search(index=INDEX, body=query_body, size=5)
    return res


def process_year_out(in_seq):
    in_seq = in_seq["aggregations"]["years"]["buckets"]
    return in_seq


def get_db_stats():
    years_q = agg_q("Year")
    year_stats = process_year_out(client.search(index=INDEX, body=years_q))
    res = {"Year": year_stats}
    return res
