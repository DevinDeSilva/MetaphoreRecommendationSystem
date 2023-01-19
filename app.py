from flask import Flask, render_template, request
from service import search, get_db_stats
from elasticsearch_dsl import Index

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def search_query_in_db():
    # return "hello world"
    if request.method == 'POST':
        query = request.form['searchTerm']
        res = search(query)
        hits = res['hits']['hits']
        time = res['took']
        # aggs = res['aggregations']
        num_results = res['hits']['total']['value']
        db_stats = get_db_stats()

        return render_template('result.html', query=query, hits=hits, num_results=num_results, time=time,
                               db_stats=db_stats)

    if request.method == 'GET':
        db_stats = get_db_stats()
        return render_template('result.html', init='True', years=db_stats["Year"])


if __name__ == '__main__':
    app.run()
