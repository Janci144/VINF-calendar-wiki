from elasticsearch import Elasticsearch
from datetime import datetime
from fuzzywuzzy import fuzz

es=Elasticsearch([{'host':'localhost','port':9200}])


while True:
    user_input = input()
    if user_input == "exit":
        break

    res= es.search(index='wiki22',doc_type='document2',size=1000, body={
            "query": {
                "multi_match" : {
                  "query":      user_input,
                  "fields":     ["title"],
                }
            }
        })

    titles = set()
    events = []
    for hit in res['hits']['hits']:
        temp = hit['_source']
        titles.add(hit['_source']['title'])
        temp['ratio'] = fuzz.ratio(user_input, hit['_source']['title'])
        events.append(temp)

    sorted_array = sorted(events, key=lambda x: x['ratio'] ,reverse=True)

    sorted_titles = []
    pick = ""
    for item in sorted_array:
        if pick == "" or pick != item['title']:
            sorted_titles.append(item['title'])
            pick = item['title']

    if sorted_array[0]['ratio'] > 80:
        filtered = [x for x in sorted_array if x['title'] == sorted_array[0]['title']]
        sorted_array = sorted(filtered, key=lambda x: datetime.strptime(x['date'], "%Y-%m-%d %H:%M:%S"))
        for i in sorted_array:
            print(i['title'], i['date'], i['text'])
    else:
        print("Phrase return too many results, please narrow search phrase from one of the suggestions:")
        counter = 0
        already_showed = set()
        for title in sorted_titles:
            if counter > 5:
                break
            if title in already_showed:
                continue
            already_showed.add(title)
            counter += 1
            print(title)
