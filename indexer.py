from elasticsearch import Elasticsearch
from datetime import datetime

es=Elasticsearch([{'host':'localhost','port':9200}])

while True:
    user_input = input()
    if user_input == "exit":
        break

    res= es.search(index='wiki',doc_type='document2',size=1000, body={
            "query": {
                "multi_match" : {
                  "query":      user_input,
                  "fields":     ["title"],
                }
            }
        })


    unsorted_events = []
    for hit in res['hits']['hits']:
        unsorted_events.append(hit['_source'])

    sorted_array = sorted(unsorted_events, key=lambda x: datetime.strptime(x['date'], "%Y-%m-%d %H:%M:%S"))
    for i in sorted_array:
        print(i['title'], i['date'], i['text'])


# res= es.search(index='wiki',doc_type='document2',size=1000, body={
#             "query": {
#                 "multi_match" : {
#                   "query":      "Neil Armstrong",
#                   "fields":     ["title"],
#                 }
#             }
#         })
#
#
# unsorted_events = []
# print("len", len(res['hits']['hits']))
# for hit in res['hits']['hits']:
#     unsorted_events.append(hit['_source'])
# # print(hit['_source'])
# # print(res)
#
# sorted_array = sorted(unsorted_events, key=lambda x: datetime.strptime(x['date'], "%Y-%m-%d %H:%M:%S"))
# for i in sorted_array:
#     print(i['title'], i['date'], i['text'])








# e1 = {
#         "first_name":"asd",
#         "last_name":"pafdfd",
#         "age": 27,
#         "about": "Love to play football",
#         }
# res = es.index(index='megacorp',doc_type='employee',body=e1)
# print(res)
# res= es.search(index='megacorp',doc_type='employee',body={
#         'query':{
#             'match':{
#                 "about":"play cricket"
#             }
#         }
#     })
#
# for hit in res['hits']['hits']:
#     print(hit['_source'])

#
#
# def gendata():
#     mywords = ['foo', 'bar', 'baz']
#     for word in mywords:
#         yield {
#             "_index": word,
#             "_type": "document2",
#             "text": "ahah word123 hahah",
#             "date": "daterere"
#             #"doc": {"word": "word1"},
#         }
#
# res = helpers.bulk(es, gendata())
# print(res)
# path = 'output.txt'
# with open(path, encoding='UTF-8') as f:
#     line = f.readline()
#     tab_splitted = line.split('\t', 1)
#     date_splitted = tab_splitted[1].split(',', 2)
#     index = tab_splitted[0]
#     date = date_splitted[1]
#     text = date_splitted[2].replace('\n', '')
#     a=5
#

# e1={
#     "first_name":"nitin",
#     "last_name":"panwar",
#     "age": 27,
#     "about": "Love to play cricket",
#     "interests": ['sports','music'],
# }
#
# res = es.index(index='megacorp',doc_type='employee',id=1,body=e1)
# print(res)
#
# e2={
#     "first_name" :  "Jane",
#     "last_name" :   "Smith",
#     "age" :         32,
#     "about" :       "I like to collect rock albums",
#     "interests":  [ "music" ]
# }
# e3={
#     "first_name" :  "Douglas",
#     "last_name" :   "Fir",
#     "age" :         35,
#     "about":        "I like to build cabinets",
#     "interests":  [ "forestry" ]
# }
# res=es.index(index='megacorp',doc_type='employee',id=2,body=e2)
# print(res)
# res=es.index(index='megacorp',doc_type='employee',id=3,body=e3)
# print(res)
#
# res=es.get(index='megacorp',doc_type='employee',id=3)
# print(res)
#
# res= es.search(index='megacorp',body={'query':{'match_all':{}}})
# print('Got %d hits:' %res['hits']['total'])
#
# res= es.search(index='megacorp',body={'query':{'match':{'first_name':'nitin'}}})
# print(res['hits']['hits'])