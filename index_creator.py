import re
import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import os
# Connect to the elastic cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def create_iter(array):
    for doc in array:
        yield {
            "_index": doc['_index'],
            "_type": "document2",
            "title": doc['title'],
            "text": doc["text"],
            "date": doc["date"]
        }

index_body = {
    "settings": {
        "analysis": {
            "filter": {
                "prefixes": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 25
                }
            },
            "analyzer": {
                "my_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "prefixes"]
                }
            }
        }
    },
    "mappings": {
        "document2": {
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "my_analyzer",
                    "search_analyzer": "standard"
                },
                "text": {
                    "type": "text",
                    "index": "false"
                },
                "date": {
                    "type": "text",
                    "index": "false"
                }
            }
        }
    }
}

es.indices.create("wiki22", body=index_body)

start_time = time.clock()
with open('output.txt', 'r', encoding='utf-8') as f:
    wrong = 0
    lines_count = 0
    too_long = 0
    bulk_array = []
    last_index = ""
    line = f.readline()
    while line:
        skipped = False
        res = re.search(r'([a-zA-Z]{3,8}|\d{1,2})[ ]\d{1,2}([ ]|(\,? ))\d{1,4}', line)
        if res:
            date_regex = res.group(0)
            data_str = date_regex.split(' ')
            month = data_str[0].replace(',', '').strip()
            day = data_str[1].replace(',', '').strip()
            year = data_str[2].replace(',', '').strip()

            month_value = 0
            if month == 'January':
                month_value = 1
            elif month == 'February':
                month_value = 2
            elif month == 'March':
                month_value = 3
            elif month == 'April':
                month_value = 4
            elif month == 'May':
                month_value = 5
            elif month == 'June':
                month_value = 6
            elif month == 'July':
                month_value = 7
            elif month == 'August':
                month_value = 8
            elif month == 'September':
                month_value = 9
            elif month == 'October':
                month_value = 10
            elif month == 'November':
                month_value = 11
            elif month == 'December':
                month_value = 12
            else:
                month_value = 0

            if int(year) > 2020:
                skipped = True

            if month_value == 0:
                try:
                    date = datetime.datetime(year=int(year), month=1, day=1)
                except ValueError as e:
                    skipped = True
            else:
                try:
                    date = datetime.datetime(year=int(year), month=month_value, day=int(day))
                except Exception as e:
                    try:
                        date = datetime.datetime(year=int(year), month=month_value, day=1)
                    except Exception as ex:
                        skipped = True
        else:
            res = re.search(r'([a-zA-Z]{3,8}|in) \d{4}', line)
            date_regex = res.group(0)
            data_str = date_regex.split(' ')
            month = data_str[0].strip()
            year = data_str[1].strip()

            month_value = 0
            if month == 'January':
                month_value = 1
            elif month == 'February':
                month_value = 2
            elif month == 'March':
                month_value = 3
            elif month == 'April':
                month_value = 4
            elif month == 'May':
                month_value = 5
            elif month == 'June':
                month_value = 6
            elif month == 'July':
                month_value = 7
            elif month == 'August':
                month_value = 8
            elif month == 'September':
                month_value = 9
            elif month == 'October':
                month_value = 10
            elif month == 'November':
                month_value = 11
            elif month == 'December':
                month_value = 12
            else:
                month_value = 0

            if month_value == 0:
                try:
                    date = datetime.datetime(year=int(year), month=1, day=1)
                except Exception as ex:
                    skipped = True
            else:
                try:
                    date = datetime.datetime(year=int(year), month=month_value, day=1)
                except Exception as ex:
                    skipped = True

        lines_count += 1
        if skipped:
            wrong += 1
            print("skipping {}. wrong item", wrong)

        splits = line.split(';', 2)
        title = splits[0].replace('\t', '')

        if len(title) > 35:
            too_long += 1
            print('title is too long', too_long)
            line = f.readline()
            continue

        date_str = str(date)
        text = re.sub(r'{{.*}}', '', splits[2])
        text = re.sub(r'[^a-zA-Z0-9.!?:%$;, ]', '', text)

        bulk_array.append({"_index": "wiki22", "title": title, "date": date_str, "text": text})
        if len(bulk_array) > 500:
            print(f"{lines_count} lines processed")
            actions = create_iter(bulk_array)
            try:
                helpers.bulk(es, actions)
                print("bulk done")
            except Exception as e:
                time.sleep(0.1)
                print("error timeout")
            bulk_array = []

        line = f.readline()

    ttime_sec = time.clock() - start_time
    print("total time: ", ttime_sec)
    f_size = os.path.getsize('output.txt')
    print(f"Speed: {(f_size / 1000000) / ttime_sec}MB/sec")
    print("wrong", wrong, ",too long", too_long)
    print("count", lines_count)
