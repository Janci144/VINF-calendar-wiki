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
                    # try:
                    #     date = datetime.datetime(year=int(year), month=month_value, day=1)
                    # except Exception as ex:
                    #     print(e)
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
                # res = re.search(r'The|in|from|the|since|early|late|at|till|arround|after', month)
                # if res:
                month_value = 0

            # if month_value == -1:
            #     skipped = True
            #     wrong += 1
            #     print("damage:", month)
            # else:
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

        # a = str(date)
        # b = datetime.datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S")
        splits = line.split(';', 2)
        #title = splits[0].replace('\t','').lower().replace(' ', '_')
        title = splits[0].replace('\t', '')

        if len(title) > 35:
            too_long += 1
            print('title is too long', too_long)
            line = f.readline()
            continue

        date_str = str(date)
        text = re.sub(r'{{.*}}', '', splits[2])
        text = re.sub(r'[^a-zA-Z0-9.!?:%$;, ]', '', text)
        #text = re.sub(r'[\[\]]', '', text).replace('\n', '')

        bulk_array.append({"_index": "wiki", "title": title, "date": date_str, "text": text})
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


 #
        # if index == last_index or last_index == "":
        #     bulk_array.append({"_index": "test1234", "title": index, "date": date_str, "text": text})
        #     last_index = index
        # else:
        #     print(bulk_array)
        #     actions = create_iter(bulk_array)
        #     try:
        #         helpers.bulk(es, actions)
        #         print("bulk done")
        #     except Exception as e:
        #         print("error timeout")
        #     bulk_array = []
        #     last_index = index
