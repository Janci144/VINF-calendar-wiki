import xml.etree.ElementTree as ET
import re
from regex import regex
import nltk


class Reader:
    g_time = 0

    def __init__(self, chunk_size):
        self.chunk_size = chunk_size

    def read_in_chunks(self, file_object):
        while True:
            data = file_object.read(self.chunk_size)
            if not data:
                break
            yield data

    def read_till_max(self, file_object, max_bytes):
        read_bytes = 0
        while True:
            data = file_object.read(self.chunk_size)
            read_bytes += self.chunk_size
            if read_bytes > max_bytes:
                break
            if not data:
                break
            yield data


class DateInText:
    def __init__(self, date, start, end):
        self.date = date
        self.start = start
        self.end = end


class Index:
    def __init__(self, token, date, info):
        self.token = token
        self.date = date
        self.info = info


class Page:
    def __init__(self, title, infobox, text):
        self.title = title
        self.infobox = infobox
        self.text = text
        self.time = time.clock()

    @staticmethod
    def find_date(text):
        date = regex.search(r'(( [a-zA-Z]{3,8}|\d{1,2})[ ]\d{1,2}([ ]|(\,? ))\d{1,4})|(([a-zA-Z]{3,8}|in) \d{4})(?<=[^\}])', text)

        if date:
            start = date.start()
            end = date.end()
            date = date.group(0)

            return DateInText(date, start, end)

        return None

    def find_token(self, text, date_start, date_end):
        token_reg = regex.finditer(r'[A-Z][a-zA-Z]{3,}(?: [A-Z][a-zA-Z]*){0,}', text)

        closest_token = None
        closest_distance = 100
        for token_it in token_reg:
            token = token_it.group(0)

            if token_it.start() == date_start:
                continue

            if token_it.end() < date_start and date_start - token_it.end() < closest_distance:
                closest_distance = date_start - token_it.end()
                closest_token = token
            elif token_it.start() - date_end < closest_distance:
                closest_distance = token_it.start() - date_end
                closest_token = token

        return closest_token

    def paragraph_splitter(self, sep):
        text_split = self.text.split(sep=sep)
        if len(text_split) >= 2:
            self.text = text_split[0]
            return True
        else:
            return None

    def parse_text(self):
        results = []
        self.text = regex.sub(r'<ref.*\n?.*</ref>', repl="", string=self.text)
        self.text = regex.sub(r'{\| class=\"wikitable.*\|}', repl="", string=self.text, flags=regex.DOTALL)
        self.text = regex.sub(r'{{[cC]ite.*}}', repl="", string=self.text, flags=regex.DOTALL)

        if self.paragraph_splitter(sep='== See also =='):
            pass
        elif self.paragraph_splitter(sep='==Notes=='):
            pass
        elif self.paragraph_splitter(sep='==References=='):
            pass
        elif self.paragraph_splitter(sep='== Bibliography =='):
            pass
        elif self.paragraph_splitter(sep='== External links =='):
            pass
        elif self.paragraph_splitter(sep='=== Sources ==='):
            pass

        sentences_reg = regex.finditer(r'(^| )[A-Z][^\.!?]{5,}[\.!?]', self.text) # possibly [A-Z][^\.!?]{5,}[\.!?] for performance

        for sentence_it in sentences_reg:
            sentence = sentence_it.group(0)
            date_in_text = self.find_date(sentence)
            if date_in_text:
                look_before = 60
                look_after = 30
                start = date_in_text.start - look_before if date_in_text.start >= look_before else 0
                end = date_in_text.end + look_after if date_in_text.end + look_after < len(sentence) else len(sentence)
                if date_in_text.end + look_after > len(sentence):
                    token = self.find_token(sentence[start:], date_in_text.start, date_in_text.end)
                else:
                    token = self.find_token(sentence[start:date_in_text.end + look_after], date_in_text.start, date_in_text.end)

                token_context = sentence[start:end]

                # token with full word at beginning
                i = start
                counter = 0
                while True:
                    i -= 1
                    counter += 1
                    if i < 0 or counter > 8:
                        break

                    if not(sentence[i].isalpha() or sentence[i].isdigit()):
                        token_context = sentence[i+1:start] + token_context
                        break

                # token with full word at end
                i = end
                counter = 0
                while True:
                    i += 1
                    counter += 1
                    if i > len(sentence)-1 or counter > 8:
                        break
                    if not(sentence[i].isalpha() or sentence[i].isdigit()):
                        token_context += sentence[end:end+counter]
                        break

                token_context = token_context.replace('\n', ' ')
                token_context = regex.sub(r'[^a-zA-Z1-9.!?:%$ ]', '', token_context)
                token_context = token_context.strip()

                results.append(Index(token=token if token else self.title, date=date_in_text.date,
                                     info=token_context))

                #  I couldnt find best word that explain the purpose, often the result was meaningful, therefore I
                #  decided not to use it.

                # tokenized = nltk.pos_tag(nltk.word_tokenize(sentence))
                #
                # proper_nouns = []
                # nouns = []
                # for (word, pos) in tokenized:
                #     if pos == 'NNP':
                #         proper_nouns.append(word)
                #     elif pos == 'NN':
                #         nouns.append(word)
                #
                # results.append(Index(token=proper_nouns[0] if proper_nouns else "title", date=date_in_text.date, info=proper_nouns[1] if
                # len(proper_nouns) > 1 else nouns[0] if nouns else ""))

        return results

    def _parse_infobox(self, text: str, title):
        result = []
        text = regex.sub(r'\n ?\|', '\n|', text)
        lines = text.split('\n|')
        for line in lines:
            date_in_text = self.find_date(line)
            if date_in_text:
                info = [x.strip() for x in line.split('=')]
                result.append(Index(token=title, date=date_in_text.date, info=info[0].replace('\n', '')))

        return result

    def get_parsed_date_tokens(self):
        results = []
        if self.infobox:
            infobox_results = self._parse_infobox(self.infobox, self.title)
            results.extend(infobox_results)

        if self.text:
            text_results = self.parse_text()
            results.extend(text_results)

        return results


class Parser:
    def __init__(self):
        pass

    def parse_page(self, page):
        tree = ET.fromstring(page)
        title = tree.find('title').text
        text = tree.find('revision').find('text').text
        infobox = None

        infobox_regex = regex.search(r'(?=\{Infobox)(\{([^{}]|(?1))*\})', page)
        text_start_index = 0
        if infobox_regex:
            text_start_index = infobox_regex.end()
            infobox = infobox_regex.groups(0)[0]

        page = Page(title, infobox, text[text_start_index:])
        return page.get_parsed_date_tokens()


def create_testing_file(name, size):
    with open('E:\VINF_data\enwiki-20200401-pages-articles.xml', encoding='UTF-8') as f:
        one_Mb = 1024*1024
        reader = Reader(chunk_size=one_Mb)
        with open(name, 'w', encoding='UTF-8') as write_file:
            for data in reader.read_till_max(f, max_bytes=size):
                write_file.write(data)


if __name__ == '__main__':
    # with open('E:\VINF_data\enwiki-20200401-pages-articles.xml', encoding='UTF-8') as f:
    import cProfile, pstats, io
    import time
    import os
    import random

    # create_testing_file('large_testing.xml', 1024*1000*2000)
    # exit(0)

    def start_parse():
        start_time = time.clock()
        path = "testing.xml"
        reader = Reader(chunk_size=1024 * 1024)
        parser = Parser()
        final_results = []

        with open(path, encoding='UTF-8') as read_file:
            page_tag_length = len('<page>')
            page_end_tag_length = len('</page>')
            data_for_next_chunk = ""
            with open('output.txt', 'w', encoding='UTF-8') as write_file:
                for data in reader.read_in_chunks(read_file):
                    data = f'{data_for_next_chunk}{data}'
                    start_pages_positions = [m.start() for m in re.finditer('<page>', data)]
                    end_pages_positions = [m.start() for m in re.finditer('</page>', data)]

                    if not start_pages_positions and not end_pages_positions:
                        continue

                    for i, end in enumerate(end_pages_positions):
                        start = start_pages_positions[i]
                        results = parser.parse_page(data[start:end+page_end_tag_length])
                        for result in results:
                            write_file.write('token: ' + result.token + "," + result.date + "," + result.info + '\n')

                    if not end_pages_positions:
                        data_for_next_chunk = data
                    elif len(start_pages_positions) != len(end_pages_positions):
                        if not end_pages_positions:
                            data_for_next_chunk = data[start_pages_positions[0]:]
                        else:
                            data_for_next_chunk = data[end_pages_positions[-1] + page_end_tag_length:]

            print("len", len(final_results))
            ttime_sec = time.clock() - start_time
            print("total time: ", ttime_sec)
            f_size = os.path.getsize(path)
            print(f"Speed: {(f_size/1000000)/ttime_sec}MB/sec")

    start_parse()
    # For debugging purposes
    # cProfile.run('start_parse()', 'restats')
    # print('Time: ', Reader.g_time)

    # import pstats
    # p = pstats.Stats('restats')
    # p.strip_dirs().sort_stats(-1)
    # p.sort_stats('time').print_stats()



