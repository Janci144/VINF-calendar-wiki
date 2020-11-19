#!/usr/bin/env python

import os
os.system('pip install regex')

from regex import regex
import sys


class Reader:
    g_time = 0

    def read_in_chunks(self, file_object):
        while True:
            data = file_object.readline()
            yield data

            if not data:
                break


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
        text_split = self.text.split(sep)
        if len(text_split) >= 2:
            self.text = text_split[0]
            return True
        else:
            return None

    def parse_text(self):
        results = []
        self.text = regex.sub(r'&lt;ref.*\n?.*&lt;/ref&gt;', repl="", string=self.text)
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

        return results

    def _parse_infobox(self, text, title):
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
        title = regex.search(r'(?<=<title>).*(?=<\/title>)', page).group(0)
        text = regex.search(r'(?<=<text).*(?=<\/text>)', page, flags=regex.DOTALL).group(0)
        infobox = None

        infobox_regex = regex.search(r'(?=\{Infobox)(\{([^{}]|(?1))*\})', text)
        text_start_index = 0
        if infobox_regex:
            text_start_index = infobox_regex.end()
            infobox = infobox_regex.groups(0)[0]

        page = Page(title, infobox, text[text_start_index:])
        return page.get_parsed_date_tokens()


parser = Parser()
data_for_next_chunk = ""
counter = 100
for line in sys.stdin:
    line = line.strip()
    if line.find('</page') != -1:
        data_for_next_chunk += '</page>'
        data = data_for_next_chunk
        try:
            results = parser.parse_page(data)
            for result in results:
                print('%s\t%s' % (result.token, "," + result.date + "," + result.info))
        except Exception:
            data_for_next_chunk = ""
            continue
        data_for_next_chunk = ""
    elif line.find('page>') != -1:
        data_for_next_chunk += '<page>' + '\n'
    else:
        data_for_next_chunk += line + '\n'

if data_for_next_chunk:
    results = parser.parse_page(data_for_next_chunk)
    for result in results:
        print('%s\t%s' % (result.token, "," + result.date + "," + result.info))
