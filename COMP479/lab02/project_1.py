import glob
import sys
import nltk
import re
import collections
import logging
from nltk.corpus import nps_chat, gutenberg

from bs4 import BeautifulSoup

PARSE_KEY = "reuters"

'''
parse a single file to return a list of documents at the level of PARSE_KEY
'''


def parse_file(file_directory):
    f = open(file_directory, "r", encoding="iso8859_2")
    data = f.read()
    f.close()
    soup = BeautifulSoup(data, features="html.parser")
    documents = soup.findAll("reuters")
    return documents


def generate_tokens_pipeline(text):
    tokens = nltk.word_tokenize(text)
    tokens = [token.lower() for token in tokens]
    wnl = nltk.WordNetLemmatizer()
    tokens = [wnl.lemmatize(t) for t in tokens]
    return tokens


def clean_source(documents):
    cleaned_documents = []
    for document in documents:
        doc_unit = []
        if document["newid"] is not None:
            doc_unit.append(document["newid"])
        else:
            doc_unit.append(None)
        if document.body is not None:
            doc_unit.append(generate_tokens_pipeline(document.body.text))
        else:
            doc_unit.append("")
        cleaned_documents.append(doc_unit)
    return cleaned_documents


def spimi(inverted_index, doc_unit):
    for token in doc_unit[1]:
        if inverted_index.get(token, None) is not None:
            inverted_index[token].add(str(doc_unit[0]))
        else:
            inverted_index[token] = set([str(doc_unit[0])])
            inverted_index[token] = set([str(doc_unit[0])])


def persist_memory_data_to_csv(inverted_index, f_name):
    f = open(f_name, "w")
    for key in sorted(inverted_index.keys()):
        f.write(key + "=" + " ".join(sorted(inverted_index.get(key))) + "\n")
    f.close()


def read_line_from_block(block_file_obj, block_number):
    top_line = block_file_obj.readline()
    key_values_pair = top_line.rstrip("\n").split("=")
    return [key_values_pair[0], [block_number, key_values_pair[1].split(" ")]]


def query_parser(query: str):
    return generate_tokens_pipeline(query)


def and_query(index_file: str, words: list):
    f = open(index_file, "r")
    res = []

    if len(words) == 0:
        return res

    a = words[0]
    line = f.readline()
    while line != '':
        print(line.split("="))
        if line.split("=")[0] == a:
            res = line.split("=")[1].split(" ")
            break
        else:
            line = f.readline()

    for i in range(1, len(words)):
        b_posting = []
        b = words[i]
        line = f.readline()
        while line != '':
            if line.split("=")[0] == b:
                b_posting = line.split("=")[1].split(" ")
                break
            else:
                line = f.readline()
        res = intersection(res, b_posting)
    f.close()
    return res


def intersection(a, b):
    res = []
    i = 0
    j = 0
    min_len = min(len(a), len(b))
    while i < min_len and j < min_len:
        if int(a[i]) == int(b[i]):
            res.append(a[i].rstrip("\n"))
            i += 1
            j += 1
        elif int(a[i]) < int(b[i]):
            i += 1
        else:
            j += 1
    return res


def or_query(index_file: str, words: list):
    f = open(index_file, "r")
    res = []
    if len(words) == 0:
        return res
    a = words[0]
    line = f.readline()
    while line:
        if line.split("=")[0] == a:
            res = line.rstrip("\n").split("=")[1].split(" ")
            break
        else:
            line = f.readline()

    for i in range(1, len(words)):
        b_posting = []
        b = words[i]
        line = f.readline()
        while line:
            if line.split("=")[0] == b:
                b_posting = line.rstrip("\n").split("=")[1].split(" ")
                break
            else:
                line = f.readline()
        res = res + b_posting
    f.close()
    c = collections.Counter(res)
    print(c)
    return [index_frequency_pair[0] for index_frequency_pair in c.most_common()]


def merge_blocks(block_files):
    def sorted_as_int(nums):
        nums = [int(num) for num in nums if len(re.findall(r"\d+", num)) > 0]
        nums = sorted(nums)
        nums = [str(num) for num in nums]
        return nums
    output_file_name = "./index/index{}.txt"
    output_file_count = 0
    f = open(output_file_name.format(output_file_count), "w")
    count = 0

    files = [i for i in range(len(block_files))]
    for file_name in block_files:
        index = int(re.findall(r"\d+", file_name)[0])
        files[index] = open(file_name, "r")

    lines = {}
    for i in range(len(files)):
        try:
            line = read_line_from_block(files[i], i)
        except IndexError:
            continue
        if line == "":
            files[index].close()
        else:
            if lines.get(line[0], None) is not None:
                lines[line[0]].append(line[1])
            else:
                lines[line[0]] = [line[1]]
    while len(lines.keys()) > 0:
        token = sorted(lines.keys())[0]
        postings = [value[1] for value in lines.get(token)]
        index_lst = [value[0] for value in lines.get(token)]

        logging.debug("Writing entry to index file:")
        logging.debug("Token: " + str(token))

        p = []
        for posting in postings:
            p.extend(posting)

        p = sorted_as_int(list(set(p)))

        f.write(str(token) + "=" + " ".join(p) + "\n")
        count += 1
        if count == 2500:
            f.close()
            output_file_count += 1
            f = open(output_file_name.format(output_file_count), "w")
            count = 0
        del lines[token]

        for index in index_lst:
            try:
                line = read_line_from_block(files[index], index)
            except IndexError:
                continue
            if line == "":
                files[index].close()
            else:
                if lines.get(line[0], None) is not None:
                    lines[line[0]].append(line[1])
                else:
                    lines[line[0]] = [line[1]]
    f.close()


# inverted_index = {}
# ordered_top = {}
# block_number = 0
# print(block_number)
#
# files = glob.glob("*reut2*.sgm")
# print(files)
# for file in files:
#     docs = parse_file(file)
#     print(file)
#     cleaned_docs = clean_source(docs)
#     counter = 0
#     for doc_unit in cleaned_docs:
#         spimi(inverted_index, doc_unit)
#         counter += 1
#         if counter == 500:
#             counter = 0
#             persist_memory_data_to_csv(inverted_index, "./blocks/block" + str(block_number) + ".txt")
#             inverted_index = {}
#             block_number += 1
#             print(block_number)

files = glob.glob("./blocks/*.txt")
print(files)
print("[INFO] Merging blocks begins")
merge_blocks(files)
print("[INFO] Merging blocks ends")

# query = "5.1 508 absence dasd dassdsdf fdgdfgdf jsahdad adsdkjad"
# res = and_query("./blocks/block37.txt", generate_tokens_pipeline(query))
# print(res)

# query = "gerhard formula promoting"
# res = or_query("./blocks/block37.txt", sorted(generate_tokens_pipeline(query)))
# print(res)

