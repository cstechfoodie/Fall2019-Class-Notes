import os
import glob
import string

import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.stem.porter import *
import collections

ROOT = "/Users/c5277994/Documents/Fall2019-Class-Notes/COMP479/crawl/AIConcordia/www.concordia.ca"
DOCUMENT_PARSE_KEY = "html"

BLOCK_FILE_PATH_REGEX = "./ConcordiaResearchBlocks/*.txt"
INDEX_FILE_PATH_TEMPLATE = "./ConcordiaResearchIndex/index{}.txt"
BLOCK_FILE_PATH_TEMPLATE = "./ConcordiaResearchBlocks/block{}.txt"

INDEX_FILE_SIZE = 250000
MEMORY_CAPACITY = 5000

'''
parse a single file to return a list of documents at the level of PARSE_KEY
'''


def parse_file(file_directory):
    f = open(file_directory, "r", encoding="iso8859_2")
    data = f.read()
    f.close()
    soup = BeautifulSoup(data, features="html.parser")
    [x.extract() for x in soup.findAll('script')]
    try:
        document = soup.findAll(DOCUMENT_PARSE_KEY)[0].body.text
        return document
    except IndexError:
        return None



def generate_tokens_pipeline(text):
    tokens = nltk.word_tokenize(text)
    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
    tokens = list(
        filter(lambda token: len(re.findall(r"^\d+(\.|,|\/|\-|\d+)*$", token)) == 0, tokens))  # ^\d+(\.|,|\/|\-|\d+)*$
    tokens = [token.lower() for token in tokens]

    nltk_words = list(stopwords.words('english'))
    tokens = [token for token in tokens if token not in nltk_words]

    # stemmer = PorterStemmer()
    # tokens = [stemmer.stem(token) for token in tokens]
    #
    # wnl = nltk.WordNetLemmatizer()
    # tokens = [wnl.lemmatize(t) for t in tokens]

    tokens = [t for t in tokens if t != ' ']
    return tokens


def clean_source(url, document, total_document_length):
    single_doc = []
    if url is not None:
        single_doc.append(url)
    else:
        single_doc.append(None)
    if document is not None:
        tokens = generate_tokens_pipeline(document)
        total_document_length += len(tokens)
        single_doc.append(tokens)
    else:
        single_doc.append("")
    return single_doc, total_document_length


def build_inverted_index_in_memory(inverted_index, single_doc):
    counter = collections.Counter(single_doc[1])
    for token in counter.keys():
        if inverted_index.get(token, None) is not None:
            inverted_index[token].add(
                "~".join(
                    [
                        str(single_doc[0]),
                        str(len(single_doc[1])),
                        str(counter[token])
                    ]
                )
            )  # url, doc_len, count
        else:
            inverted_index[token] = set(
                [
                    "~".join(
                        [
                            str(single_doc[0]),
                            str(len(single_doc[1])),
                            str(counter[token])
                        ]
                    )
                ]
            )


def persist_memory_data(inverted_index, f_name):
    f = open(f_name, "w")
    for key in sorted(inverted_index.keys()):
        f.write(key + "=" + " ".join(
            sorted(inverted_index.get(key), key=lambda combo: combo.split("~")[0])) + "\n")
    f.close()


def read_line_from_block(block_file_obj, block_number):
    top_line = block_file_obj.readline()
    key_values_pair = top_line.rstrip("\n").split("=")
    return [key_values_pair[0],
            [block_number, key_values_pair[1].split(" ")]]  # [url, [block_number, [combo1, combo2, combo3....]]]


def merge_blocks(block_files):
    global ending_words

    def sorted_as_int(nums):
        nums = [num for num in nums if len(re.findall(r"\d+", num.rstrip("\n"))) > 0]
        nums = sorted(nums, key=lambda combo: int(combo.split("~")[0]))  # sort by the docId
        return nums

    non_positional_postings_size = 0

    output_file_name = INDEX_FILE_PATH_TEMPLATE
    output_file_count = 0
    f = open(output_file_name.format(output_file_count), "w")
    count = 0

    files = [i for i in range(len(block_files))]  # [0, 1, 2, ..., 42]
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
            files[i].close()
        else:
            if lines.get(line[0], None) is not None:
                lines[line[0]].append(line[1])
            else:
                lines[line[0]] = [line[1]]

    while len(lines.keys()) > 0:  # [ key, [index, [value1, value2]] ]
        token = sorted(lines.keys())[0]
        postings = [value[1] for value in lines.get(token)]
        index_lst = [value[0] for value in lines.get(token)]

        p = []
        for posting in postings:
            p.extend(posting)

        # p = sorted_as_int(list(set(p)))

        non_positional_postings_size += len(p)
        count += 1
        f.write(str(token) + "=" + " ".join(p) + "\n")
        if count == INDEX_FILE_SIZE:
            ending_words.append(token)
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
    return int(output_file_count * INDEX_FILE_SIZE + count), non_positional_postings_size


if __name__ == "__main__":
    inverted_index_dictionary = {}
    ordered_top = {}
    block_number = 0
    total_document_length = 0

    files = [os.path.join(path, name) for path, subdirs, files in os.walk(ROOT) for name in files]

    print("[INFO] SPIMI generating block files begins")
    counter = 0
    for file in files:
        doc = parse_file(file)
        if file.find(DOCUMENT_PARSE_KEY) < 0 or not doc:
            continue
        cleaned_doc, total_document_length = clean_source(file, doc, total_document_length)

        build_inverted_index_in_memory(inverted_index_dictionary, cleaned_doc)
        counter += 1
        if counter == MEMORY_CAPACITY:
            counter = 0
            inverted_index_dictionary = {}
            block_number += 1
        persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))
    persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))

    print("[INFO] SPIMI generating block files ends")
    files = sorted(glob.glob(BLOCK_FILE_PATH_REGEX), key=lambda file_name: int(re.findall(r"\d+", file_name)[0]))
    print("[INFO] Merging blocks begins")
    ending_words = []
    distinct_term_size, non_positional_postings_size = merge_blocks(files)
    print("[INFO] Total number of distinct_term is: ", distinct_term_size)
    print("[INFO] Total number of non_positional_postings is: ", non_positional_postings_size)
    print("[INFO] Ending words for each index file: ", ending_words)
    print("[INFO] Merging blocks ends")

    f = open("spliting_word.txt", "w")
    f.write(' '.join(ending_words) + "\n")
    docs_num = block_number * MEMORY_CAPACITY + counter
    l_avg = round(total_document_length / docs_num)
    f.write("docs_num=" + str(docs_num) + "\n")
    f.write("l_avg=" + str(l_avg) + "\n")
    f.close()
