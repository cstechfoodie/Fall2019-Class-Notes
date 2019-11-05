import glob
import string

import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.stem.porter import *

DOCUMENT_PARSE_KEY = "reuters"

POSTING_ATTRIBUTE = "newid"

SOURCE_FILE_PATH_REGEX = "./reuters/*reut2*.sgm"
BLOCK_FILE_PATH_REGEX = "./blocks/*.txt"
INDEX_FILE_PATH_TEMPLATE = "./index/index{}.txt"
BLOCK_FILE_PATH_TEMPLATE = "./blocks/block{}.txt"

INDEX_FILE_SIZE = 25000
MEMORY_CAPACITY = 500

'''
parse a single file to return a list of documents at the level of PARSE_KEY
'''


def parse_file(file_directory):
    f = open(file_directory, "r", encoding="iso8859_2")
    data = f.read()
    f.close()
    soup = BeautifulSoup(data, features="html.parser")
    documents = soup.findAll(DOCUMENT_PARSE_KEY)
    return documents


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


def clean_source(documents):
    cleaned_documents = []
    for document in documents:
        single_doc = []
        if document[POSTING_ATTRIBUTE] is not None:
            single_doc.append(document[POSTING_ATTRIBUTE])
        else:
            single_doc.append(None)
        if document.body is not None:
            single_doc.append(generate_tokens_pipeline(document.body.text))
        else:
            single_doc.append("")
        cleaned_documents.append(single_doc)
    return cleaned_documents


def build_inverted_index_in_memory(inverted_index, single_doc):
    for token in single_doc[1]:
        if inverted_index.get(token, None) is not None:
            inverted_index[token].add(str(single_doc[0]))
        else:
            inverted_index[token] = set([str(single_doc[0])])
            inverted_index[token] = set([str(single_doc[0])])


def persist_memory_data(inverted_index, f_name):
    f = open(f_name, "w")
    for key in sorted(inverted_index.keys()):
        f.write(key + "=" + " ".join(sorted(inverted_index.get(key))) + "\n")
    f.close()


def read_line_from_block(block_file_obj, block_number):
    top_line = block_file_obj.readline()
    key_values_pair = top_line.rstrip("\n").split("=")
    return [key_values_pair[0], [block_number, key_values_pair[1].split(" ")]]


def merge_blocks(block_files):
    global ending_words

    def sorted_as_int(nums):
        nums = [int(num) for num in nums if len(re.findall(r"\d+", num.rstrip("\n"))) > 0]
        nums = sorted(nums)
        nums = [str(num) for num in nums]
        return nums

    non_positional_postings_size = 0

    output_file_name = INDEX_FILE_PATH_TEMPLATE
    output_file_count = 0
    f = open(output_file_name.format(output_file_count), "w")
    count = -1

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

        p = sorted_as_int(list(set(p)))

        if count == -1:
            count += 1
        else:
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

    files = glob.glob(SOURCE_FILE_PATH_REGEX)
    files.sort()
    print("[INFO] SPIMI generating block files begins")
    for file in files:
        docs = parse_file(file)
        cleaned_docs = clean_source(docs)
        counter = 0
        for doc_unit in cleaned_docs:
            build_inverted_index_in_memory(inverted_index_dictionary, doc_unit)
            counter += 1
            if counter == MEMORY_CAPACITY:
                counter = 0
                persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))
                inverted_index_dictionary = {}
                block_number += 1
        persist_memory_data(inverted_index_dictionary, BLOCK_FILE_PATH_TEMPLATE.format(str(block_number)))

    print("[INFO] SPIMI generating block files ends")
    files = sorted(glob.glob(BLOCK_FILE_PATH_REGEX))
    print("[INFO] Merging blocks begins")
    ending_words = []
    distinct_term_size, non_positional_postings_size = merge_blocks(files)
    print("[INFO] Total number of distinct_term is: ", distinct_term_size)
    print("[INFO] Total number of non_positional_postings is: ", non_positional_postings_size)
    print("[INFO] Ending words for each index file: ", ending_words)
    print("[INFO] Merging blocks ends")

    f = open("spliting_word.txt", "w")
    f.write(' '.join(ending_words))
    f.close()
