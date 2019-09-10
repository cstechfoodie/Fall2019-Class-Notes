import nltk
import re


def read_file_as_string(file_directory):
    f = open(file_directory, "r")
    all_records = f.read()
    f.close()
    return all_records


records = read_file_as_string("all-places-strings.lc.txt")
tokens = nltk.word_tokenize(records)
#print(tokens)

for token in tokens:
    if len(token) > 4:
        print(token)
