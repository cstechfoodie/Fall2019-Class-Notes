import collections
import glob
import sys
from project_1 import generate_tokens_pipeline


def query_parser(query: str):
    return sorted(generate_tokens_pipeline(query))


def find_file_index(spliting_words, term):
    for i in range(len(spliting_words)):
        if i + 1 < len(spliting_words) and spliting_words[i] < term <= spliting_words[i + 1]:
            return i + 1
        elif i == 0 and term <= spliting_words[i]:
            return 0
        elif i + 1 == len(spliting_words) and term >= spliting_words[i]:
            return i + 1

    return len(spliting_words)


def and_query_resolver(index_files: str, words: list, splits: list, verbose=False):
    res = []

    if len(words) == 0:
        return res

    a = words[0]
    file_index = find_file_index(splits, a)
    f = open(index_files[file_index], "r")
    line = f.readline().strip("\n")
    while line != '':
        if line.split("=")[0] == a:
            res = line.split("=")[1].split(" ")
            break
        elif line > a:
            break
        else:
            line = f.readline().strip("\n")
    if verbose:
        print("index: 0", line)
    for i in range(1, len(words)):
        b_posting = []
        b = words[i]
        line = f.readline().strip("\n")
        while line != '':
            if line.split("=")[0] == b:
                b_posting = line.split("=")[1].split(" ")
                if verbose:
                    print("index:", i, line)
                break
            elif line > b:
                break
            else:
                line = f.readline().strip("\n")
                if line == '':
                    f.close()
                    next_file_index = find_file_index(splits, b)
                    if file_index == next_file_index:
                        b_posting = []
                        break
                    f = open(index_files[next_file_index], "r")
                    line = f.readline().strip("\n")
                    file_index = next_file_index
        res = intersection(res, b_posting)
    f.close()
    return res


def intersection(a, b):
    res = []
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        if int(a[i]) == int(b[j]):
            res.append(a[i])
            i += 1
            j += 1
        elif int(a[i]) < int(b[j]):
            i += 1
        else:
            j += 1
    return res


def or_query_resolver(index_files: str, words: list, splits: list, verbose=False):
    res = []
    if len(words) == 0:
        return res
    a = words[0]
    file_index = find_file_index(splits, a)
    f = open(index_files[file_index], "r")
    line = f.readline().strip("\n")
    while line:
        if line.split("=")[0] == a:
            res = line.rstrip("\n").split("=")[1].split(" ")
            if verbose:
                print("index: 0", line)
            break
        elif line > a:
            break
        else:
            line = f.readline().strip("\n")

    for i in range(1, len(words)):
        b_posting = []
        b = words[i]
        line = f.readline().strip("\n")
        while line:
            if line.split("=")[0] == b:
                b_posting = line.rstrip("\n").split("=")[1].split(" ")
                if verbose:
                    print("index:", i, line)
                break
            elif line > b:
                break
            else:
                line = f.readline().strip("\n")
                if line == '':
                    f.close()
                    next_file_index = find_file_index(splits, b)
                    if file_index == next_file_index:
                        print("[INFO] No postings for ", b)
                        b_posting = []
                        break
                    f = open(index_files[next_file_index], "r")
                    line = f.readline().strip("\n")
                    file_index = next_file_index
        res = res + b_posting
    f.close()
    c = collections.Counter(res)
    if verbose:
        print("[INFO] Counter for OR query: ", c)
    return [index_frequency_pair[0] for index_frequency_pair in c.most_common()]


def help():
    print("[Usage] python3 project_1_query.py -[mode] [query:string] -[v:verbose]")


if __name__ == "__main__":
    f = open("spliting_word.txt", "r")
    spliting_words = f.readline().strip("\n").split(" ")
    f.close()
    print("[INFO] Spliting_words: ", spliting_words)
    print("[INFO] System arguments: ", sys.argv)

    files = sorted(glob.glob("./index/*.txt"))

    try:
        mode = sys.argv[1]
        query = query_parser(sys.argv[2])
        if len(sys.argv) == 4 and sys.argv[3] == "-v":
            verbose = True
        else:
            verbose = False
        print("[INFO] Query: ", query)
        print("[INFO] Mode: ", mode)
    except IndexError:
        print("Error! Please check command input")
        help()
        sys.exit(1)

    if mode == "-a":
        res = and_query_resolver(files, query, spliting_words, verbose)
        print("[INFO]----Results------", res)
    elif mode == "-o":
        res = or_query_resolver(files, query, spliting_words, verbose)
        print("[INFO]----Results------", res)
    else:
        print("[ERROR] missing boolean operator '-a' '-o'")
        help()
