import sys
import requests

words = ["Zoo", "Brutus", "Ceasar", "Jack"]
invert_index = {}
for word in words:
    invert_index[word] = []
print('Beginning file download with requests')

url = 'http://shakespeare.mit.edu'
url_list = [url]
r = requests.get(url)

html = str(r.content)
index_of_html = html.find("href", 0)
index_of_first_quote = index_of_html + 6
index_of_second_quote = html.find("\"", index_of_first_quote)

url_level1 = url + "/" + str(html[index_of_first_quote:index_of_second_quote])
if(url_level1.endswith(".html")):
    print(url_level1)
    url_list.append(url_level1)
    r = requests.get(url_level1)
    print(r.content)
    document = str(r.content)
    for word in words:
        index = document.find(word)
        if index > 0 :
            invert_index["Brutus"].append(url_level1)

for key in invert_index.keys():
    invert_index[key] = sorted(invert_index[key])

print(invert_index)
