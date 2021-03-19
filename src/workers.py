import re
import io
import os
import ijson
import random
from pyinstrument import Profiler

normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}
alphabets=io.open("src/alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
for alphabet in alphabets[1:]:
    for ind, char in enumerate(alphabet):
        try:normalize_chars[char]=alphabets[0][ind]
        except: print(alphabet, len(alphabet), len(alphabets[0]));break
bot_prefixes=tuple(io.open("src/prefixes.txt", mode="r", encoding="utf-8").read().strip().split("\n"))
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
replace_names={}

def gen_name(username):
    try:
        int(username)
        try: out_name=replace_names[username]
        except: 
            out_name=random.choice(names)
            replace_names[username]=out_name
        return out_name
    except: return "@"+random.choice(names)
    
#precompile regex
r1=re.compile(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|<:.+?:\d+>|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```(?:.?)+```|:[^:\s]*(?:::[^:\s]*)*:|(?:\\n)+|(?<=[:.,!?()]) (?=[:.,!?()])|\b(a*ha+h[ha]*|o?l+o+l+[ol]*)\b|(?!:3)[^a-z0-9.,!@?\s\/\U0001F600-\U0001F64F\U0001F300-\U0001F5FF]+', flags=re.DOTALL | re.IGNORECASE)
r2=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t]+')
r3=re.compile(r"([\.\'\"@?!a-z])\1{3,}|([\s!?@\"\'])\2+|\s([?.!\"](?:\s|$))", re.IGNORECASE)
r4=re.compile(r'@Deleted User')

def clean(text, author=None):
    if text.lower().startswith(bot_prefixes): return None #handle bot commands
    if author != None and text == "Deleted User": return gen_name(author)
        
    unique=[i for i in list(set(text)) if i not in alphabets[0]] #handle special chars from other langs
    for char in unique: 
        try: text=text.replace(char, normalize_chars[char])
        except:pass
        
    text= re.sub(r1, "", text.strip()) #remove urls, emails, code blocks, custom emojis, spaces between punctuation, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r2, " ", text) #handle... interesting spaces
    text= re.sub(r3, r"\1\1\1\2\3", text) #handle excessive repeats of punctuation, limited to 3, repeated words, excessive spaces or excessive punctuation, spaces before punctuation but after text
    if author == None: text= re.sub(r4, gen_name, text) #replace "deleted users" with names
    text= text.strip().replace("\n","\\n") #handle newlines
    
    if text != "\\n" and text != " " and text != "" and author==None:
        return text
    elif text != "\\n" and text != " " and text != "" and author!=None:
        return text.split(" ")[-1]
    else:
        return None

def worker(filename, input_folder, output_folder):
    profiler = Profiler()
    profiler.start()
    messages=ijson.items(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), 'messages.item')
    with io.open(os.path.join(output_folder,filename.replace(".json",".txt")), mode="w", encoding="utf-8") as f:
        for data in messages:
            if data['author']['isBot'] == False and data["type"] == "Default" and data["content"]:
                data["content"]=data["content"].replace("\n","\\n")
                f.write(f'{clean(data["author"]["name"], author=data["author"]["id"])}\t{clean(data["content"])}\n')
                #f.write(f'{data["author"]["name"]}\t{data["content"]}\n')
    profiler.stop()
    profiler.open_in_browser()
    return "Done"

if __name__ == '__main__':
    worker("test.json", "data", "output")