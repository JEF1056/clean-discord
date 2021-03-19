import re
import io
import os
import ijson
import random
import time
from datetime import datetime
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
r1=re.compile(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|<:.+?:\d+>|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```(?:.?)+```|:[^:\s]*(?:::[^:\s]*)*:|(?:\\n)+|(?<=[:.,!?()]) (?=[:.,!?()])|\b(a*ha+h[ha]*|o?l+o+l+[ol]*)\b|(?!:[3\)\>])[^a-z0-9.,\'@!?\s\/\U0001F600-\U0001F64F\U0001F300-\U0001F5FF]+', flags=re.DOTALL | re.IGNORECASE)
r2=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t]{2,}')
r3=re.compile(r"([\.\'\"@?!a-z])\1{3,}|([\s!?@\"\'])\2+|\s([?.!\"](?:\s|$))", re.IGNORECASE)
r4=re.compile(r'@Deleted User')

def clean(text, author=None):
    if text.lower().startswith(bot_prefixes): return None #handle bot commands
    if author != None and text == "Deleted User": return gen_name(author)
        
    unique=[i for i in list(set(text)) if i not in alphabets[0]] #handle special chars from other langs
    for char in unique: 
        try: text=text.replace(char, normalize_chars[char])
        except:pass
    if author == None: text= re.sub(r4, gen_name, text) #replace "deleted users" with names
    text= re.sub(r1, "", text.strip()) #remove urls, emails, code blocks, custom emojis, spaces between punctuation, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r2, " ", text) #handle... interesting spaces
    text= re.sub(r3, r"\1\1\1\2\3", text) #handle excessive repeats of punctuation, limited to 3, repeated words, excessive spaces or excessive punctuation, spaces before punctuation but after text
    text= text.strip().replace("\n","\\n").strip("\\n").strip("\t") #handle newlines
    
    if text != "\\n" and text != " " and text != "" and author==None:
        return text
    elif text != "\\n" and text != " " and text != "" and author!=None:
        return " ".join(text.split(" ")[-2:])
    else:
        return None

def worker(filename, input_folder, output_folder, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    messages, fst, count=ijson.items(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), 'messages.item'), True,{"server": filename[:-5],"conversations":0,"messages":0}
    with io.open(os.path.join(output_folder,filename.replace(".json",".txt")), mode="w", encoding="utf-8") as f:
        msg, last_seen, last_author, curr_time=[],0,"",0
        for data in messages:
            if data['author']['isBot'] == False and data["type"] == "Default" and data["content"]:
                content, author=clean(data["content"]),clean(data["author"]["name"], author=data["author"]["id"])
                if content != None:
                    try:
                        if last_author != author or len(msg)==0:
                            msg.append(f'{author}: {content}')
                            count["messages"]+=1
                            curr_time=time.mktime(datetime.strptime(data["timestamp"].split(".")[0].replace("+00:00", ""),"%Y-%m-%dT%H:%M:%S",).timetuple())
                            if len(msg)==21 or (curr_time - last_seen > 600 and last_seen != 0 and len(msg) > 1):
                                msg="/b".join(msg)
                                if fst: f.write(msg); fst=False
                                else: f.write("\n"+msg)
                                msg=[]; last_author=""; curr_time=0; count["conversations"]+=1
                            last_author = author
                        else:
                            msg[len(msg)-1]+=f"/n{content}"
                    except Exception as e:
                        print(e,"   from file:", filename)
            last_seen = curr_time
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))#profiler.open_in_browser()
    return count

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Clean Discord data')
    parser.add_argument('-file', type=str, default="data json",
                        help='data json file')
    parser.add_argument('-dir', type=str, default="data",
                        help='the fonlder that contains the data file')
    parser.add_argument('-out', type=str, default="output",
                        help='the folder to output txts')
    args = parser.parse_args()
    worker(args.file, args.dir, args.out, debug=True)