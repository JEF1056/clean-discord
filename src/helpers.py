import io
import re
import random
import threading
import argparse
import time
from datetime import datetime

alphabets=io.open("src/alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
replace_names={}
normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}

class worker(threading.Thread):
    def __init__(self, filename, ilist, olist, pbar, disposed, completed, args):
        threading.Thread.__init__(self)
        self.filename, self.ilist, self.olist = filename, ilist, olist
        self.pbar, self.disposed, self.completed, self.args = pbar, disposed, completed, args

    def run(self):
        self.pbar.set_description(f"{threading.active_count()} active workers")
        last_known_name=""
        last_known_time=0
        build=""
        temp=[]
        title=self.filename.split(" - ")
        try: part=re.findall(r"\[part (\d)\]",self.filename)[0]
        except: part=0
        for curr_message in self.ilist:
            msg=clean(curr_message["content"])
            if msg != None:
                if curr_message["author"]["name"] != last_known_name:
                    last_known_name=curr_message["author"]["name"]
                    build+=f"\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}"
                else:
                    build+="\\n"+msg
            else:self.disposed+=1
            today=time.mktime(datetime.strptime(curr_message["timestamp"].split(".")[0].replace("+00:00",""), "%Y-%m-%dT%H:%M:%S").timetuple())
            if today-last_known_time > self.args.conversation_timeout and last_known_time != 0:
                if build.startswith("\t"): build=build[1:]
                if build.startswith("\\n"): build=build[2:]
                if build.count("\t") > 1 and build != "":
                    temp.append(build.replace("\n","")+"\n")
                    self.completed+=1
                build=""
                last_known_name=""
            last_known_time=today
                
            if self.args.workers == 1: self.pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {self.completed} Removed: {self.disposed}')
            self.pbar.update(1)
        self.olist.append(temp)
        self.pbar.set_description(f"{threading.active_count()} active workers")

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def gen_name(username):
    try:
        int(username)
        try: out_name=replace_names[username]
        except: 
            out_name=random.choice(names)
            replace_names[username]=out_name
        return out_name
    except: return "@"+random.choice(names)

def clean(text, author=None):
    if "```" in text: return None   
    
    text= re.sub(r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)', "", text) #remove urls
    temp=""
    for char in text.strip():
        convi=None
        if char not in alphabets[0]:
            for alphabet in alphabets[1:]:
                try: convi=alphabet.index(char)
                except ValueError: pass
                if convi != None:
                    temp+= alphabets[0][convi]
                    break
        if convi==None: temp+=char
    text= temp
    text= text.replace("\t"," ") #handle tabs
    if author == None: text= re.sub(r'@Deleted User', gen_name, text) #replace "deleted users" with names
    text= re.sub(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\U00000020]', " ", text) #handle... interesting spaces
    text= "".join([normalize_chars[char] if char in normalize_chars else char for char in text.strip()]) #handle special chars from other langs
    text= re.sub(r'([:.,!?@\'\"]|\\n) ([:.,!?\'\"]|\\n)', r'\1\2', text) #handle extraneous spaces between punctuation    
    text= re.sub(r"[^A-Za-z1-9.!@?\"\'\s\U0001F600-\U0001F64F\U0001F300-\U0001F5FF]+", "",text.strip()) #handle non-emoji, punctuation, and letters
    text= re.sub(r"(?i)([\.a-z])\1{3,}", r"\1\1\1", text.strip()) #handle excessive repeats of letters or ...
        #text= re.sub(r"([A-Za-z])\.{2,}", r"\1 ... ", text.strip())
    text= re.sub(r"([\s!?@\"\'])\1+", r"\1",text.strip()) #handle excessive spaces or excessive punctuation
    text= re.sub(r'\s([?.!\"](?:\s|$))', r'\1', text) #handle spaces before punctuation but after text
    text= text.replace("\n","\\n") #handle newlines
    
    if text != "\\n" and text != " " and text != "" and author==None:
        return text
    elif text != "\\n" and text != " " and text != "" and text != "Deleted User" and author!=None:
        return text
    elif author!=None:
        return gen_name(author)
    else:
        return None