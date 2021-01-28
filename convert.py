import io
import os
import re
import time
import json
import random
from tqdm import tqdm
from datetime import datetime

data_dir="data"

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
#test_neg=list(dict(detect(["test"])).values())[1:]
#test_pos=list(dict(detect(["slut"])).values())[1:]
#print(f'''\n\nFalse:{max(test_neg)}:{test_neg}
#True:{max(test_pos)}:{test_pos}''')

alphabets=io.open("alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
replace_names={}
normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}

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
        # add code to replace names
        return text
    elif author!=None:
        return gen_name(author)
    else:
        return None

all_messages={}
with tqdm(os.listdir(data_dir), desc="Reading files") as pbar:
    for file in pbar:
        all_messages[file]=json.load(io.open(f"{data_dir}/{file}", mode="r", encoding="utf-8"))["messages"]
        pbar.set_description(f"Found {all_messages} messages")
   
disposed=0 
completed=0
with tqdm(total=len(all_messages), desc="Processing messages") as pbar, io.open(f"context.txt", mode="w", encoding="utf-8") as f:
    last_id="0"
    for file in all_messages:
        if re.findall(r"\[\d{18,}\]",file)[0] != last_id:
            last_known_name=""
            last_known_time=0
            build=""
            last_id=re.findall(r"\[\d{18,}\]",file)[0]
        for curr_message in all_messages[file]:
            msg=clean(curr_message["content"])
            if msg != None:
                if curr_message["author"]["name"] != last_known_name:
                    last_known_name=curr_message["author"]["name"]
                    build+=f"\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}"
                else:
                    build+="\\n"+msg
            else:disposed+=1
            try: today=time.mktime(datetime.strptime(curr_message["timestamp"].split(".")[0].replace("+00:00",""), "%Y-%m-%dT%H:%M:%S").timetuple())
            except: print(curr_message["timestamp"])
            if today-last_known_time > 1800 and last_known_time != 0:
                if build.startswith("\t"): build=build[1:]
                if build.startswith("\\n"): build=build[2:]
                if build.count("\t") > 1 and build != "":
                    f.write(build.replace("\n","")+"\n")
                    completed+=1
                build=""
                last_known_name=""
            last_known_time=today
                
            #CasualConversation - I made this - writers [695705759597723689].json
            title=file.split(" - ")
            try:
                part=re.findall(r"\[part (\d)\]",file)[0]
            except:
                part=0
            pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
            pbar.update(1)

from tox_block.prediction import make_predictions as detect       
to_clean=io.open(f"context.txt", mode="r", encoding="utf-8").read().strip().split("\n")
disposed_tox=0
with io.open(f"context-detox.txt", mode="w", encoding="utf-8") as f:
    with tqdm(to_clean, desc="Processing messages") as pbar:
        for conversation in pbar:
            sents=conversation.strip().split("\t")
            pbar.set_description(f"Batch of {len(sents)}, Removed {disposed_tox}")
            prediction_vals=detect(sents)
            scores=[max(list(dict(prediction_vals[detection]).values())[1:]) for detection in prediction_vals]
            to_write=[]
            for i,v in enumerate(scores):
                if v <= 0.85: to_write.append(sents[i])
                else: disposed_tox+=1
            to_write="\t".join(to_write)
            f.write(to_write+"\n")

print(f"Removed {disposed}+{disposed_tox}/{all_messages}, {round((disposed+disposed_tox)/all_messages,2)}%")
print(f"Dataset final size: {all_messages - disposed - disposed_tox} messages, reduced from {sum([os.path.getsize(f'{data_dir}/{fle}') >> 20 for fle in os.listdir(data_dir)])}mb to {os.path.getsize('context-detox.txt') >> 20}mb")