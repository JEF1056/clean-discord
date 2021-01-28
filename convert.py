import io
import os
import re
import sys
import json
import time
from tqdm import tqdm
from datetime import datetime
from tox_block.prediction import make_single_prediction as detect

data_dir="data"

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
print(f'{detect("test")}\n{detect("slut")}')

alphabets=io.open("alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}

def clean(text, author=False):
    if "```" in text or \
    "https://" in text or \
    "http://" in text: return None   
     
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
    text=temp
    text= text.replace("\t"," ") #handle tabs
    text= re.sub(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\U00000020]', " ", text) #handle... interesting spaces
    text="".join([normalize_chars[char] if char in normalize_chars else char for char in text.strip()]) #handle special chars from other langs
    text= re.sub(r'([:.,!?]|\\n) ([:.,!?]|\\n)', r'\1\2', text) #handle extraneous spaces between punctuation    
    text= re.sub(r"[^A-Za-z1-9.!?\"\s\U0001F600-\U0001F64F\U0001F300-\U0001F5FF]+", "",text.strip()) #handle non-emoji, punctuation, and letters
    text= re.sub(r"(?i)([\.a-z])\1{3,}", r"\1\1\1", text.strip()) #handle excessive repeats of letters or ...
        #text= re.sub(r"([A-Za-z])\.{2,}", r"\1 ... ", text.strip())
    text= re.sub(r"([\s!?\"])\1+", r"\1",text.strip()) #handle excessive spaces or excessive punctuation
    text= re.sub(r'\s([?.!\"](?:\s|$))', r'\1', text) #handle spaces before punctuation but after text
    text= text.replace("\n","\\n") #handle newlines
    
    try: prediction_vals=list(dict(detect(text)).values())[1:]
    except: return None
    if sum(prediction_vals)/len(prediction_vals) >= 0.9:
        if author == False: 
            return None
        else: 
            # add code to replace names
            return "[insertnamehere]"
    if text != "\\n" and text != " " and author==False:
        return text
    elif text != "\\n" and text != " " and author==True:
        # add code to replace names
        return text
    else:
        return "[insertnamehere]"

all_messages=0
with tqdm(os.listdir(data_dir), desc="Reading files") as pbar:
    for file in pbar:
        all_messages+=len(json.load(io.open(f"{data_dir}/{file}", mode="r", encoding="utf-8"))["messages"])
        pbar.set_description(f"Found {all_messages} messages")
   
disposed=0 
completed=0
with tqdm(total=all_messages, desc="Processing messages") as pbar, io.open(f"context.txt", mode="w", encoding="utf-8") as f:
    last_id="0"
    for file in os.listdir(data_dir):
        if re.findall(r"\[\d{18,}\]",file)[0] != last_id:
            last_known_name=""
            last_known_time=0
            build=""
            last_id=re.findall(r"\[\d{18,}\]",file)[0]
        for curr_message in json.load(io.open(f"{data_dir}/{file}", mode="r", encoding="utf-8"))["messages"]:
            msg=clean(curr_message["content"])
            if msg != None:
                if curr_message["author"]["name"] != last_known_name:
                    last_known_name=curr_message["author"]["name"]
                    build+=f"\t{clean(last_known_name,author=True)}: {msg}"
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
            last_known_time=today
                
            #CasualConversation - I made this - writers [695705759597723689].json
            title=file.split(" - ")
            try:
                part=re.findall(r"\[part (\d)\]",file)[0]
            except:
                part=0
            pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
            pbar.update(1)