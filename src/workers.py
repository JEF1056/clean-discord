import re
import io
import os
import json
import ijson
import random
import ciso8601
import numpy as np
from pyinstrument import Profiler
from profanity_check import predict

normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}
alphabets=io.open("src/alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
emojis=json.load(io.open("src/emojis.json", mode="r", encoding="utf-8"))
for alphabet in alphabets[1:]:
    for ind, char in enumerate(alphabet):
        try:normalize_chars[char]=alphabets[0][ind]
        except: print(alphabet, len(alphabet), len(alphabets[0]));break
normalize_chars.update({i:i for i in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'})

normal_map=str.maketrans(normalize_chars)
del normalize_chars

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
    except: return " @"+random.choice(names)
    
def write_stats(ret, dir):
    messages_total, conversations_total, removed_total, new_ret=0,0,0, {}
    for val in ret:
        messages_total+=val["messages"]
        conversations_total+=val["conversations"]
        removed_total+=val["removed_messages"]
        try: new_ret[val["channel"]]={"messages": new_ret[val["channel"]]["messages"]+val["messages"], "conversations": new_ret[val["channel"]]["conversations"]+val["conversations"]}
        except: new_ret[val["channel"]]={"messages": val["messages"], "conversations": val["conversations"]}
    json.dump({"messages_total":messages_total,"conversations_total":conversations_total, "removed_total":removed_total, "num_files":len(ret), "num_channels":len(new_ret), "individual":ret, "merged":new_ret}, open(os.path.join(dir,"stats.json"),"w"))
    
#precompile regex
r1=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t ]{2,}')
r2=re.compile(r'@Deleted User')
r3=re.compile(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|:.+?:|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```.+?```\n?|(?:\\n)+|(?<=[.,!?()]) (?=[.,!?()])|\b(?:a*ha+h[ha]*|o?l+o+l+[ol]*)\b|(?![:;][3DP])[^a-z0-9.,\'@!?\s\/'+''.join(emojis)+r']+|([a-z])\s([.,\'@!?\/])', flags=re.DOTALL | re.IGNORECASE)
r4=re.compile(r"([a-z.])\1{3,}|([,\'@!?\s\/])\2+", re.IGNORECASE)
r5=re.compile(r'['+"".join(emojis)+r']')

def convemojis(i):
    if i in emojis: return emojis[i]
    return i

def clean(text, author=None):
    if text.lower().startswith(bot_prefixes): return None #handle bot commands
    if author != None and text == "Deleted User": return gen_name(author).strip()
    
    text=text.translate(normal_map)#handle special chars from other langs
    text= re.sub(r1, " ", text) #handle... interesting spaces
    if author == None: text= re.sub(r2, gen_name, text) #replace "deleted users" with names
    text= re.sub(r3, r"\2\3", text.strip()) #remove urls, emails, code blocks, custom emojis, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r4, r"\1\1\1\2", text) #handle excessive repeats of punctuation, limited to 3, repeated words, excessive spaces or excessive punctuation, spaces before punctuation but after text
    text= "".join(list(map(convemojis,text))) #translate emojis to their `:text:` shorthand form
    text= text.replace("\n","\\n").strip().strip("\\n").strip("\t") #handle newlines
         
    if text != "\\n" and text != " " and text != "" and author==None:
        return text
    elif text != "\\n" and text != " " and text != "" and author!=None:
        return " ".join(text.split(" ")[-2:])
    else:
        return None

def worker_regex(filename, input_folder, output_folder, max_context=1000, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    messages, fst, count=ijson.items(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), 'messages.item'), True,{"channel": re.search(r"\[\d{18}\]", filename).group(0),"conversations":0,"messages":0,"removed_messages":0}
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    with io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8") as f:
        msg, last_seen, last_author, curr_time=[],None,"",0
        for data in messages:
            if data['author']['isBot'] == False and data["type"] == "Default" and data["content"]:
                content, author=clean(data["content"]),clean(data["author"]["name"], author=data["author"]["id"])
                if content and author:
                    if last_author != author or len(msg)==0:
                        msg.append(f'{author}: {content}')
                        count["messages"]+=1
                        curr_time=ciso8601.parse_datetime(data['timestamp'])
                        if len(msg)==max_context or (last_seen and ((curr_time - last_seen).total_seconds() > 600 and len(msg) > 1)):
                            msg="\t".join(msg)
                            if fst: f.write(msg); fst=False
                            else: f.write("\n"+msg)
                            msg=[]; last_author=""; last_seen=None; count["conversations"]+=1
                        last_author = author
                    else:
                        msg[len(msg)-1]+=f"\\n{content}"
                else:
                    count["removed_messages"]+=1
            else:
                count["removed_messages"]+=1
            last_seen = curr_time
    os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.txt"))
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))#profiler.open_in_browser()
    return count

def worker_detox(filename, input_folder, output_folder, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    file_data, fst, count=io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), True, {"channel": re.search(r"\[\d{18}\]", filename).group(0),"conversations":0,"messages":0,"removed_messages":0}
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    with io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8") as f:
        while True:
            line = file_data.readline().strip()
            if not line:
                os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.txt"))
                if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
                return count
            count["conversations"]+=1
            line=np.array(line.split("\t"))
            pred_map=predict(line)
            count["removed_messages"], np.count_nonzero(pred_map == 1)
            count["messages"] += np.count_nonzero(pred_map == 0)
            line=line[pred_map < 1]
            if len(line) > 1:
                if fst: f.write("\t".join(line)); fst=False
                else: f.write("\n"+"\t".join(line))
            
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Clean Discord data')
    parser.add_argument('-dir', type=str, default="data",
                        help='the fonlder that contains the data file')
    parser.add_argument('-out', type=str, default="output",
                        help='the folder to output txts')
    parser.add_argument('-step', type=str, default="all",
                        help='the step to start cleaning from')
    args = parser.parse_args()
    
    worstcase_clean="""
Hi, this is a test.
```
This is some code:
if args.step == "all":
    steps=["regex", "pairs", "detox"]
else:
    try:
        steps=json.loads(args.step)
        assert type(steps)==list
    except: raise Exception("Unable to load steps json.")
```
`this too, but it's only one line`
heh maybe if i put it on one line ```e``` or does `this` work
REEE WHY IS IS BEING CLEANED OOOOF HOWWWWWWWW
I AM THE KING ♕ ✦ —• YOU CANNOT STOP ME
what about this 𝐈𝐌𝐀𝐆𝐄, it should be IMAGE.
hahahahaha i bet you can't beat my cool asian language 毛泽东万岁
WHAAAAAAAAAAAAAAAAAAAAAAT noooooooooooooooooooooooo it can't be..................
hahaha but my best invention yet, my friend @Deleted User and @Deleted User. They will surely defeat you.
                     plenty              of                      spaces               ???????????????       🥲
fine. one last resort. my email is contact@j-fan.ml and you can join my server at https://jadeai.ml/server. Join or else.
if those didn't work maybe my phone numbers, +2 (666) 768-1111 or 408 220 0343 will work
    """
    
    print("Running a clean test case ~~~~~~~~")
    print(f"{worstcase_clean}\nRaw ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(clean(worstcase_clean).replace("\\n","\n"))
    print("Clean ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    
    if args.step == "all":
        steps=["regex", "pairs", "detox"]
    else:
        try:
            steps=json.loads(args.step)
            assert type(steps)==list
        except: raise Exception("Unable to load steps json.")
    for step in steps:
        if step == "regex":
            print("\033[1mRunning regex test\033[0m")
            ret=[worker_regex(os.listdir(args.dir)[0], args.dir, args.out, debug=True)]
            write_stats(ret, args.out)
        elif step == "pairs":
            print("\033[1mPairs test not implemented\033[0m")
            pass #not implemented
        elif step == "detox":
            print(f"\033[1mRunning detox test\033[0m")
            ret=[worker_detox([f for f in os.listdir(args.out) if f.endswith(".txt")][0], args.out, args.out+"-detox", debug=True)]
            write_stats(ret, args.out+"-detox")
    print("DONE")