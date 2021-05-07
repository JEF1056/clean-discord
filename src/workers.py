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
from src.helpers import clean
    
def write_stats(ret, dir):
    messages_total, conversations_total, removed_total, new_ret=0,0,0, {}
    for val in ret:
        messages_total+=val["messages"]
        conversations_total+=val["conversations"]
        removed_total+=val["removed_messages"]
        try: new_ret[val["channel"]]={"messages": new_ret[val["channel"]]["messages"]+val["messages"], "conversations": new_ret[val["channel"]]["conversations"]+val["conversations"]}
        except: new_ret[val["channel"]]={"messages": val["messages"], "conversations": val["conversations"]}
    json.dump({"messages_total":messages_total,"conversations_total":conversations_total, "removed_total":removed_total, "num_files":len(ret), "num_channels":len(new_ret), "individual":ret, "merged":new_ret}, open(os.path.join(dir,"stats.json"),"w"))
    
def antispam(conversation):
    res=[]
    for convo in conversation:
        if len(convo) < 1500 and len(": ".join(convo.split(": ")[1:])) > 2:
            try:
                for group in re.search(r4, convo).groups():
                    if len(str(group))*2 >= 30:
                        res.append(1)
                    else:
                        res.append(0)
            except:
                res.append(0)
        else: res.append(1)
    return np.array(res)

def worker_regex(filename, input_folder, output_folder, max_context=1000, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    messages, fst, count=ijson.items(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), 'messages.item'), True,{"channel": re.search(r"\[\d{18}\]", filename).group(0),"conversations":0,"messages":0,"removed_messages":0}
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    with io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8") as f:
        msg, last_seen, last_author, curr_time=[],None,"",0
        for data in messages:
            if data['author']['isBot'] == False and data["type"] == "Default" and data["content"]:
                cl=clean(data["author"]["name"]+chr(0)+data["content"], author=data["author"]["id"])
                if cl and len(cl) == 2:
                    author, content = cl
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
            count["removed_messages"] += np.count_nonzero(pred_map == 1)
            count["messages"] += np.count_nonzero(pred_map == 0)
            line=line[pred_map < 1]
            if len(line) > 1:
                if fst: f.write("\t".join(line)); fst=False
                else: f.write("\n"+"\t".join(line))
            else: count["removed_messages"] +=1
            
def worker_antispam(filename, input_folder, output_folder, debug=False):
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
            pred_map=antispam(line)
            count["removed_messages"] += np.count_nonzero(pred_map == 1)
            count["messages"] += np.count_nonzero(pred_map == 0)
            line=line[pred_map < 1]
            if len(line) > 1:
                if fst: f.write("\t".join(line)); fst=False
                else: f.write("\n"+"\t".join(line))
            else: count["removed_messages"] +=1 
            
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Clean Discord data')
    parser.add_argument('-dir', type=str, default="data",
                        help='the fonlder that contains the data file')
    parser.add_argument('-out', type=str, default="output",
                        help='the folder to output txts')
    parser.add_argument('-step', type=str, nargs="+", default="all",
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
if those didn't work maybe my phone numbers, +2 (666) 768-1111 or 408 220 0343 will work. meet me at 12:00 :3
✧・ﾟ:*ａｎｇｅｌａ*:･ﾟ☆✧ ::::::: I am the best
    """
    print("Running a clean test case ~~~~~~~~")
    print(f"{worstcase_clean}\nRaw ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(clean(worstcase_clean).replace("\\n","\n"))
    print("Clean ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    
    if args.step == "all": args.step = ["regex", "detox", "antispam"]
    for step in args.step:
        if step == "regex":
            try:os.mkdir(args.out)
            except: pass
            print("\033[1mRunning regex test\033[0m")
            ret=[worker_regex(os.listdir(args.dir)[0], args.dir, args.out, debug=True)]
            write_stats(ret, args.out)
        elif step == "detox":
            try:os.mkdir(args.out+"-detox")
            except: pass
            print(f"\033[1mRunning detox test\033[0m")
            ret=[worker_detox([f for f in os.listdir(args.out) if f.endswith(".txt")][0], args.out, args.out+"-detox", debug=True)]
            write_stats(ret, args.out+"-detox")
        elif step == "antispam":
            try:os.mkdir(args.out+"-antispam")
            except: pass
            print("\033[1mRunning antispam test\033[0m")
            ret=[worker_antispam([f for f in os.listdir(args.out+"-detox") if f.endswith(".txt")][0], args.out+"-detox", args.out+"-antispam", debug=True)]
            write_stats(ret, args.out+"-antispam")
    print("DONE")