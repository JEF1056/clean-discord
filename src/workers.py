import re
import io
import os
import json
import ijson
import ciso8601
import numpy as np
from src.helpers import clean
from pyinstrument import Profiler
from profanity_check import predict
from atpbar import atpbar, register_reporter, find_reporter
    
global reporter
reporter = find_reporter()
def clear():
    if os.name == 'nt':
        _ = os.system('cls')
    else:
        _ = os.system('clear')
    
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

def worker_regex(filename, input_folder, output_folder, mem_load=False, debug=False):
    global reporter
    if debug: profiler = Profiler(); profiler.start()
    
    if mem_load: messages=json.load(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"))["messages"]
    else:messages=ijson.items(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"), 'messages.item')
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    fst, count=True,{"channel": re.search(r"\[\d{18}\]", filename).group(0),"conversations":0,"messages":0,"removed_messages":0}
    if mem_load:
        register_reporter(reporter)
        iterable=atpbar(messages, name=ch)
    else:iterable=messages
    
    with io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8") as f:
        msg, last_seen, last_author, curr_time=[],None,"",0
        for data in iterable:
            if data['author']['isBot'] == False and data["type"] == "Default" and data["content"] and len(data["content"])<500:
                cleaned=clean(f'{data["author"]["name"].replace(":","")}: {data["content"]}', author=data["author"]["id"])
                if cleaned:
                    if last_author != data["author"]["id"] or len(msg)==0:
                        msg.append(cleaned)
                        count["messages"]+=1
                        curr_time=ciso8601.parse_datetime(data['timestamp'])
                        if (last_seen and ((curr_time - last_seen).total_seconds() > 600 and len(msg) > 1)):
                            msg="\t".join(msg)
                            if fst: f.write(msg); fst=False
                            else: f.write("\n"+msg)
                            msg=[]; last_author=""; last_seen=None; count["conversations"]+=1
                        last_author = data["author"]["id"]
                    else:
                        msg[len(msg)-1]+=f"\\n{cleaned[cleaned.find(': ')+2:]}"
                else:
                    count["removed_messages"]+=1
            else:
                count["removed_messages"]+=1
            last_seen = curr_time
        if msg!=[]:
            msg="\t".join(msg)
            if fst: f.write(msg); fst=False
            else: f.write("\n"+msg)
    os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.txt"))
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
    if mem_load and not debug:clear()
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