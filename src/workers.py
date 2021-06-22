import re
import io
import os
import json
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

def worker_regex(filename, input_folder, output_folder, debug=False):
    global reporter
    register_reporter(reporter)
    if debug: profiler = Profiler(); profiler.start()
    
    messages, ch=json.load(io.open(os.path.join(input_folder,filename), mode="r", encoding="utf-8"))["messages"], re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    temp= {"channel":ch, "stats": {"original":len(messages), "removed": [], "current":[]}, "conversations":[]}
    msg, last_seen, last_author, curr_time=[],None,"",0
    
    for data in atpbar(messages, name=ch):
        if data['author']['isBot'] == False and data["type"] == "Default" and data["content"] and len(data["content"])<500:
            cleaned=clean(f'{data["author"]["name"].replace(":","")}: {data["content"]}', author=data["author"]["id"])
            if cleaned:
                if last_author != data["author"]["id"] or len(msg)==0:
                    msg.append([cleaned, data["author"]["id"]])
                    curr_time=ciso8601.parse_datetime(data['timestamp'])
                    if (last_seen and ((curr_time - last_seen).total_seconds() > 600 and len(msg) > 1)):
                        #msg="\t".join(msg)
                        temp["conversations"].append(msg)
                        msg=[]; last_author=""; last_seen=None
                    last_author = data["author"]["id"]
                else:
                    msg[-1][0]+=f"\\n{cleaned[cleaned.find(': ')+2:]}"
        last_seen = curr_time
    if msg!=[]: temp["conversations"].append(msg); msg=[]
    
    temp["stats"]["current"].append(sum([len(convo) for convo in temp["conversations"]]))
    temp["stats"]["removed"].append(temp["stats"]["original"] - temp["stats"]["current"][-1])
    json.dump(temp, io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8"))
    try: os.remove(os.path.join(output_folder,f"{ch}.json"))
    except: pass
    os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.json"))
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
    clear()

def worker_detox(filename, output_folder, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    temp, temp_lst = json.load(io.open(os.path.join(output_folder,f"{ch}.json"), mode="r", encoding="utf-8")), []
    
    for convo in temp["conversations"]:
        convo=np.array(convo)
        pred_map=predict(np.array([msg[0] for msg in convo]))
        temp_lst.append(convo[pred_map < 1].tolist())
        
    temp["conversations"]=temp_lst
    temp["stats"]["current"].append(sum([len(convo) for convo in temp["conversations"]]))
    temp["stats"]["removed"].append(temp["stats"]["original"] - temp["stats"]["current"][-1])
    json.dump(temp, io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8"))
    try: os.remove(os.path.join(output_folder,f"{ch}.json"))
    except: pass
    os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.json"))
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
    clear()
            
def worker_antispam(filename, output_folder, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", filename).group(0)
    temp, temp_lst = json.load(io.open(os.path.join(output_folder,f"{ch}.json"), mode="r", encoding="utf-8")), []
    
    for convo in temp["conversations"]:
        convo=np.array(convo)
        pred_map=antispam(np.array([msg[0] for msg in convo]))
        temp_lst.append(convo[pred_map < 1].tolist())
        
    temp["conversations"]=temp_lst
    temp["stats"]["current"].append(sum([len(convo) for convo in temp["conversations"]]))
    temp["stats"]["removed"].append(temp["stats"]["original"] - temp["stats"]["current"][-1])
    json.dump(temp, io.open(os.path.join(output_folder,f"{ch}.temp"), mode="w", encoding="utf-8"))
    try: os.remove(os.path.join(output_folder,f"{ch}.json"))
    except: pass
    os.rename(os.path.join(output_folder,f"{ch}.temp"),os.path.join(output_folder,f"{ch}.json"))
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
    clear()