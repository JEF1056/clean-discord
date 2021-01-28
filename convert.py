import io
import os
import re
import time
import json
import argparse
from tqdm import tqdm
from src.helpers import *
from datetime import datetime

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="./",
                    help='the folder to output the cleaned files')
parser.add_argument('-conversation_timeout', type=int, default=1800,
                    help='amount of time before a conversation is considered dead (in minutes) default is 30 min')
parser.add_argument('-update_interval', type=int, default=1000,
                    help='TQDM update interval')
parser.add_argument("-disable_description", type=str2bool, nargs='?', const=True, default=False,
                    help="disable TQDM description")
parser.add_argument("-cache", type=str2bool, nargs='?', const=True, default=False,
                    help="turn on cache when reading files (uses a lot of memory)")
parser.add_argument('-step', type=int, default=0, choices=[0,1],
                    help='TQDM update interval')

parser.add_argument("-nontoxic", type=str2bool, nargs='?', const=True, default=False,
                    help="use an AI to clean text files")
parser.add_argument("-batches", type=int, default=100,
                    help="minimum number of batches to feed the AI (only needed if -nontoxic is used)")
parser.add_argument("-confidence", type=float, default=0.85,
                    help="AI must be > 0.85 sure that the message is toxic to remove it")
args = parser.parse_args()


os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

disposed_tox=0
disposed=0 
completed=0
len_all_messages=1

if args.step < 1:
    all_messages={} if args.cache else 0
    with tqdm(os.listdir(args.dir), desc="Reading files") as pbar:
        for file in pbar:
            if type(all_messages) == tuple:
                all_messages[file]=json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"]
            else:
                all_messages+=len(json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"])
            pbar.set_description(f"Found {sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages)==tuple else all_messages} messages")
    
    try: os.mkdir(args.out)
    except FileExistsError: pass
    
    len_all_messages=sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages)==tuple else all_messages
    with tqdm(total=len_all_messages, desc="Processing messages") as pbar, io.open(os.path.join(args.out,"context.txt"), mode="w", encoding="utf-8") as f:
        last_id="0"
        for file in len(os.listdir(args.dir)):
            file = all_messages[file] if type(all_messages)==tuple else json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"]
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
                if today-last_known_time > args.conversation_timeout and last_known_time != 0:
                    if build.startswith("\t"): build=build[1:]
                    if build.startswith("\\n"): build=build[2:]
                    if build.count("\t") > 1 and build != "":
                        f.write(build.replace("\n","")+"\n")
                        completed+=1
                    build=""
                    last_known_name=""
                last_known_time=today
                    
                title=file.split(" - ")
                try:
                    part=re.findall(r"\[part (\d)\]",file)[0]
                except:
                    part=0
                if not args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
                pbar.update(1)
    del all_messages

if args.step < 2:
    if args.nontoxic:
        from tox_block.prediction import make_predictions as detect       
        to_clean=io.open(os.path.join(args.out,"context.txt"), mode="r", encoding="utf-8").read().strip().split("\n")
        with io.open(os.path.join(args.out,"context-detox.txt"), mode="w", encoding="utf-8") as f:
            with tqdm(to_clean, desc="Processing messages") as pbar:
                batch=[]
                for curr_index,conversation in enumerate(pbar):
                    batch.append(conversation)
                    if curr_index==len(to_clean)-1 or sum([len(msgs.strip().split("\t")) for msgs in batch]) >= args.batches:
                        batch_placement,sents=[0],[]
                        for conv in batch:
                            splt=[e for e in conv.strip().split("\t") if e != ""]
                            sents.extend(splt)
                            batch_placement.append(len(splt))
                        prediction_vals=detect(sents)
                        scores=[max(list(dict(prediction_vals[detection]).values())[1:]) for detection in prediction_vals]
                        offsets=[sum(batch_placement[0:i]) for i in range(1,len(batch_placement))]
                        for ind, batch_score in enumerate([scores[sum(batch_placement[0:i]):sum(batch_placement[0:i])+batch_placement[i]] for i in range(1,len(batch_placement))]):
                            to_write=[]
                            for i,v in enumerate(batch_score):
                                if v <= args.confidence: to_write.append(sents[offsets[ind]+i])
                                else: disposed_tox+=1
                            to_write="\t".join(to_write)
                            f.write(to_write+"\n")
                        pbar.set_description(f"Batch of {len(sents)}, Removed {disposed_tox}")
                        batch=[]

print(f"Removed {disposed}+{disposed_tox}/{len_all_messages}, {round((disposed+disposed_tox)/len_all_messages,2)}%")
print(f"Dataset final size: {len_all_messages - disposed - disposed_tox} messages, reduced from {sizeof_fmt(sum([os.path.getsize(f'{os.path.join(args.dir,fle)}') >> 20 for fle in os.listdir(args.dir)]))} to {sizeof_fmt(os.path.getsize(os.path.join(args.out,'context-detox.txt'))) if args.nontoxic else sizeof_fmt(os.path.getsize(os.path.join(args.out,'context.txt')))}")