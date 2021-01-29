import io
import os
import re
import time
import json
import argparse
from tqdm import tqdm
from src.helpers import *
from datetime import datetime
from src.validate import check_files

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="./",
                    help='the folder to output the cleaned files')
parser.add_argument('-conversation_timeout', type=int, default=600,
                    help='amount of time before a conversation is considered dead (in minutes) default is 10 min')
parser.add_argument('-update_interval', type=int, default=1000,
                    help='TQDM update interval')
parser.add_argument('-min_messages', type=int, default=2,
                    help='TQDM update interval')
parser.add_argument("-disable_description", type=str2bool, nargs='?', const=True, default=False,
                    help="disable TQDM description")
parser.add_argument("-cache", type=str2bool, nargs='?', const=True, default=False,
                    help="turn on cache when reading files (uses a lot of memory)")
parser.add_argument('-step', type=str, default="clean", choices=["clean", "nontoxic"],
                    help="which step to start on (in case you've already cleaned the data)")
parser.add_argument("-ascii", type=str2bool, nargs='?', const=True, default=False,
                    help="create an extra file that includes ascii-only data")
parser.add_argument("-pairs", type=str2bool, nargs='?', const=True, default=False,
                    help="takes into account discord's new replies feature to create a file fo only sentence pairs")

parser.add_argument("-nontoxic", type=str2bool, nargs='?', const=True, default=False,
                    help="use an AI to clean text files")
parser.add_argument('-nontoxic_source', type=str, default="context", choices=["context", "context-ascii"],
                    help="clean ascii or non-ascii context")
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
all_data_files=sorted(os.listdir(args.dir))
if args.pairs: assert args.min_messages == 2

if args.step == "clean":
    print(check_files(args.dir))
    all_messages={} if args.cache else 0
    with tqdm(all_data_files, desc="Reading files") as pbar:
        for file in pbar:
            if type(all_messages) == tuple:
                all_messages[file]=json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"]
            else:
                with open(os.path.join(args.dir,file), 'rb') as f:
                    f.seek(-2, os.SEEK_END)
                    while f.read(1) != b' ':
                        f.seek(-2, os.SEEK_CUR)
                    last_line = f.readline().decode()
                    all_messages+=int(int(last_line))
            pbar.set_description(f"Found {sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages)==tuple else all_messages} messages")
    
    try: os.mkdir(args.out)
    except FileExistsError: pass
    
    len_all_messages=sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages)==tuple else all_messages
    if args.ascii: a=io.open(os.path.join(args.out,"context-ascii.txt"), mode="w", encoding="utf-8")
    if args.pairs: p=io.open(os.path.join(args.out,"context-pairs.txt"), mode="w", encoding="utf-8")
    with tqdm(total=len_all_messages, desc="Processing messages") as pbar, io.open(os.path.join(args.out,"context.txt"), mode="w", encoding="utf-8") as f:
        last_id="0"
        for file in all_data_files:
            file_data = all_messages[file] if type(all_messages)==tuple else json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"]
            title=file.split(" - ")
            try: part=re.findall(r"\[part (\d)\]",file)[0]
            except: part=1
            if args.pairs: #generate a dict of messages and their index in messages
                message_indexes={msgdata["id"]:loc for loc, msgdata in file_data}
            if args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
            if re.findall(r"\[\d{18,}\]",file)[0] != last_id:
                last_known_name=""
                last_known_time=0
                build=""
                last_id=re.findall(r"\[\d{18,}\]",file)[0]
            for curr_message in file_data:
                if not curr_message["author"]["isBot"]:
                    today=time.mktime(datetime.strptime(curr_message["timestamp"].split(".")[0].replace("+00:00",""), "%Y-%m-%dT%H:%M:%S").timetuple())
                    msg=clean(curr_message["content"])
                    try:
                        source=curr_message["reference"]["messageId"]
                        source_author=clean(file_data[message_indexes[source]]["author"]["name"], author=file_data[message_indexes[source]]["author"]["id"])
                        source_msg=clean(file_data[message_indexes[source]]["content"])
                        p.write(f"{source_author}: {source_msg}\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}\n")
                    except Exception as e: print(e)
                    if msg != None:
                        if curr_message["author"]["name"] != last_known_name or build=="":
                            last_known_name=curr_message["author"]["name"]
                            build+=f"\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}"
                        else:
                            build+="\\n"+msg
                    else:disposed+=1
                    if today-last_known_time > args.conversation_timeout and last_known_time != 0:
                        build=re.sub(r"^[\t\\n]+","", build.replace("\n","\\n"))
                        if len(build.split("\t")) > 1 and build != "":
                            f.write(build+"\n")
                            if args.ascii: a.write(build.replace("\n","").encode("ascii", "ignore").decode()+"\n")
                            completed+=1
                        else: disposed+=1
                        build=""
                        last_known_name=""
                    last_known_time=today
                    if not args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
                    pbar.update(1)
                else: disposed+=1
            if args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
    del all_messages
    if args.ascii: a.close()
    if args.pairs: p.close()

if args.step == "nontoxic" or args.nontoxic:
    from tox_block.prediction import make_predictions as detect       
    to_clean=io.open(os.path.join(args.out,f"{args.nontoxic_source}.txt"), mode="r", encoding="utf-8").read().strip().split("\n")
    with io.open(os.path.join(args.out,f"{args.nontoxic_source}-detox.txt"), mode="w", encoding="utf-8") as f:
        with tqdm(to_clean, desc="Processing messages") as pbar:
            batch=[]
            for curr_index,conversation in enumerate(pbar):
                batch.append(conversation)
                if curr_index==len(to_clean)-1 or sum([len(msgs.strip().split("\t")) for msgs in batch]) >= args.batches:
                    batch_placement,sents=[0],[]
                    for conv in batch:
                        splt=[e for e in conv.strip().split("\t") if e != ""]
                        sents.extend([remove.replace("\\n","\n").strip() for remove in splt]) #not sure currently if the tox-block model is affected by "\\n", experiment?
                        batch_placement.append(len(splt))
                    prediction_vals=detect(sents)
                    scores=[max(list(dict(prediction_vals[detection]).values())[1:]) for detection in prediction_vals]
                    offsets=[sum(batch_placement[0:i]) for i in range(1,len(batch_placement))]
                    for ind, batch_score in enumerate([scores[sum(batch_placement[0:i]):sum(batch_placement[0:i])+batch_placement[i]] for i in range(1,len(batch_placement))]):
                        to_write=[]
                        for i,v in enumerate(batch_score):
                            if v <= args.confidence: to_write.append(sents[offsets[ind]+i].replace("\n","\\n"))
                            else: disposed_tox+=1
                        if to_write[0].startswith("\\n"): to_write=to_write[1:]
                        if len(to_write) < 2: 
                            disposed+=len(to_write)
                        else:
                            to_write="\t".join(to_write)
                            f.write(to_write+"\n")
                    pbar.set_description(f"From {args.nontoxic_source}.txt, Batch: {len(sents)}, Removed: {disposed_tox}")
                    batch=[]

print(f"Removed {disposed}+{disposed_tox}/{len_all_messages}, {round((disposed+disposed_tox)/len_all_messages,2)}%")
final_file_path=os.path.join(args.out,f'{args.nontoxic_source}{"-detox" if args.nontoxic else ""}.txt')
print(f"Dataset final size: {len_all_messages - disposed - disposed_tox} messages, reduced from "+
      f"{sizeof_fmt(sum([os.path.getsize(f'{os.path.join(args.dir,fle)}') for fle in os.listdir(args.dir)]))} to "+
      f"{sizeof_fmt(os.path.getsize(os.path.join(args.out,f'{args.nontoxic_source}-detox.txt'))) if args.nontoxic else sizeof_fmt(os.path.getsize(final_file_path))}")