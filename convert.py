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
                    help='override the minimum number of messages that form a conversation')
parser.add_argument("-disable_description", type=str2bool, nargs='?', const=True, default=False,
                    help="disable TQDM description")
parser.add_argument("-cache", type=str2bool, nargs='?', const=True, default=False,
                    help="turn on cache when reading files (uses a lot of memory)")
parser.add_argument('-step', type=str, default="clean", choices=["clean", "nontoxic"],
                    help="which step to start on (in case you've already cleaned the data)")
parser.add_argument("-ascii", type=str2bool, nargs='?', const=True, default=False,
                    help="create an extra file that includes ascii-only data")
parser.add_argument("-pairs", type=str2bool, nargs='?', const=True, default=False,
                    help="takes into account discord's new replies feature to create a file of only sentence pairs (data yeilds will be low)")

parser.add_argument("-nontoxic", type=str2bool, nargs='?', const=True, default=False,
                    help="use an AI to clean text files")
parser.add_argument('-nontoxic_source', type=str, default="context", choices=["context", "context-ascii", "context-pairs"],
                    help="clean ascii or non-ascii context")
parser.add_argument("-batches", type=int, default=100,
                    help="minimum number of batches to feed the AI (only needed if -nontoxic is used)")
parser.add_argument("-confidence", type=float, default=0.85,
                    help="AI must be > 0.85 sure that the message is toxic to remove it")
args = parser.parse_args()


os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

#these are primarily statistics and the sort
disposed_tox=0
disposed=0 
completed=0
len_all_messages=1
all_data_files=sorted(os.listdir(args.dir))
if args.pairs: assert args.min_messages == 2

#this if statement encapsulates the code used to load the total number of messages into memory
#this is either by the means of reading each file backward and checking for the messagescount field or just brute-force caching them into memory
if (args.cache==False and args.step == "nontoxic") or args.step == "clean":
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

#NOTE: NEED TO MULTITHREAD
if args.step == "clean":
    len_all_messages=sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages)==tuple else all_messages #this is to determine the length of tqdm's progress bar
    if args.ascii: a=io.open(os.path.join(args.out,"context-ascii.txt"), mode="w", encoding="utf-8") #this (and the line below it) opens extra files for different arguments that have been passed
    if args.pairs: p=io.open(os.path.join(args.out,"context-pairs.txt"), mode="w", encoding="utf-8") #i don't think that you need to multithread these, unless there is a way to do that
    with tqdm(total=len_all_messages, desc="Processing messages") as pbar, io.open(os.path.join(args.out,"context.txt"), mode="w", encoding="utf-8") as f: #initializes tqdm and the primary file to write to
        last_id="0"
        for file in all_data_files: # loop through each file containing messages
            file_data = all_messages[file] if type(all_messages)==tuple else json.load(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8"))["messages"] #load the file or if cached, full it from memory
            title=file.split(" - ")  #this and the next two lines parse th name of the file to reduce its length in the tqdm progressbar
            try: part=re.findall(r"\[part (\d)\]",file)[0]
            except: part=1
            if args.pairs: #generate a dict of messages and their index in messages
                message_indexes={msgdata["id"]:loc for loc, msgdata in enumerate(file_data)}
            if args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}') #this just reduces the number of updates to the progressbar from every time to just before and after a file
            if re.findall(r"\[\d{18,}\]",file)[0] != last_id: #checks if we're on a new server, and if we are, reset the locality variables
                last_known_name=""
                last_known_time=0
                build=""
                last_id=re.findall(r"\[\d{18,}\]",file)[0]
            for curr_message in file_data: #loop through the messages
                if not curr_message["author"]["isBot"]: #ignore bots
                    today=time.mktime(datetime.strptime(curr_message["timestamp"].split(".")[0].replace("+00:00",""), "%Y-%m-%dT%H:%M:%S").timetuple()) # time is formatted in a specific way, we need to convert it to a unix timestamp
                    msg=clean(curr_message["content"]) #clean the message
                    if args.pairs:
                        try: #check if the message has a reply attached. if so, add it to the pairs
                            source=curr_message["reference"]["messageId"]
                            source_author=clean(file_data[message_indexes[source]]["author"]["name"], author=file_data[message_indexes[source]]["author"]["id"])
                            source_msg=clean(file_data[message_indexes[source]]["content"])
                            if source_msg != None and msg != None: #make sure the messages after being cleaned are not empty
                                p.write(f"{source_author}: {source_msg}\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}\n") #write files instead of storing them to save memory
                        except: pass
                    if msg != None:
                        if curr_message["author"]["name"] != last_known_name or build=="": #check if the author of the last message is also the author of this message
                            last_known_name=curr_message["author"]["name"]
                            build+=f"\t{clean(last_known_name,author=curr_message['author']['id'])}: {msg}" #if not, include the author's name
                        else:
                            build+="\\n"+msg # if so, add to the last message
                    else:disposed+=1
                    if today-last_known_time > args.conversation_timeout and last_known_time != 0: # if not the first message and 10 minutes have elapsed form the last message
                        build=re.sub(r"^[\t\\n]+","", build.replace("\n","\\n")) # remove leading \n and \t if there are any
                        if len(build.split("\t")) >= args.min_messages and build != "": #check if the number of messages in the conversation is >2
                            f.write(build+"\n") #write the conversation
                            if args.ascii: a.write(build.replace("\n","").encode("ascii", "ignore").decode()+"\n") #write the line but ascii-encoded
                            completed+=1
                        else: disposed+=1
                        build="" #reset the last known people
                        last_known_name=""
                    last_known_time=today#save the time of the curent message
                    if not args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
                    pbar.update(1) #really, really should optimize this
                else: disposed+=1
            if args.disable_description: pbar.set_description(f'{title[0]} - {title[1]} - Part {part}, Conversations: {completed} Removed: {disposed}')
    del all_messages
    if args.ascii: a.close() #close the files
    if args.pairs: p.close()

if args.step == "nontoxic" or args.nontoxic:
    from tox_block.prediction import make_predictions as detect       
    to_clean=io.open(os.path.join(args.out,f"{args.nontoxic_source}.txt"), mode="r", encoding="utf-8").read().strip().split("\n")
    if args.ascii: a=io.open(os.path.join(args.out,"context-ascii-detox.txt"), mode="w", encoding="utf-8")
    with io.open(os.path.join(args.out,"context-detox.txt"), mode="w", encoding="utf-8") as f:
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
                        if len(to_write) < args.min_messages: 
                            disposed+=len(to_write)
                        else:
                            to_write="\t".join(to_write)
                            f.write(to_write+"\n")
                            if args.ascii: a.write(to_write.replace("\n","").encode("ascii", "ignore").decode()+"\n")
                    pbar.set_description(f"From {args.nontoxic_source}.txt, Batch: {len(sents)}, Removed: {disposed_tox}")
                    batch=[]
    if args.ascii: a.close()

print(f"Removed {disposed}+{disposed_tox}/{len_all_messages}, {round((disposed+disposed_tox)/len_all_messages,2)}%")
final_file_path=os.path.join(args.out,f'{args.nontoxic_source}{"-detox" if args.nontoxic else ""}.txt')
print(f"Dataset final size: {len_all_messages - disposed - disposed_tox} messages, reduced from "+
      f"{sizeof_fmt(sum([os.path.getsize(f'{os.path.join(args.dir,fle)}') for fle in os.listdir(args.dir)]))} to "+
      f"{sizeof_fmt(os.path.getsize(os.path.join(args.out,f'{args.nontoxic_source}-detox.txt'))) if args.nontoxic else sizeof_fmt(os.path.getsize(final_file_path))}")