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

import threading

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
parser.add_argument('-threads', type=int, default=16,
                    help='override the maximum number of threads to spawn')
parser.add_argument("-cache", type=str2bool, nargs='?', const=True, default=False,
                    help="turn on cache when reading files (uses a lot of memory)")
parser.add_argument('-step', type=str, default="clean", choices=["clean", "nontoxic"],
                    help="which step to start on (in case you've already cleaned the data)")
parser.add_argument("-pairs", type=str2bool, nargs='?', const=True, default=False,
                    help="takes into account discord's new replies feature to create a file of only sentence pairs (data yeilds will be low)")

parser.add_argument("-nontoxic", type=str, default=None, choices=["fast", "slow", None],
                    help="if none, don't remove profanity, if fast, use a wordlist, if slow, use an AI")
parser.add_argument("-censor", type=str, default="remove", choices=["remove", "censor", None],
                    help="censor sentences instead of removing them")
parser.add_argument('-nontoxic_source', type=str, default="context",
                    choices=["context", "context-pairs"],
                    help="clean base or pairs context")
parser.add_argument("-batches", type=int, default=100,
                    help="minimum number of batches to feed the AI (only needed if -nontoxic is used)")
parser.add_argument("-confidence", type=float, default=0.85,
                    help="AI must be > 0.85 sure that the message is toxic to remove it")

args = parser.parse_args()
if args.nontoxic == None or args.nontoxic == "slow": args.censor=None

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# these are primarily statistics and the sort
disposed_tox = 0
disposed = 0
completed = 0
len_all_messages = 1
all_data_files = sorted(os.listdir(args.dir))
if args.pairs: assert args.min_messages == 2

# this if statement encapsulates the code used to load the total number of messages into memory
# this is either by the means of reading each file backward and checking for the messagescount field or just brute-force caching them into memory
if (args.cache == False and args.step == "nontoxic") or args.step == "clean":
    print(check_files(args.dir))
    all_messages = {} if args.cache else 0
    with tqdm(all_data_files, desc="Reading files") as pbar:
        for file in pbar:
            if type(all_messages) == tuple:
                all_messages[file] = json.load(io.open(os.path.join(args.dir, file), mode="r", encoding="utf-8"))[
                    "messages"]
            else:
                with open(os.path.join(args.dir, file), 'rb') as f:
                    f.seek(-2, os.SEEK_END)
                    while f.read(1) != b' ':
                        f.seek(-2, os.SEEK_CUR)
                    last_line = f.readline().decode()
                    all_messages += int(int(last_line))
            pbar.set_description(
                f"Found {sum([len(all_messages[msgs]) for msgs in all_messages]) if type(all_messages) == tuple else all_messages} messages")

    try:
        os.mkdir(args.out)
    except FileExistsError:
        pass

len_all_messages = (
    sum([len(all_messages[msgs]) for msgs in all_messages])
    if type(all_messages) == tuple
    else all_messages
)  # this is to determine the length of tqdm's progress bar

def clean_worker(file_data, outFunc_Primary, outFunc_Pairs):
    global disposed, completed, pbar

    if args.pairs:  # generate a dict of messages and their index in messages
        message_indexes = {msgdata["id"]: loc for loc, msgdata in enumerate(file_data)}

    last_known_name = ""
    last_known_time = 0
    build = ""
    pairs=[]
    msgs=[]
    
    for curr_message in file_data:  # loop through the messages

        if not curr_message["author"]["isBot"]:  # ignore bots

            # time is formatted in a specific way, we need to convert it to a unix timestamp
            today = time.mktime(
                datetime.strptime(
                    curr_message["timestamp"]
                        .split(".")[0]
                        .replace("+00:00", ""),
                    "%Y-%m-%dT%H:%M:%S",
                ).timetuple()
            )

            msg = clean(curr_message["content"])  # clean the message

            if args.pairs:

                # check if the message has a reply attached. if so, add it to the pairs
                try:
                    source = curr_message["reference"]["messageId"]

                    source_author = clean(
                        file_data[message_indexes[source]]["author"]["name"],
                        author=file_data[message_indexes[source]]["author"]["id"],
                    )

                    source_msg = clean(file_data[message_indexes[source]]["content"])

                    # make sure the messages after being cleaned are not empty
                    if source_msg is not None and msg is not None:
                        # write files instead of storing them to save memory
                        pairs.append(f"{source_author}: {source_msg}\t{clean(last_known_name, author=curr_message['author']['id'])}: {msg}\n")

                except Exception:
                    pass

            if msg is not None:

                # check if the author of the last message is also the author of this message
                if curr_message["author"]["name"] != last_known_name or build == "":
                    last_known_name = curr_message["author"]["name"]
                    # if not, include the author's name
                    build += f"\t{clean(last_known_name, author=curr_message['author']['id'])}: {msg}"
                else:
                    build += "\\n" + msg  # if so, add to the last message
            else:
                disposed += 1

            # if not the first message and 10 minutes have elapsed form the last message
            if today - last_known_time > args.conversation_timeout and last_known_time != 0:

                # remove leading \n and \t if there are any
                build = re.sub(r"^[\t\\n]+", "", build.replace("\n", "\\n"))

                # check if the number of messages in the conversation is >2
                if len(build.split("\t")) >= args.min_messages and build != "":
                    msgs.append(build + "\n")  # write the conversation
                    completed += 1
                else:
                    disposed += 1

                build = ""  # reset the last known people
                last_known_name = ""
            pbar.update(1)
            last_known_time = today  # save the time of the current message

        else:
            disposed += 1
    del file_data
    for i in msgs:  outFunc_Primary(i)
    for i in pairs: outFunc_Pairs(i)
    del msgs
    del pairs


if args.step == "clean":
    if args.pairs:
        p = io.open(
            os.path.join(args.out, "context-pairs.txt"), mode="w", encoding="utf-8"
        )  # i don't think that you need to multithread these, unless there is a way to do that

    with io.open(
            os.path.join(args.out, "context.txt"), mode="w", encoding="utf-8"
    ) as f:  # initializes tqdm and the primary file to write to
        t1=time.time()
        file_lock = threading.Lock()

        def outputFunc_Primary(dat):
            file_lock.acquire()
            f.write(dat)
            file_lock.release()
            
        if args.pairs:
            def outputFunc_Pairs(dat):
                file_lock.acquire()
                p.write(dat)
                file_lock.release()
        else:outputFunc_Pairs=None

        threads = []
        pbar=tqdm(total=len_all_messages, desc="Processing files")

        for file in all_data_files:  # loop through each file containing messages
            
            if len(threads) == args.threads:
                pbar.set_description(f"Thread cap reached, waiting ...")
                x = threads.pop(0)
                x.join()

            #print("Starting", all_data_files[i])
            pbar.set_description(f"Starting {file}")

            file_data = (
                all_messages[file]
                if type(all_messages) == tuple
                else json.load(
                    io.open(os.path.join(args.dir, file), mode="r", encoding="utf-8")
                )["messages"]
            )  # load the file or if cached, full it from memory
            
            th = threading.Thread(target=clean_worker, args=(file_data, outputFunc_Primary, outputFunc_Pairs))
            th.start()
            threads.append(th)

        print("\nNo more threads left to start")

        for i, th in enumerate(threads):
            print(f"Joining thread {i+1}/{len(threads)}, waiting for end...", end=" ")
            th.join()
            print(f"Done in {round(time.time()-t1, 2)} seconds")

    del all_messages

    if args.pairs:
        p.close()  # close the files


if args.step == "nontoxic" or args.nontoxic != None:
    if args.step == "nontoxic": assert args.nontoxic != None
    to_clean = io.open(os.path.join(args.out, f"{args.nontoxic_source}.txt"), mode="r",
                        encoding="utf-8").read().strip().split("\n")
    with io.open(os.path.join(args.out, "context-detox.txt"), mode="w", encoding="utf-8") as f:
        with tqdm(to_clean, desc=f"Processing messages {args.nontoxic}ly") as pbar:
            if args.nontoxic == "fast":
                from better_profanity import profanity
                badwords=io.open("src/badwords.txt", mode="r", encoding="utf-8").read().strip().split("\n")
                profanity.load_censor_words(badwords)
                for conversation in pbar:
                    conversation=list(filter(None, conversation.strip().split("\t")))
                    to_write=[]
                    for conv in conversation:
                        if args.censor == "remove" and profanity.contains_profanity(conv): 
                            pass
                        elif args.censor == "censor": 
                            tc=conv.split(': ')
                            to_write.append(f"{tc[0]}: {profanity.censor(tc[1])}")                            
                        else: to_write.append(conv)   
                    pbar.set_description(f"From {args.nontoxic_source}.txt, Batch: {len(sents)}, Removed: {disposed_tox}")   
                    if len(to_write) < args.min_messages: 
                        disposed+=len(to_write)
                    else:
                        to_write="\t".join(to_write)
                        f.write(to_write+"\n") 
            elif args.nontoxic == "slow":
                from tox_block.prediction import make_predictions as detect
                batch = []
                for curr_index, conversation in enumerate(pbar):
                    batch.append(conversation)
                    if curr_index == len(to_clean) - 1 or sum(
                            [len(msgs.strip().split("\t")) for msgs in batch]) >= args.batches:
                        batch_placement, sents = [0], []
                        for conv in batch:
                            splt= list(filter(None, conv.strip().split("\t")))
                            sents.extend(list(filter(None, [remove.replace("\\n","\n").strip() for remove in splt]))) #not sure currently if the tox-block model is affected by "\\n", experiment?
                            batch_placement.append(len(splt))
                        prediction_vals=detect(sents)
                        scores=[max(list(dict(prediction_vals[detection]).values())[1:]) for detection in prediction_vals]
                        offsets=[sum(batch_placement[0:i]) for i in range(1,len(batch_placement))]
                        for ind, batch_score in enumerate([scores[sum(batch_placement[0:i]):sum(batch_placement[0:i])+batch_placement[i]] for i in range(1,len(batch_placement))]):
                            to_write=[]
                            for i,v in enumerate(batch_score):
                                if v <= args.confidence: to_write.append(sents[offsets[ind]+i].replace("\n","\\n"))
                                else: disposed_tox+=1
                            if to_write != []:
                                if to_write[0].startswith("\\n"): to_write=to_write[1:]
                                if len(to_write) < args.min_messages: 
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