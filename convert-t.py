import os
import re
import json
import argparse
from tqdm import tqdm
import concurrent.futures
from itertools import repeat
from src.workers import worker
from src.validate import check_files

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output",
                    help='the folder to output txts')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
args = parser.parse_args()

try:os.mkdir(args.out)
except: pass

#use \[\d{18}\](?:\s\[part \d{1,3}\])* instead
tasks=[m for m in os.listdir(args.dir) if re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", m).group(0)+".txt" not in os.listdir(args.out)]

def start_work():
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker, tasks, repeat(args.dir), repeat(args.out)), total=len(tasks)))
    messages_total, conversations_total, new_ret=0,0, {}
    for val in ret: 
        messages_total+=val["messages"]
        conversations_total+=val["conversations"]
        try:
            new_ret[val["channel"]]={"messages": new_ret[val["channel"]]["messages"]+val["messages"], "conversations": new_ret[val["channel"]]["conversations"]+val["conversations"]}
        except:
            new_ret[val["channel"]]={"messages": val["messages"], "conversations": val["conversations"]}
    json.dump({"messages_total":messages_total,"conversations_total":conversations_total, "num_files":len(ret), "num_channels":len(new_ret), "individual":ret, "merged":new_ret}, open(os.path.join(args.out,"stats.json"),"w"))
        
if __name__ == '__main__':
    check_files(args.dir)
    start_work()