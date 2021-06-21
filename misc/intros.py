import os
import io
import re
import sys
import json
import argparse
os.chdir('../')
sys.path.append('src')
from helpers import clean
import concurrent.futures
from validatefiles import check_files
from atpbar import atpbar, register_reporter, find_reporter

def str2bool(v):
    if isinstance(v, bool): return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'): return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'): return False
    else: raise argparse.ArgumentTypeError('Boolean value expected.')
    
def clear():
    if os.name == 'nt': _ = os.system('cls')
    else: _ = os.system('clear')

parser = argparse.ArgumentParser(description='Clean Discord intro data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output.json",
                    help='the folder to output txts')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
parser.add_argument("-skip-validation", type=str2bool, nargs='?', const=True, default=False, 
                    help="extract pairs from discord's replies system")
parser.add_argument("-overwrite", type=str2bool, nargs='?', const=True, default=False, 
                    help="overwrite existing files")
parser.add_argument("-lowmem", type=str2bool, nargs='?', const=True, default=False, 
                    help="use low-mem clenaing method")
args = parser.parse_args()

global reporter
reporter = find_reporter()
check_files(args.dir)

def worker(file):
    global reporter
    temp={}
    data=json.load(io.open(os.path.join(args.dir, file), "r"))["messages"]
    register_reporter(reporter)
    for row in atpbar(data, name=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", file).group(0)):
        if row['author']['isBot'] == False and row["type"] == "Default" and row["content"] and len(row["content"]) >= 80:
            cleaned=clean(row["content"])
            if cleaned:
                if str(row["author"]["id"]) not in temp:
                    temp[str(row["author"]["id"])]=[cleaned]
                else:
                    temp[str(row["author"]["id"])].append(cleaned)
    return temp

tasks=os.listdir(args.dir) 
with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
    ret=list(executor.map(worker, tasks))
    
master_output={}
for ret_dict in ret:
    for key in ret_dict:
        if key not in master_output:
            master_output[key]=ret_dict[key]
        else:
            master_output[key].extend(ret_dict[key])
        master_output[key]=list(set(master_output[key]))
        
print(f"{len(master_output)} people, {sum([len(master_output[key]) for key in master_output])} profiles")
            
json.dump(master_output, io.open(args.out, "w"))