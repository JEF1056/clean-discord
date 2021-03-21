import os
import re
import json
import argparse
from tqdm import tqdm
import concurrent.futures
from itertools import repeat
from src.validate import check_files
from src.workers import worker_regex, worker_detox, write_stats

def str2bool(v):
    if isinstance(v, bool): return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'): return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'): return False
    else: raise argparse.ArgumentTypeError('Boolean value expected.')

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output",
                    help='the folder to output txts')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
parser.add_argument('-step', type=str, default="all",
                    help='the step to start cleaning from')
parser.add_argument("-detox", type=str2bool, nargs='?', const=True, default=False, 
                    help="use detoxify to remove toxic messages")
parser.add_argument("-pairs", type=str2bool, nargs='?', const=True, default=False, 
                    help="extract pairs from discord's replies system")
parser.add_argument("-device", type=str, default="cuda", choices=["cpu", "cuda"],
                    help="device detoxify should use")
parser.add_argument("-overwrite", type=str2bool, nargs='?', const=True, default=False, 
                    help="overwrite existing files")
args = parser.parse_args()

def run_regex():
    #precompute tasks and create required dir
    try:os.mkdir(args.out)
    except: pass
    print("(regex): ", end="")
    if args.overwrite: tasks=os.listdir(args.dir); print(f"Overwriting data in {args.out}")
    else: tasks=[m for m in os.listdir(args.dir) if re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", m).group(0)+".txt" not in os.listdir(args.out)]; print(f"Found {len(os.listdir(args.dir))-len(tasks)} files in {args.out}, skipping." if len(os.listdir(args.dir))-len(tasks) != 0 else f"Writing data to {args.out}")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker_regex, tasks, repeat(args.dir), repeat(args.out)), total=len(tasks), desc="Regex cleaning..."))
    if ret != []: write_stats(ret, args.out)
        
def run_detox(to_clean):
    #precompute tasks and create required dir
    try:os.mkdir(to_clean+"-detox")
    except: pass
    print("(detox): ", end="")
    all_to_clean=[f for f in os.listdir(to_clean) if f.endswith(".txt")]
    if args.overwrite: tasks=to_clean; print(f"Overwriting data in {to_clean}")
    else: tasks=[m for m in all_to_clean if m not in os.listdir(to_clean+"-detox")]; print(f"Found {len(all_to_clean)-len(tasks)} files in {to_clean}, skipping." if len(all_to_clean)-len(tasks) != 0 else f"Writing data to {to_clean}-detox")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker_detox, tasks, repeat(to_clean), repeat(to_clean+"-detox")), total=len(tasks), desc="Detoxifying..."))
    if ret != []: write_stats(ret, to_clean+"-detox")
        
if __name__ == '__main__':
    check_files(args.dir)
    if args.step == "all":
        steps=["regex", "pairs", "detox"]
    else:
        try:
            steps=json.loads(args.step)
            assert type(steps)==list
        except: raise Exception("Unable to load steps json.")
    for step in steps:
        if step == "regex":
            run_regex()
        elif step == "pairs":
            if args.pairs:  pass #not implemented
        elif step == "detox":
            if args.detox: run_detox(args.out)
    print("DONE")