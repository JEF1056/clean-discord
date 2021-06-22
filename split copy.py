import os
import io
import json
import gzip
import random
import argparse
import multiprocessing
from tqdm import tqdm
import concurrent.futures
from itertools import repeat

parser = argparse.ArgumentParser(description='Split dataset in multiple files into train and validation sets')
parser.add_argument('-dir', nargs="+", default=["data"],
                    help='the data folder containing the processed files on the top level')
parser.add_argument('-out', type=str, default="context",
                    help='prefix the compressed output file sets')
parser.add_argument('-max_length', type=int, default=10,
                    help='maximum number of turns that the inputs amy have')
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(0,10)),
                    help='how compressed the file should be')
parser.add_argument('-workers', type=int, default=8,
                    help='number of workers to use (reccomended to be core count *2)')
parser.add_argument('-personality', type=str, default=None,
                    help='number of workers to use (reccomended to be core count *2)')
parser.add_argument('-step', type=str, nargs="+", default="all",
                    help='step')
args = parser.parse_args()

try:os.mkdir("temp")
except: pass
personalities=json.load(io.open(args.personality, "r", encoding="utf-8"))
lock, fst= multiprocessing.Lock(), {"train": True, "val": True}
if args.personality: fst.update({"personality-train": True, "personality-val":True})

def writefile(data, split):
    lock.acquire()
    with gzip.open(f"{args.out}-{split}.txt.gz", "wb", compresslevel=args.compression_level) as f:
        for line in data:
            if fst[split]: f.write(line); fst[split]=False
            else: f.write(f"\n{line}")
    lock.release()

def worker(filename, split):
    temp, data=[], json.load(io.open(filename, mode="r", encoding="utf-8"))
    for conversation in data["conversations"]:
        if set([pair[1] for pair in conversation]) & set(list(personalities)): split="personality-"+split
        if len(conversation) >= 2:
            for y in range(1,len(conversation)):
                x=y-args.max_length if y-args.max_length >= 0 else 0
                out=f"persona: {random.choice(conversation)[0] if set([pair[1] for pair in conversation]) & set(list(personalities)) else 'None'} context: {'/b'.join([msg[0] for msg in conversation[x:y]])}\t{': '.join(conversation[y][0].split(': ')[1:])}".strip().replace("\\n", "/n")
                temp.append(out)
        else: print(conversation)
            
    writefile(temp)
    
if __name__ == '__main__':
    for step in args.step:
        files=[]
        for dir in args.dir:
            ofiles=[os.path.join(dir, file) for file in os.listdir(dir) if file.endswith(".json")]
            files.extend(ofiles)
        random.shuffle(files)
        cut_off = int(len(files) * .05)
        train_files, eval_files = files[:-cut_off], files[-cut_off:]
        print(f"Train size: {len(train_files)} files\tVal size: {len(eval_files)} files")

        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
            ret=list(tqdm(executor.map(worker, train_files, repeat("train")), total=len(train_files), desc="Writing train..."))
        #with gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level) as w:
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
            ret=list(tqdm(executor.map(worker, eval_files, repeat("val")), total=len(eval_files), desc="Writing val..."))