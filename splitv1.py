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
from pyinstrument import Profiler

parser = argparse.ArgumentParser(description='Split dataset in multiple files into train and validation sets')
parser.add_argument('-dir', nargs="+", default=["data"],
                    help='the data folder containing the processed files on the top level')
parser.add_argument('-out', type=str, default="context",
                    help='prefix the compressed output file sets')
parser.add_argument('-max_length', type=int, default=8,
                    help='maximum number of turns that the inputs amy have')
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(0,10)),
                    help='how compressed the file should be')
parser.add_argument('-workers', type=int, default=8,
                    help='number of workers to use (reccomended to be core count *2)')
parser.add_argument('-personality', type=str, default=None,
                    help='number of workers to use (reccomended to be core count *2)')
args = parser.parse_args()

try:os.mkdir("temp")
except: pass
lck=multiprocessing.Lock()
fst= {"train": True, "val": True}
if args.personality: 
    personalities=json.load(io.open(args.personality, "r", encoding="utf-8"))
    fst.update({"personality-train": True, "personality-val":True})

def writefile(data, split):
    lck.acquire()
    with gzip.open(f"{args.out}-{split}.txt.gz", "ab+", compresslevel=args.compression_level) as f:
        for line in data:
            if fst[split]: 
                f.write(line.encode("utf-8"))
                fst[split]=False
            else: f.write(f"\n{line}".encode("utf-8"))
    lck.release()

def worker(filename, split, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    temp, data, spl=[], json.load(io.open(filename, mode="r", encoding="utf-8")), split
    for conversation in data["conversations"]:
        spl=split
        per=not set([pair[1] for pair in conversation]).isdisjoint(list(personalities))
        if per: spl="personality-"+spl
        if len(conversation) >= 2:
            for y in range(1,len(conversation)):
                x=y-args.max_length if y-args.max_length >= 0 else 0
                out=(f"persona: {(random.choice(personalities[str(conversation[y][1])]) if str(conversation[y][1]) in personalities else 'None').replace('	',' ')} context: {('/b'.join([msg[0] for msg in conversation[x:y]])).replace('	',' ')}\t{(': '.join(conversation[y][0].split(': ')[1:])).replace('	',' ')}").strip().replace("\\n", "/n").replace("\n","")
                temp.append(out)
    writefile(temp, spl)
    if debug: profiler.stop(); print(profiler.output_text(unicode=True, color=True))
    return 0
    
if __name__ == '__main__':
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