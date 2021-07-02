import os
import io
import json
import gzip
import random
import argparse
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
                    help='file containing personality data')
args = parser.parse_args()

try:os.mkdir("temp")
except: pass
if args.personality: personalities=json.load(io.open(args.personality, "r", encoding="utf-8"))

def writefile(data, pref, split, num):
    fst=False
    with gzip.open(os.path.join(args.out, f"{pref}-{split}-{num}.txt.gz"), "w", compresslevel=args.compression_level) as f:
        for line in data:
            if fst:
                f.write(f"{line}\n".encode("utf-8"))
                fst=False
            else: f.write(f"\n{line}".encode("utf-8"))

def get_perms(conversation):
    temp=[]
    for y in range(1,len(conversation)):
        max_back=y-args.max_length if y-args.max_length >= 0 else 0
        sample=random.sample(range(max_back+1, y), y-max_back-1 if y-max_back-1 <=5 else 5)+[max_back]
        for x in sample: 
            psn=(random.choice(personalities[str(conversation[y][1])]) if str(conversation[y][1]) in personalities else 'None').replace('\t',' ')
            ctx=('/b'.join([msg[0] for msg in conversation[x:y]])).replace('\t',' ')
            rsp=(': '.join(conversation[y][0].split(': ')[1:])).replace('\t',' ')
            temp.append(f"persona: {psn} context: {ctx}\t{rsp}".strip().replace("\\n", "/n").replace("\n","/n"))
    return temp
    
def worker(filename, split, num, debug=False):
    if debug: profiler = Profiler(); profiler.start()
    temp, data=[], json.load(io.open(filename, "r", encoding="utf-8"))
    for conversation in data["conversations"]:
        per=not set([pair[1] for pair in conversation]).isdisjoint(list(personalities))
        if per: pref="persona"
        else: pref="context"
        if len(conversation) >= 2:
            temp.extend(get_perms(conversation))
    writefile(temp, pref, split, num)
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
        ret=list(tqdm(executor.map(worker, train_files, repeat("train"), range(len(train_files))), total=len(train_files), desc="Writing train..."))
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker, eval_files, repeat("val"), range(len(train_files))), total=len(eval_files), desc="Writing val..."))