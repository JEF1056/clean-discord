import os
import io
import gzip
import random
import argparse
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
parser.add_argument('-workers', type=int, default=4,
                    help='number of workers to use (reccomended to be core count *2)')
args = parser.parse_args()

try:os.mkdir("temp")
except: pass

def worker(filename, split, id):
    fst=True
    if args.compression_level != 0: w=gzip.open(os.path.join("temp", f"{split}-{id}.temp.gz"), "wb", compresslevel=args.compression_level)
    else: w=io.open(os.path.join("temp", f"{split}-{id}.temp"), mode="w", encoding="utf-8")
    with io.open(filename, mode="r", encoding="utf-8") as f:
        line = f.readline()
        while line:
            line=line.strip().replace("\\n", "/n").split("\t")
            for y in range(1,len(line)):
                x=y-args.max_length if y-args.max_length >= 0 else 0
                try:
                    out=f"{'/b'.join(line[x:y])}\t{line[y].split(': ')[1]}"
                    if args.compression_level != 0: out=out.encode("utf-8")
                    if fst: w.write(out); fst=False
                    else: w.write(("\n").encode("utf-8")+out) if args.compression_level != 0 else w.write("\n"+out)
                except: pass
            line=f.readline()
    w.close()

if __name__ == '__main__':
    files=[]
    for dir in args.dir:
        ofiles=[os.path.join(dir, file) for file in os.listdir(dir) if file != "stats.json" and file.endswith(".txt")]
        files.extend(ofiles)
    random.shuffle(files)
    cut_off = int(len(files) * .05)
    train_files, eval_files = files[:-cut_off], files[-cut_off:]
    print(f"Train size: {len(train_files)} files\tVal size: {len(eval_files)} files\nFiles will be {'compressed' if args.compression_level != 0 else 'uncompressed'}.")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker, train_files, repeat("train"), range(0, len(train_files))), total=len(train_files), desc="Writing train..."))
    #with gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level) as w:
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker, eval_files, repeat("eval"), range(0, len(eval_files))), total=len(eval_files), desc="Writing val..."))
    
    if args.compression_level != 0: 
        t=gzip.open(f"{args.out}-train.txt.gz", "wb", compresslevel=args.compression_level)
        v=gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level)
    else: 
        t=io.open(f"{args.out}-train.txt", mode="w", encoding="utf-8")
        v=io.open(f"{args.out}-val.txt", mode="w", encoding="utf-8")
    fst=True
    for file in tqdm(os.listdir("temp"), desc="Merging files..."):
        if args.compression_level != 0: f=gzip.open(os.path.join("temp", file),'rb')
        else: f=io.open(os.path.join("temp", file), mode='r', encoding="utf-8")
        file_content=f.read()
        if not fst: file_content="\n"+file_content
        if file.startswith("train"):
            t.write(file_content)
        elif file.startswith("eval"):
            v.write(file_content)
        f.close()
    t.close(); v.close()
        