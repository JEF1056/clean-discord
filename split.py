import os
import io
import gzip
import random
import argparse
from tqdm import tqdm
import concurrent.futures

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

global fst, w

def worker(filename):
    global fst, lock, w
    with io.open(filename, mode="r", encoding="utf-8") as f:
        line = f.readline()
        while line:
            line=line.strip().replace("\\n", "/n").split("\t")
            for y in range(1,len(line)):
                x=y-args.max_length if y-args.max_length >= 0 else 0
                if args.compression_level != 0:
                    if fst: w.write(f"{'/b'.join(line[x:y])}\t{line[y].split(': ')[1]}".encode("utf-8")); fst=False
                    else: w.write(f"\n{'/b'.join(line[x:y])}\t{line[y].split(': ')[1]}".encode("utf-8"))
                else:
                    if fst: w.write(f"{'/b'.join(line[x:y])}\t{line[y].split(': ')[1]}"); fst=False
                    else: w.write(f"\n{'/b'.join(line[x:y])}\t{line[y].split(': ')[1]}")
            line=f.readline()

if __name__ == '__main__':
    files=[]
    for dir in args.dir:
        ofiles=os.listdir(dir)
        try:ofiles.remove('stats.json')
        except:pass
        files.extend(os.path.join(dir, i) for i in ofiles)
    random.shuffle(files)
    cut_off = int(len(files) * .05)
    train_files, eval_files = files[:-cut_off], files[-cut_off:]
    print(f"Train size: {len(train_files)} files\tVal size: {len(eval_files)} files\nFiles will be {'compressed' if args.compression_level != 0 else 'uncompressed'}.")
    
    if args.compression_level != 0:
        with gzip.open(f"{args.out}-train.txt.gz", "wb", compresslevel=args.compression_level) as w:
            fst=True
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                ret=list(tqdm(executor.map(worker, train_files), total=len(train_files), desc="Writing train..."))
        with gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level) as w:
            fst=True
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                ret=list(tqdm(executor.map(worker, eval_files), total=len(eval_files), desc="Writing val..."))
    else:
        with open(f"{args.out}-train.txt", "w") as w:
            fst=True
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                ret=list(tqdm(executor.map(worker, train_files), total=len(train_files), desc="Writing train..."))
        with open(f"{args.out}-val.txt", "w") as w:
            fst=True
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                ret=list(tqdm(executor.map(worker, eval_files), total=len(eval_files), desc="Writing val..."))