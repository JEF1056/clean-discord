#NOTE: This can be made more efficient by running it with multiprocessing, but I need to solve the issue with writing to one file

import os
import io
import gzip
import random
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Split dataset in multiple files into train and validation sets')
parser.add_argument('-dir', nargs="+", default=["data"],
                    help='the data folder containing the processed files on the top level')
parser.add_argument('-out', type=str, default="context",
                    help='prefix the compressed output file sets')
parser.add_argument('-max_length', type=int, default=10,
                    help='maximum number of turns that the inputs amy have')
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(0,10)),
                    help='how compressed the file should be')
args = parser.parse_args()

global fst

def worker(files, split, w):
    global fst
    for filename in tqdm(files, desc=f"{split} split..."):
        with io.open(filename, mode="r", encoding="utf-8") as f:
            line = f.readline()
            while line:
                line=line.strip().replace("\\n", "/n").split("\t")
                for y in range(1,len(line)):
                    x=y-args.max_length if y-args.max_length >= 0 else 0
                    if args.compression_level != 0:
                        if fst: w.write(f"{'/b'.join(line[x:y])}\t{line[y]}".encode("utf-8")); fst=False
                        else: w.write(f"\n{'/b'.join(line[x:y])}\t{line[y]}".encode("utf-8"))
                    else:
                        if fst: w.write(f"{'/b'.join(line[x:y])}\t{line[y]}"); fst=False
                        else: w.write(f"\n{'/b'.join(line[x:y])}\t{line[y]}")
                line=f.readline()
    return "DONE"

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
            worker(train_files, "train", w)
        with gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level) as w:
            fst=True
            worker(eval_files, "val",w)
    else:
        with open(f"{args.out}-train.txt", "w") as w:
            fst=True
            worker(train_files, "train", w)
        with open(f"{args.out}-val.txt", "w") as w:
            fst=True
            worker(eval_files, "val",w)