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
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(0,10)),
                    help='how compressed the file should be')
args = parser.parse_args()

def worker(files, split, w, max_length=10):
    for filename in tqdm(files, desc=f"{split} split..."):
        with io.open(filename, mode="r", encoding="utf-8") as f:
            line = f.readline()
            while line:
                line=line.split("\t")
                for y in range(1,len(line)):
                    x=y-max_length if y-max_length >= 0 else 0
                    w.write(f"{'/b'.join(line[x:y])}\t{line[y]}\n".encode("utf-8"))
                line=f.readline()
    return "DONE"

if __name__ == '__main__':
    files=[]
    for dir in args.dir:
        files.extend(os.path.join(dir, i) for i in os.listdir(dir))
    random.shuffle(files)
    files.remove('stats.json')
    cut_off = int(len(files) * .05)
    train_files, eval_files = files[:-cut_off], files[-cut_off:]
    print(f"Train size: {len(train_files)} files\tVal size: {len(eval_files)} filesss")
    
    with gzip.open(f"{args.out}-train.txt.gz", "wb", compresslevel=args.compression_level) as w:
        worker(train_files, "train", w)
    with gzip.open(f"{args.out}-val.txt.gz", "wb", compresslevel=args.compression_level) as w:
        worker(eval_files, "val",w)