import os
import io
import zlib
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Split dataset in multiple files into train and validation sets')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing the processed files on the top level')
parser.add_argument('-out', type=str, default="context",
                    help='prefix the compressed output file sets')
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(-1,10)),
                    help='how compressed the file should be')
args = parser.parse_args()

def worker(files, split, max_length=10):
    for filename in tqdm(files, desc=f"{split} split..."):
        with io.open(os.path.join(args.dir, filename), mode="r", encoding="utf-8") as f:
            line = f.readline()
            while line:
                line=line.split()
                with open(f"{args.out}-{split}.txt", "wb") as w:
                    for y in range(1,len(line)):
                        x=y-max_length if y-max_length >= 0 else 0
                        w.write(zlib.compress(f"{'/b'.join(line[x:y])}\t{line[y]}"))
                line=f.readline()
    return "DONE"

if __name__ == '__main__':
    files=os.listdir(args.dir)
    files.sort()
    files.remove('stats.json')
    cut_off = int(len(files) * .05)
    train_files, eval_files = files[:-cut_off], files[-cut_off:]
    
    worker(train_files, "train")
    worker(eval_files, "val")