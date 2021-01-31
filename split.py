import io
import os
from tqdm import tqdm
import argparse
import numpy as np

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing preprocessed data to merge and split')
parser.add_argument('-out', type=str, default="context",
                    help='prefix for the files to be written')
parser.add_argument('-split', type=float, default=0.95,
                    help='split% for training data')
parser.add_argument('-max_len', type=int, default=2038,
                    help='max length of the dataset')
args = parser.parse_args()

data=[]
for file in os.listdir(args.dir):
    data.extend(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8").read().strip().split("\n"))
data=list(filter(None, data))

with io.open(os.path.join(f"{args.out}-train.txt"), mode="w", encoding="utf-8") as t,  io.open(os.path.join(f"{args.out}-val.txt"), mode="w", encoding="utf-8") as v:
    dist=np.random.choice(["t","v"], size=len(data), p=[args.split,1-args.split])
    train=[e for i,e in enumerate(data) if dist[i]=="t"]
    val=[e for i,e in enumerate(data) if dist[i]=="v"]
    del data
    del dist
    for line in tqdm(train): 
        bld=line.split("\t")[0]
        for dta in line.split("\t")[1:]:
            t.write(bld+"\t"+dta+"\n")
            bld+="\b"+dta
            if len(bld.replace("\\b", " ")) >= int(args.max_len*1.5):break

    for line in tqdm(val): 
        bld=line.split("\t")[0]
        for dta in line.split("\t")[1:]:
            v.write(bld+"\t"+dta+"\n")
            bld+="\b"+dta
            if len(bld.replace("\\b", " ")) >= int(args.max_len*1.5):break