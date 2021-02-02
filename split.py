import io
import os
from tqdm import tqdm
import argparse
import numpy as np
from src.helpers import str2bool as s2b

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing preprocessed data to merge and split')
parser.add_argument('-out', type=str, default="context",
                    help='prefix for the files to be written')
parser.add_argument('-div_symbol', type=str, default="/b",
                    help='prefix for the files to be written')
parser.add_argument('-newline_symbol', type=str, default="/n",
                    help='prefix for the files to be written')
parser.add_argument('-split', type=float, default=0.95,
                    help='split% for training data')
parser.add_argument('-chunks', type=int, default=20,
                    help='max length of the dataset')
parser.add_argument('-max_len', type=int, default=2048,
                    help='max length of the dataset')
parser.add_argument("-ascii", type=s2b, nargs='?', const=True, default=False,
                    help="debugging flag")
parser.add_argument("-shuffle", type=s2b, nargs='?', const=True, default=False,
                    help="debugging flag")
args = parser.parse_args()

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield "\t".join(lst[i:i + n])

data=[]
for file in os.listdir(args.dir):
    for convo in tqdm(io.open(os.path.join(args.dir,file), mode="r", encoding="utf-8").read().strip().split("\n"), desc=f"Chunking {file}"):
        dta=list(chunks(convo.split("\t"), args.chunks+1))
        data.extend(dta)
data=list(filter(None, data))

with io.open(os.path.join(f"{args.out}-train.txt"), mode="w", encoding="utf-8") as t,  io.open(os.path.join(f"{args.out}-val.txt"), mode="w", encoding="utf-8") as v:
    dist=np.random.choice(["t","v"], size=len(data), p=[args.split,1-args.split])
    train=[e for i,e in enumerate(data) if dist[i]=="t"]
    val=[e for i,e in enumerate(data) if dist[i]=="v"]
    del data
    del dist
    if args.shuffle:
        np.random.shuffle(train)
        np.random.shuffle(val)
    for line in tqdm(train, desc="Cleaning lines for train split"): 
        bld=line.split("\t")[0]
        for dta in line.split("\t")[1:]:
            try:
                ln=bld+"\t"+dta.split(": ")[1]+"\n"
                if args.ascii: ln=ln.encode("ascii", "ignore").decode()
                t.write(ln.replace("\\n",args.newline_symbol))
                bld+=args.div_symbol+dta
                if len(bld.replace(args.div_symbol, " ")) >= int(args.max_len*1.5):break
            except: pass

    for line in tqdm(val, desc="Cleaning lines for val split"): 
        bld=line.split("\t")[0]
        for dta in line.split("\t")[1:]:
            try:
                ln=bld+"\t"+dta.split(": ")[1]+"\n"
                if args.ascii: ln=ln.encode("ascii", "ignore").decode()
                v.write(ln.replace("\\n",args.newline_symbol))
                bld+=args.div_symbol+dta
                if len(bld.replace(args.div_symbol, " ")) >= int(args.max_len*1.5):break
            except: pass