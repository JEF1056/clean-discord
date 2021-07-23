import os
import re
import io
import sys
import gzip
os.chdir('../')
from tqdm import tqdm
sys.path.append('src')
import concurrent.futures
from helpers import clean as cl
from atpbar import atpbar, register_reporter, find_reporter

source_path="/home/jfan/data/writingPrompts"
num_splits=3000

global reporter
reporter = find_reporter()
r0=re.compile(r'\[\s([^\[\]\(\)]+)\s\]\s?\(.*?\)\s*')
r1=re.compile(r'\s*\[\s[A-z]{2}\s\]\s*|(``)\s*|(\'\')\s*|\s*([.,?!;:*/\(\)\'])|\s*#')
r2=re.compile(r'(?:\s*<newline>\s*)+')

def clean(text):
    text=re.sub(r0, r"\1", text.strip())
    text=re.sub(r1, r"\1\2\3", text.strip())
    text=re.sub(r2, r"/n", text.strip())
    text=text.replace("''", '"').replace("``", '"')
    cl(text)
    return text.strip().replace("\n","/n")

def worker(inp):
    split, source, target=inp
    ret=[]
    global reporter
    register_reporter(reporter)
    for i in atpbar(range(len(source)), name=split):
        s=clean(source[i])
        t=clean(target[i])
        if not (s.strip() in ["", "\\n", "\n", " ", "\t"]) and not (t.strip() in ["", "\\n", "\n", " ", "\t"]): 
            ret.append(f"{s}\t{t}")
    return split, ret
                                                                       
print(os.listdir(source_path))
tasks=[]
for file in tqdm(os.listdir(source_path)):
    if file.endswith(".wp_source"):
        source_data=io.open(os.path.join(source_path, file), "r").read().split("\n")
        source_data=[source_data[i:i + num_splits] for i in range(0, len(source_data), num_splits)]
        target_data=io.open(os.path.join(source_path,file.replace(".wp_source", ".wp_target")), "r").read().split("\n")
        target_data=[target_data[i:i + num_splits] for i in range(0, len(target_data), num_splits)]
        for i in range(len(source_data)):
            tasks.append((file.replace(".wp_source",""), source_data[i], target_data[i]))

with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
    ret=list(executor.map(worker, tasks))

with gzip.open("story-train-1.txt.gz", "w") as t, gzip.open("story-val-1.txt.gz","w") as v:
    for part in tqdm(ret):
        if part[0] in ["train", "test"]:
            for line in part[1]:
                t.write((line+"\n").encode())
        else:
            for line in part[1]:
                v.write((line+"\n").encode())