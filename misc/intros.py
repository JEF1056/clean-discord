import os
import io
import re
import sys
import json
import gzip
import random
import argparse
import itertools
os.chdir('../')
sys.path.append('src')
from helpers import clean
import concurrent.futures
from validatefiles import check_files
from atpbar import atpbar, register_reporter, find_reporter

def str2bool(v):
    if isinstance(v, bool): return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'): return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'): return False
    else: raise argparse.ArgumentTypeError('Boolean value expected.')
    
def clear():
    if os.name == 'nt': _ = os.system('cls')
    else: _ = os.system('clear')

parser = argparse.ArgumentParser(description='Clean Discord intro data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output",
                    help='the folder to output txts')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
parser.add_argument("-skip-validation", type=str2bool, nargs='?', const=True, default=False, 
                    help="extract pairs from discord's replies system")
parser.add_argument("-overwrite", type=str2bool, nargs='?', const=True, default=False, 
                    help="overwrite existing files")
parser.add_argument("-lowmem", type=str2bool, nargs='?', const=True, default=False, 
                    help="use low-mem clenaing method")
args = parser.parse_args()

global reporter
reporter = find_reporter()
check_files(args.dir)
try: os.mkdir(args.out)
except: pass

r10=re.compile(r"^(?!my|\bdm\b|server|location|located)[a-z]+\s??(?!my|\bdm\b|server|location|located)[a-z]+\s??:[a-z0-9/.,\'\" ]{1,40}$", flags=re.M | re.IGNORECASE)
def gen_permuatations(name, info, shuffle=False):
    carrier=["who is", "who's", "that's", "that is"]
    outputs=[]
    all_combs=[]
    try:  del info["name"]
    except: pass
    [all_combs.extend(list(itertools.combinations(info, r))) for r in range(1,len(info)+1) if r <= 4]
    #name: [LITERAL JSON]
    #name: [LITERAL JSON WITHOUT BRACKETS]
    #name [LITERAL JSON]
    #name [LITERAL JSON WITHOUT BRACKETS]
    #[name][:/no :] ([x], [y], [z]...) (in vague/random order)
    #[name] [carrier] [x] [delimiter] [y] [delimiter] [z]... (in vague/random order)
    for comb in all_combs:
        get_json=[f"{combd}: {info[combd]}" for combd in comb]
        outputs.extend([name+": {"+", ".join(get_json)+"}",
                        f"{name}, {', '.join(get_json)}",
                        f"{name}: {', '.join(get_json)}",
                        f"{name}: [{', '.join(get_json)}]",
                        f"{name}: ({', '.join(get_json)})",
                        f"{name} ({', '.join(get_json)})",
                        f"{name}: ({', '.join([i.split(': ')[1].strip() for i in get_json])})",
                        f"{name} ({', '.join([i.split(': ')[1].strip() for i in get_json])})"
                        ]+
                        [f"{name} {carry} {' and '.join([i.split(': ')[1].strip() for i in get_json])}" for carry in carrier]+
                        [f"{name} {carry} {', '.join([i.split(': ')[1].strip() for i in get_json])}" for carry in carrier]
                        )
    random.shuffle(outputs)
    return outputs[:100]


def worker(file):
    global reporter
    temp, fst={}, True
    data=json.load(io.open(os.path.join(args.dir, file), "r"))["messages"]
    ch=re.search(r"\[\d{18}\](?:\s\[part \d{1,3}\])*", file).group(0)
    register_reporter(reporter)
    with gzip.open(os.path.join(args.out,ch+".txt.gz"), mode='wb', compresslevel=9) as f:
        for row in atpbar(data, name=ch):
            if row['author']['isBot'] == False and row["type"] == "Default" and row["content"] and len(row["content"]) >= 80:
                cleaned=clean(row["content"])
                if cleaned:
                    cleaned=cleaned.replace("\t"," ")
                    if str(row["author"]["id"]) not in temp:
                        temp[str(row["author"]["id"])]=[cleaned]
                    else:
                        temp[str(row["author"]["id"])].append(cleaned)
                    
                    extracted={e[0].lower():e[1] for e in [[v.strip() for v in i.split(":")] for i in re.findall(r10, cleaned.replace("\\n", "\n"))]}
                    if extracted == None: return []
                    output=[]
                    output.extend(gen_permuatations(row['author']['name'], extracted))
                    if "name" in extracted: output.extend(gen_permuatations(clean(extracted["name"]), extracted))
                    for line in [f"{inp}\t{cleaned}".replace("\n","\\n") for inp in output]:
                        if fst: f.write(line.encode("utf-8")); fst=False
                        else: f.write(f"\n{line}".encode("utf-8"))
    return temp

tasks=os.listdir(args.dir) 
with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
    ret=list(executor.map(worker, tasks))
    
master_output={}
for ret_dict in ret:
    for key in ret_dict:
        if key not in master_output:
            master_output[key]=ret_dict[key]
        else:
            master_output[key].extend(ret_dict[key])
        master_output[key]=list(set(master_output[key]))
        
for key in [key for key in master_output if len(master_output[key]) > 10]:
    del(master_output[key])
        
print(f"{len(master_output)} people, {sum([len(master_output[key]) for key in master_output])} profiles")
            
json.dump(master_output, io.open(args.out+".json", "w"))