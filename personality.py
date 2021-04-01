import io
from tqdm import tqdm
import random
import gzip

examples_pairs=io.open("src/commonquestions.txt", mode="r", encoding="utf-8").read().strip().split("\n")
examples_convos=io.open("src/commonconvos.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")

max_len=10
compression_level=9
output_file="personality"
partitions= 2000

def gen_all(line):
    line=line.strip().replace("\\n", "/n").split("\t")
    ret=[]
    for y in range(1,len(line)):
        x=y-max_len if y-max_len >= 0 else 0
        try:
            ret.append(f"{'/b'.join(line[x:y])}\t{': '.join(line[y].split(': ')[1:])}")
        except: pass
    return ret

out=[]
val=[]

print(gen_all(examples_convos[10].replace("[USER]", "Jake").replace("[BOT]", "Jade")))

for example in tqdm(examples_pairs, "Generating Pairs..."):
    random.shuffle(names)
    for name in names[:5000]:
        out.append(example.replace("[USER]", name).replace("[BOT]", "Jade"))
    for name in names[:250]:
        val.append(example.replace("[USER]", name).replace("[BOT]", "Jade"))

for example in tqdm(examples_convos, "Generating Convos..."):
    random.shuffle(names)
    for name in names[:2000]:
        out.extend(gen_all(example.replace("[USER]", name).replace("[BOT]", "Jade")))
    for name in names[:100]:
        val.extend(gen_all(example.replace("[USER]", name).replace("[BOT]", "Jade")))
    
random.shuffle(out)
random.shuffle(val)
print(len(out))
print(len(val))

pbar=tqdm(total=len(out), desc="Writing Train...")
for part, i in enumerate(range(int(len(out)/partitions), len(out), int(len(out)/partitions))):
    if compression_level != 0: 
        t=gzip.open(f"compressed-personality/train-{output_file}-{part}.txt.gz", "wb", compresslevel=compression_level)
    else: 
        t=io.open(f"personality/train-{output_file}-{part}.txt", mode="w", encoding="utf-8")
    fst=True   
    for line in out[i-int(len(out)/partitions):i]:
        if not fst: line="\n"+line
        else: fst=False
        if compression_level != 0: line=line.encode()
        t.write(line)
        pbar.update(1)
    t.close()
pbar.close()
     
pbar=tqdm(total=len(val), desc="Writing Val...")
for part, i in enumerate(range(int(len(val)/partitions), len(val), int(len(val)/partitions))):
    if compression_level != 0: 
        t=gzip.open(f"compressed-personality/eval-{output_file}-{part}.txt.gz", "wb", compresslevel=compression_level)
    else: 
        t=io.open(f"personality/eval-{output_file}-{part}.txt", mode="w", encoding="utf-8")
    fst=True  
    for line in val[i-int(len(val)/partitions):i]:
        if not fst: line="\n"+line
        else: fst=False
        if compression_level != 0: line=line.encode()
        t.write(line)
        pbar.update(1)
    t.close()
pbar.close()