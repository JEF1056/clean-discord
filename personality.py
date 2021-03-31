import io
from tqdm import tqdm
import random
import gzip

examples_pairs=io.open("src/commonquestions.txt", mode="r", encoding="utf-8").read().strip().split("\n")
examples_convos=io.open("src/commonconvos.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")

max_len=10
compression_level=0
out="personality"

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

if compression_level != 0: 
    t=gzip.open(f"train-{out}.txt.gz", "wb", compresslevel=compression_level)
    v=gzip.open(f"eval-{out}.txt.gz", "wb", compresslevel=compression_level)
else: 
    t=io.open(f"train-{out}.txt", mode="w", encoding="utf-8")
    v=io.open(f"eval-{out}.txt", mode="w", encoding="utf-8")
    
fst=True
for line in tqdm(out, desc="Writing Train..."):
        if not fst: line="\n"+line
        else: fst=False
        if compression_level != 0: line.encode()
        t.write(line)
        
for line in tqdm(val, desc="Writing Val..."):
        if not fst: line="\n"+line
        else: fst=False
        if compression_level != 0: line.encode()
        v.write(line)

t.close(); v.close()