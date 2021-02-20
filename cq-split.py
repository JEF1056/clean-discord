import io
import random
from tqdm import tqdm

lines=io.open("src/commonquestions.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")

out=[]
for entry in tqdm(lines):
    for name in names:
        out.append(name+": "+entry.replace("[USER]",name)+"\n")

print("Shuffling...")
random.shuffle(out)
print("Splitting...")
cut_off = int(len(out) * .05)
train_data, eval_data = out[:-cut_off], out[-cut_off:]
del out
         
with io.open("commonq/splt-commonq-train.txt", mode="w", encoding="utf-8") as f:
    for val in train_data:
        f.write(val)
        
with io.open("commonq/splt-commonq-val.txt", mode="w", encoding="utf-8") as f:
    for val in eval_data:
        f.write(val)