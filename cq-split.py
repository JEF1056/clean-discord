import io
import random

lines=io.open("src/commonquestions.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")

out=[]
for entry in lines:
    for name in names:
        out.append(name+": "+entry.replace("[USER]",name)+"\n")

random.shuffle(out)   
         
with io.open("splt-commonq.txt", mode="w", encoding="utf-8") as f:
    for val in out:
        f.write(val)