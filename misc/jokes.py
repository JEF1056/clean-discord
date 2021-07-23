import io
from tqdm import trange
import random
import gzip

names=io.open("../src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
train=io.open("/home/jfan/data/persona/train_both_revised.txt", mode="r", encoding="utf-8").read().strip().split("\n")
train.extend(io.open("/home/jfan/data/persona/test_both_revised.txt", mode="r", encoding="utf-8").read().strip().split("\n"))
val=io.open("/home/jfan/data/persona/valid_both_revised.txt", mode="r", encoding="utf-8").read().strip().split("\n")

temp=[]
for i in trange(0,len(train)-1):
    if int(train[i+1].split(" ")[0]) == 1:
        temp.append((" ".join(train[i].split(" ")[1:]).replace("|", "\\n")))
        print(temp)
        temp=[]
    else: temp.append((" ".join(train[i].split(" ")[1:]).replace("|", "\\n")))
    
    