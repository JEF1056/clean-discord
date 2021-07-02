import io
from tqdm import tqdm
import random
import gzip

examples_pairs=io.open("../src/commonquestions.txt", mode="r", encoding="utf-8").read().strip().split("\n")
examples_convos=io.open("../src/commonconvos.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("../src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")

personas=[
    "Name: Jade\\nAge: 18\\nGender: Female\\nAbout me: I like video games, anime, music, and a lot of things actually. I've never had any online friends",
    "Name: Jade\\nAge: 18\\nGender: female\\nSexuality: lesbian\\nLikes: Koop, sleeping, Netflix, cats, music etc\\nCountry: United States!\\nFact: I also speak random languages occasionally!\\nStatus: single",
    "name : Jade\\ngender: Female\\nage: 17\\nheight : 55\\nhobbies: Gaming, watching anime, cuddling, being spoiled, and talking in general with people",
    "name: jade\\nage: 18\\ngender: female\\nheight: 5'4 on the dot\\nhobbies: making music, gaming, riding horses",
    "jade\\nshe/her\\n18\\nusa\\nsanriocore, angelcore, scenecore!!!\\ni like slice of life animes but some fantasy are cute too!\\nalso open to making new friends (:\\nfrog girl...",
    "Name: jade\\nAge: minor\\nGender: female\\nAbout me: I play volleyball and I like anime my favorite is kakguri and Naruto...",
    "name: jade\\nage : 18\\ngender : f\\npronouns : she/her/cool kid\\nsexuality : straight\\ndms open",
    "name : jade\\nage : 18\\ngender : f\\nlikes : anything\\ndislikes : anything\\ndms open",
    "Name: Jade\\nGender: female\\nAge: 18\\nCountry: N/A\\nInterest/hobby: drawing, vocal synths, and sleeping\\nDMs: I don't mind, but VCing is more fun for me if possible.",
    "Name/Nickname: Jade\\nAge: adult\\nGender: whatever\\nHobbies: drawing, vocalsynth (vocaloid/utau), and vidya games\\nOther facts you want to share: I really really love pizza",
    "Name: Jade\\nGender: Female\\nAge: 18\\nFrom : USA\\nLikes: avocados mangos and animals\\nDislikes: spiders mosquitoes and rude people",
    "name: jade\\ngender: female\\nage: 18\\nsexuality: pans\\nalso i live in USA",
    "name: jade\\npronouns: she/her\\ncountry: america\\nhobbies: listening to music and getting into journalism\\nfav villagers: cant pick one yet"
]

max_len=10
compression_level=9
output_file="../personality/persona"
partitions= 1000

def gen_all(line):
    line=line.strip().replace("\\n", "/n").split("\t")
    ret=[]
    for y in range(1,len(line)):
        max_back=y-max_len if y-max_len >= 0 else 0
        sample=random.sample(range(max_back+1, y), y-max_back-1 if y-max_back-1 <=5 else 5)+[max_back]
        for x in sample:
            try:
                ret.append(f"persona: {random.choice(personas)} context: {'/b'.join(line[x:y])}\t{': '.join(line[y].split(': ')[1:])}")
            except: break
    return ret

out=[]
val=[]

for example in tqdm(examples_pairs, "Generating Pairs..."):
    random.shuffle(names)
    for name in names[:500]:
        out.append(f"persona: {random.choice(personas)} context: "+example.replace("[USER]", name).replace("[BOT]", "Jade"))
    for name in names[:50]:
        val.append(f"persona: {random.choice(personas)} context: "+example.replace("[USER]", name).replace("[BOT]", "Jade"))

for example in tqdm(examples_convos, "Generating Convos..."):
    random.shuffle(names)
    for name in names[:500]:
        out.extend(gen_all(example.replace("[USER]", name).replace("[BOT]", "Jade")))
    for name in names[:50]:
        val.extend(gen_all(example.replace("[USER]", name).replace("[BOT]", "Jade")))
    
random.shuffle(out)
random.shuffle(val)
print(len(out))
print(len(val))

pbar, fst=tqdm(total=len(out), desc="Writing Train..."), True 
for part, i in enumerate(range(int(len(out)/partitions), len(out), int(len(out)/partitions))):
    with gzip.open(f"{output_file}-train-{part}.txt.gz", "wb", compresslevel=compression_level) as t:
        for line in out[i-int(len(out)/partitions):i]:
            if not fst: line="\n"+line
            else: fst=False
            t.write(line.encode())
            pbar.update(1)
pbar.close()

pbar, fst=tqdm(total=len(val), desc="Writing val..."), True 
for part, i in enumerate(range(int(len(val)/partitions), len(val), int(len(val)/partitions))):
    with gzip.open(f"{output_file}-val-{part}.txt.gz", "wb", compresslevel=compression_level) as v:
        for line in val[i-int(len(val)/partitions):i]:
            if not fst: line="\n"+line
            else: fst=False
            v.write(line.encode())
            pbar.update(1)
pbar.close()