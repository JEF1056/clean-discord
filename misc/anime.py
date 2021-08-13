import os
import re
import sys
import ass
import srt
import json
os.chdir('../')
from tqdm import tqdm
sys.path.append('src')
from helpers import clean
import concurrent.futures

fdir="/home/jfan/data/Anime Datasets V3/Extracted"
odir="/home/jfan/clean-discord/ads"
workers=8

r0=re.compile(r'\{.*?\}|\[.*?\]|\s*?\\N\s*?', flags=re.DOTALL | re.IGNORECASE)
r1=re.compile(r'\<.*?\>|\[.*?\]', flags=re.DOTALL | re.IGNORECASE)

def worker(filename):
    assert filename.lower().endswith(".ass") or filename.lower().endswith(".srt"), f"Only .ass and .srt files are currently supported. found {filename}"
    template={"title": "Anime Dataset", "stats": {"original": 0, "removed": 0, "current": 0}, "conversations":[]}
    title=filename[:-4]
    filename=os.path.join(fdir, filename)
    if filename.lower().endswith(".ass"):
        with open(filename, encoding='utf-8-sig') as f: #load the data
            try: data = ass.parse(f).events
            except Exception as e: 
                print(f"err in {filename} {e}")
                return None
            for text in data: #clean the data
                text=re.sub(r0, " ", text.text)
                text=clean(text)
                if (text and len(text) > 1 and len(text) < 1000 and 
                    text.count(" ") <= ((len(text)//4) if (len(text)//4) > 0 else 0)): 
                    template["conversations"].append(text)  
    elif filename.lower().endswith(".srt"):
        with open(filename, encoding='utf-8-sig') as f: #load the data
            try: data = list(srt.parse(f))
            except Exception as e: 
                print(f"err in {filename}, {e}")
                return None
            for text in data: #clean the data
                text=re.sub(r1, " ", text.content.replace("\n", " ").replace("\\n", " "))
                text=clean(text)
                if (text and len(text) > 1 and len(text) < 1000 and 
                    text.count(" ") <= ((len(text)//4) if (len(text)//4) > 0 else 0) and
                    "downloaded from" not in text.lower() and "translation by" not in text.lower() and 
                    "subtitles by" not in text.lower() and "episode" not in text.lower()): 
                    template["conversations"].append(text)
    else:
        print("file read mode detection failed.")
        return None
    
    temp=[]
    for text in template["conversations"]: #deduplication
        if text in temp[-16:]: pass
        else: temp.append([text])
    template["conversations"]=[temp]
    template["stats"]["original"]+=len(data)
    template["stats"]["removed"]+=(len(data)-len(template["conversations"]))
    template["stats"]["current"]+=len(template["conversations"])
    json.dump(template, open(os.path.join(odir, f"{title}.json"), mode="w"))
    
    
if __name__ == '__main__':
    tasks=[f for f in os.listdir(fdir)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        ret=list(tqdm(executor.map(worker, tasks), total=len(tasks), desc="Processing..."))
    print("DONE")