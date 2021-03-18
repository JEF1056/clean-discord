import re
import io
import ijson
from pyinstrument import Profiler

def worker(filename):
    messages=ijson.items(io.open(filename, mode="r", encoding="utf-8"), 'messages.item')
    reqinfo=({"id": d["id"], #this reduces the memory down furhter by only keping required elements
              "timestamp": d["timestamp"],
              "name": d['author']['name'], 
              'authorid':d['author']['id'], 
              "content":d["content"]}
             for d in messages #this filters obvious issues from our dataset, reducing the compute time required with uneeded calls
             if d['author']['isBot'] == False #author is a bot
             and d["type"] == "Default" #the message is not a system message
             and d["content"] #he message is empty
             )
    for i, v in enumerate(reqinfo):
        print(v)
        exit()
    
worker("data/test.json")