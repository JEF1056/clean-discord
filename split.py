import os
import io
import time
import gzip
import argparse
from tqdm import tqdm
import multiprocessing as mp

parser = argparse.ArgumentParser(description='Split dataset in multiple files into train and validation sets')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing the processed files on the top level')
parser.add_argument('-out', type=str, default="context",
                    help='prefix the compressed output file sets')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
parser.add_argument('-compression_level', type=int, default=9, choices=list(range(-1,10)),
                    help='how compressed the file should be')
args = parser.parse_args()

if args.workers == None: args.workers= mp.cpu_count()*2

def convertline(text, max_length=20):
    text=text.split("\t") #split the conversation by tabs
    inputs, targets=[],[] #create empty arrays for inputs and targets
    for y in range(1,len(text)): #iterate through the split conversation
        x=y-max_length if y-max_length >= 0 else 0 #get the starting value; if it's negative, use 0 instead
        inputs.append("/b".join(text[x:y])) #append to the inputs the current window, joined by /b
        targets.append(text[y]) #append the target
    return [f"{inputs[i]}\t{targets[i]}" for i in range(len(inputs))] #zip them together in a dict of inputs and targets

def worker(filename, q):
    with io.open(os.path.join(args.dir, filename), mode="r", encoding="utf-8") as f:
        line = f.readline()
        while line:
            q.put(convertline(line.strip()))
            line=f.readline()
    return "DONE"

def listener(q, split):
    fst=True
    with gzip.open(f"{args.out}-{split}.txt.gz", "wb", compresslevel=args.compression_level) as f:
        while 1:
            m = q.get()
            if m == 'kill':
                f.write('killed')
                break
            for line in m:
                if fst: f.write(line, args.compression_level); fst=False
                else: f.write("\n"+line, args.compression_level)
                
def main(files, split):
    #must use Manager queue here, or will not work
    manager = mp.Manager()
    q = manager.Queue()    
    pool = mp.Pool(args.workers)

    #put listener to work first
    watcher = pool.apply_async(listener, (q, split))

    #fire off workers
    jobs = []
    for file in files:
        job = pool.apply_async(worker, (file, q))
        jobs.append(job)

    # collect results from the workers through the pool result queue
    for job in tqdm(jobs, desc=f"Processing {split}..."): 
        job.get()

    #now we are done, kill the listener
    q.put('kill')
    time.sleep(0.01)
    pool.close()
    pool.join()

if __name__ == '__main__':
    files=os.listdir(args.dir)
    files.sort()
    files.remove('stats.json')
    cut_off = int(len(files) * .05)
    train_files, eval_files = files[:-cut_off], files[-cut_off:]
    main(train_files, "train")
    main(eval_files, "val")