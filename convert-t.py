import os
import argparse
from tqdm import tqdm
import concurrent.futures
from itertools import repeat
from src.workers import worker
from src.validate import check_files

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output",
                    help='the folder to output txts')
parser.add_argument('-workers', type=int, default=None,
                    help='the folder to output txts')
args = parser.parse_args()

try:os.mkdir(args.out)
except: pass

def start_work():
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        ret=list(tqdm(executor.map(worker, os.listdir(args.dir), repeat(args.dir), repeat(args.out)), total=len(os.listdir(args.dir))))
    print(ret)
        
if __name__ == '__main__':
    start_work()