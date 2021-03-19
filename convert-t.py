import os
import argparse
import functools
from tqdm import tqdm
import concurrent.futures
from src.workers import worker
from src.validate import check_files

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-dir', type=str, default="data",
                    help='the data folder containing discord .jsons')
parser.add_argument('-out', type=str, default="output",
                    help='the folder to output txts')
args = parser.parse_args()

try:os.mkdir(args.out)
except: pass

def start_work():
    total_lines=check_files(args.dir)
    with concurrent.futures.ProcessPoolExecutor(max_workers=None) as executor:
        arg=((filename, args.dir, args.out)  for filename in os.listdir(args.dir))
        print(list(arg))  
        ret=list(tqdm(executor.map(lambda p: worker(*p), arg), total=total_lines))
    print(ret)
        
if __name__ == '__main__':
    start_work()