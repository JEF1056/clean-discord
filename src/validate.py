import os
import io
import json
from tqdm import tqdm

def check_files(dir, server=""):
    failed, num_processed=[],0
    for file in os.listdir(dir):
        if file.startswith(server) or server=="":
            with open(os.path.join("data",file), 'rb') as f:
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b' ':
                    f.seek(-2, os.SEEK_CUR)
                last_line = f.readline().decode()
                try: int(last_line)
                except: failed.append(f"{file} ||| {last_line}")
                num_processed+=1

    assert num_processed != 0, f"No files processed/No files in dir:{dir}"
    if failed != []:
        if input("Found errors:\n\n"+"\n".join(failed)+"\n\nFix files? Type one of the following:  [y,yes]\n> ") in ["yes",'y']:
            fix_files(dir, [i.split(" ||| ")[0] for i in failed])
        else: assert failed == [], f"Some files in data directory are incomplete downloads (see above)"
    return "All files passed."

def fix_files(dir, filenames):
    assert type(filenames)==list, "inputs must be a list of filenames in the directory"
    for filename in tqdm(filenames, desc="Fixing files"):
        file_data=io.open(os.path.join(dir,filename), mode="r", encoding="utf-8").read()+"]}"
        file_data = json.loads(file_data)
        len_file_data=str(len(file_data["messages"]))
        del file_data
        with io.open(os.path.join(dir,filename), mode="a", encoding="utf-8") as f:
            f.write('\n  ],\n  "messageCount": '+len_file_data+'\n}')