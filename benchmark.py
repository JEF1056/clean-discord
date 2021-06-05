from src.workers import *

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Clean Discord data')
    parser.add_argument('-dir', type=str, default="data",
                        help='the fonlder that contains the data file')
    parser.add_argument('-out', type=str, default="output",
                        help='the folder to output txts')
    parser.add_argument('-step', type=str, nargs="+", default="all",
                        help='the step to start cleaning from')
    args = parser.parse_args()
    
    worstcase_clean="""
Hi, this is a test.
```
This is some code:
if args.step == "all":
    steps=["regex", "pairs", "detox"]
else:
    try:
        steps=json.loads(args.step)
        assert type(steps)==list
    except: raise Exception("Unable to load steps json.")
```
`this too, but it's only one line`
heh maybe if i put it on one line ```e``` or does `this` work
REEE WHY IS IS BEING CLEANED OOOOF HOWWWWWWWW
I AM THE KING ♕ ✦ —• YOU CANNOT STOP ME
what about this 𝐈𝐌𝐀𝐆𝐄, it should be IMAGE.
hahahahaha i bet you can't beat my cool asian language 毛泽东万岁
WHAAAAAAAAAAAAAAAAAAAAAAT noooooooooooooooooooooooo it can't be..................
hahaha but my best invention yet, my friend @Deleted User and @Deleted User. They will surely defeat you.
                     plenty              of                      spaces               ???????????????       🥲
fine. one last resort. my email is contact@j-fan.ml and you can join my server at https://jadeai.ml/server. Join or else.
if those didn't work maybe my phone numbers, +2 (666) 768-1111 or 408 220 0343 will work. meet me at 12:00 :3
✧・ﾟ:*ａｎｇｅｌａ*:･ﾟ☆✧: I am the best
    """
    print("Running a clean test case ~~~~~~~~")
    print(f"{worstcase_clean}\nRaw ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(clean(worstcase_clean).replace("\\n","\n"))
    print("Clean ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    
    if args.step == "all": args.step = ["regex", "detox", "antispam"]
    for step in args.step:
        if step == "regex":
            try:os.mkdir(args.out)
            except: pass
            print("\033[1mRunning regex test\033[0m")
            ret=[worker_regex(os.listdir(args.dir)[0], args.dir, args.out, debug=True)]
            write_stats(ret, args.out)
        elif step == "detox":
            try:os.mkdir(args.out+"-detox")
            except: pass
            print(f"\033[1mRunning detox test\033[0m")
            ret=[worker_detox([f for f in os.listdir(args.out) if f.endswith(".txt")][0], args.out, args.out+"-detox", debug=True)]
            write_stats(ret, args.out+"-detox")
        elif step == "antispam":
            try:os.mkdir(args.out+"-antispam")
            except: pass
            print("\033[1mRunning antispam test\033[0m")
            ret=[worker_antispam([f for f in os.listdir(args.out+"-detox") if f.endswith(".txt")][0], args.out+"-detox", args.out+"-antispam", debug=True)]
            write_stats(ret, args.out+"-antispam")
    print("DONE")