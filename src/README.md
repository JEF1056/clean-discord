# Debugging results for the worker script
## Regex cleaning on 300k lines (~25-27s/300k lines, e.g. ~11,600 lines/sec):
```
27.158 worker_regex  workers.py:84
├─ 18.921 clean  workers.py:65
│  ├─ 13.026 sub  re.py:203
│  │     [18 frames hidden]  re, .., sre_parse
│  ├─ 2.651 [self]
│  ├─ 1.623 str.startswith  ../<built-in>:0
│  │     [2 frames hidden]  ..
│  ├─ 0.669 convemojis  workers.py:61
│  └─ 0.484 str.translate  ../<built-in>:0
│        [2 frames hidden]  ..
├─ 5.601 [self]
└─ 2.256 read  ijson/compat.py:31
      [12 frames hidden]  ijson, codecs, ..
```
## Detox cleaning on 300k lines (~4-5s/300k lines, e.g. ~70k lines/sec):
```
4.746 worker_detox  workers.py:115
├─ 3.885 predict  profanity_check/profanity_check.py:11
│     [617 frames hidden]  profanity_check, sklearn, .., numpy, ...
│        3.041 _count_vocab  sklearn/feature_extraction/text.py:1097
│        ├─ 1.104 [self]
├─ 0.365 str.join  ../<built-in>:0
│     [2 frames hidden]  ..
├─ 0.325 [self]
└─ 0.068 array  ../<built-in>:0
      [2 frames hidden]  ..
```
--------------------------
## Test case:
```
Hi, this is a test.
```This is some code:
if args.step == "all":
    steps=["regex", "pairs", "detox"]
else:
    try:
        steps=json.loads(args.step)
        assert type(steps)==list
    except: raise Exception("Unable to load steps json.")```
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
if those didn't work maybe my phone numbers, +2 (666) 768-1111 or 408 220 0343 will work
```
## Test results: 
```
Hi, this is a test.
this too, but it's only one line
heh maybe if i put it on one line or does this work
REEE WHY IS IS BEING CLEANED OOOF HOWWW
I AM THE KING YOU CANNOT STOP ME
what about this IMAGE, it should be IMAGE.
 i bet you can't beat my cool asian language
WHAAAT nooo it can't be...
 but my best invention yet, my friend @Nelan and @Calmas. They will surely defeat you.
 plenty of spaces? :smiling_face_with_tear:
fine. one last resort. my email is and you can join my server at Join or else.
if those didn't work maybe my phone numbers, or will work
```