import io
import itertools
import re
import random
import argparse

normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}
alphabets=io.open("src/alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
for alphabet in alphabets[1:]:
    for ind, char in enumerate(alphabet):
        try:normalize_chars[char]=alphabets[0][ind]
        except: print(alphabet, len(alphabet), len(alphabets[0]));break
bot_prefixes=io.open("src/prefixes.txt", mode="r", encoding="utf-8").read().strip().split("\n")
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
replace_names={}

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def sizeof_fmt(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"

def gen_name(username):
    try:
        int(username)
        try: out_name=replace_names[username]
        except: 
            out_name=random.choice(names)
            replace_names[username]=out_name
        return out_name
    except: return "@"+random.choice(names)
    
#precompile regex
r1=re.compile(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|<:.+?:\d+>|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```(?:.?)+```|:[^:\s]*(?:::[^:\s]*)*:|(?:\\n)+|(?<=[:.,!?()]) (?=[:.,!?()])|(?!:3)[^a-z0-9.,!@?\s\/\U0001F600-\U0001F64F\U0001F300-\U0001F5FF]+', flags=re.DOTALL | re.IGNORECASE)
r2=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t]+')
r5=re.compile(r"([\.\'\"@?!a-z])\1{4,}", re.IGNORECASE)
r6=re.compile(r"\s(.+?)\1+\s", re.IGNORECASE)
r7=re.compile(r'@Deleted User')
r8=re.compile(r"([\s!?@\"\'])\1+")
r9=re.compile(r'\s([?.!\"](?:\s|$))')
r10=re.compile(r"^(?!my|\bdm\b|server|location|located)[a-z]+\s??(?!my|\bdm\b|server|location|located)[a-z]+\s??:[a-z0-9/.,\'\" ]{1,40}$", flags=re.M | re.IGNORECASE)

def clean(text, author=None):
    for prefix in bot_prefixes:
        if text.lower().startswith(prefix): return None #handle bot commands
    if "joined the server" in text.lower(): return None
    if "pinned a message" in text.lower(): return None
    if author != None and text == "Deleted User": return gen_name(author)
        
    unique=[i for i in list(set(text)) if i not in alphabets[0]] #handle special chars from other langs
    for char in unique: 
        try: text=text.replace(char, normalize_chars[char])
        except:pass
    text= re.sub(r1, "", text.strip()) #remove urls, emails, code blocks, custom emojis, spaces between punctuation, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r2, " ", text) #handle... interesting spaces
    text= re.sub(r5, r"\1\1\1", text) #handle excessive repeats of punctuation, limited to 3
    text= re.sub(r6, r" \1 ", text) #handle repeated words
    if author == None: text= re.sub(r7, gen_name, text) #replace "deleted users" with names
    text= re.sub(r8, r"\1",text) #handle excessive spaces or excessive punctuation
    text= re.sub(r9, r'\1', text) #handle spaces before punctuation but after text
    text= text.strip().replace("\n","\\n") #handle newlines
    
    if text != "\\n" and text != " " and text != "" and author==None:
        return text
    elif text != "\\n" and text != " " and text != "" and author!=None:
        return text.split(" ")[-1]
    else:
        return None
    
def gen_permuatations(name, info, shuffle=False):
    carrier=["who is", "who's", "that's", "that is"]
    outputs=[]
    all_combs=[]
    try:  del info["name"]
    except: pass
    [all_combs.extend(list(itertools.combinations(info, r))) for r in range(1,len(info)+1)]
    #name: [LITERAL JSON]
    #name: [LITERAL JSON WITHOUT BRACKETS]
    #name [LITERAL JSON]
    #name [LITERAL JSON WITHOUT BRACKETS]
    #[name][:/no :] ([x], [y], [z]...) (in vague/random order)
    #[name] [carrier] [x] [delimiter] [y] [delimiter] [z]... (in vague/random order)
    for comb in all_combs:
        get_perms=[[f"{combd}: {info[combd]}" for combd in perms] for perms in itertools.permutations(comb)]
        for get_json in get_perms:
            outputs.extend([name+": {"+", ".join(get_json)+"}",
                            f"{name}, {', '.join(get_json)}",
                            f"{name}: {', '.join(get_json)}",
                            f"{name}: [{', '.join(get_json)}]",
                            f"{name}: ({', '.join(get_json)})",
                            f"{name} ({', '.join(get_json)})",
                            f"{name}: ({', '.join([i.split(': ')[1].strip() for i in get_json])})",
                            f"{name} ({', '.join([i.split(': ')[1].strip() for i in get_json])})"
                            ]+
                           [f"{name} {carry} {' and '.join([i.split(': ')[1].strip() for i in get_json])}" for carry in carrier]+
                           [f"{name} {carry} {', '.join([i.split(': ')[1].strip() for i in get_json])}" for carry in carrier]
                            )
    return outputs
    
def extract_fields(message, shuffle=False):
    #NOTE: message should include a "\t" in it
    assert "\t" in message, "tab not found in message"
    assert len(message.split("\t")) == 2, f"more than one tab in message: "+str(len(message.split('\t')))
    input_str, output_str=[i.strip() for i in message.split("\t")]
    extracted={e[0].lower():e[1] for e in [[v.strip() for v in i.split(":")] for i in re.findall(r10, output_str)]}
    if extracted == None: return None
    
    output=[]
    output.extend(gen_permuatations(input_str, extracted))
    if "name" in extracted: output.extend(gen_permuatations(extracted["name"], extracted))
    output=[f"{inp}\{message}" for inp in output]
    
    return output