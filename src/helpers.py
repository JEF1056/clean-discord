import re, io, json, random

normalize_chars={'Š':'S', 'š':'s', 'Ð':'Dj','Ž':'Z', 'ž':'z', 'À':'A', 'Á':'A', 'Â':'A', 'Ã':'A', 'Ä':'A',
    'Å':'A', 'Æ':'A', 'Ç':'C', 'È':'E', 'É':'E', 'Ê':'E', 'Ë':'E', 'Ì':'I', 'Í':'I', 'Î':'I',
    'Ï':'I', 'Ñ':'N', 'Ń':'N', 'Ò':'O', 'Ó':'O', 'Ô':'O', 'Õ':'O', 'Ö':'O', 'Ø':'O', 'Ù':'U', 'Ú':'U',
    'Û':'U', 'Ü':'U', 'Ý':'Y', 'Þ':'B', 'ß':'Ss','à':'a', 'á':'a', 'â':'a', 'ã':'a', 'ä':'a',
    'å':'a', 'æ':'a', 'ç':'c', 'è':'e', 'é':'e', 'ê':'e', 'ë':'e', 'ì':'i', 'í':'i', 'î':'i',
    'ï':'i', 'ð':'o', 'ñ':'n', 'ń':'n', 'ò':'o', 'ó':'o', 'ô':'o', 'õ':'o', 'ö':'o', 'ø':'o', 'ù':'u',
    'ú':'u', 'û':'u', 'ü':'u', 'ý':'y', 'ý':'y', 'þ':'b', 'ÿ':'y', 'ƒ':'f',
    'ă':'a', 'î':'i', 'â':'a', 'ș':'s', 'ț':'t', 'Ă':'A', 'Î':'I', 'Â':'A', 'Ș':'S', 'Ț':'T',}
alphabets=io.open("src/alphabets.txt", mode="r", encoding="utf-8").read().strip().split("\n")
emojis=json.load(io.open("src/emojis.json", mode="r", encoding="utf-8"))
for alphabet in alphabets[1:]:
    for ind, char in enumerate(alphabet):
        try:normalize_chars[char]=alphabets[0][ind]
        except: print(alphabet, len(alphabet), len(alphabets[0]));break
normalize_chars.update({i:i for i in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'})

normal_map=str.maketrans(normalize_chars)
del normalize_chars

bot_prefixes=tuple(io.open("src/prefixes.txt", mode="r", encoding="utf-8").read().strip().split("\n"))
names=io.open("src/names.txt", mode="r", encoding="utf-8").read().strip().split("\n")
replace_names={}

def gen_name(username):
    try:
        int(username)
        try: out_name=replace_names[username]
        except: 
            out_name=random.choice(names)
            replace_names[username]=out_name
        return out_name
    except: return " @"+random.choice(names)

def convemojis(i):
    if i in emojis: return emojis[i]
    return i

#precompile regex
r1=re.compile(r'@Deleted User')
r2=re.compile(r'^> (?:.*)+$|https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|:[a-z0-9]+?:|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(?:\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```.+?```\n?|(?:\\n)+|\b(?:a*ha+h[ha]*|o?l+o+l+[ol]*)\b|[^a-z0-9.,:;\'\”@!?\s\/'+''.join(emojis)+chr(0)+r']+|(?<=[a-z.,\':;!?\/]) +(?=[.,\':;!?\/])|([a-z.])\1{3,}|([,\':;!?\s\/])\2+', flags=re.DOTALL | re.IGNORECASE)
r3=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t]+| {2,}')
r4=re.compile(r"(.{3,})\1", re.IGNORECASE | re.DOTALL)

def clean(text, author=False):
    if text.lower() == "welcome" or text.lower().startswith("welcome"): return None #welcome is the bane of exisitence and needs to be culled
    if "@everyone" in text.lower() or "@here" in text.lower(): return None #no need for these kinds of pings, and messages in them are even more useless.
    if text.lower().startswith(bot_prefixes): return None #handle bot commands
    if author:
        if text.startswith("Deleted User"): text=gen_name(author)+text[len("Deleted User"):]
        elif text.startswith(chr(0)): text=gen_name(author)+text
    
    text=text.translate(normal_map)#handle special chars from other langs
    text= re.sub(r1, gen_name, text) #replace "deleted users" with names
    text= re.sub(r2, r"\1\1\1\2", text.strip()) #remove urls, emails, code blocks, custom emojis, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r3, " ", text) #handle... interesting spaces
    text= "".join(list(map(convemojis,text))) #translate emojis to their `:text:` shorthand form
    text= "\\n".join([ln.strip().strip("\t") for ln in text.split("\n")]) #handle newlines
         
    if text != chr(0) and author:
        text=text.split(chr(0))
        if not text[0] in ["", "\\n", "\n", " ", "\t"] and not text[1] in ["", "\\n", "\n", " ", "\t"]:
            return text
        else:
            return None
    elif not author:
        if not text in ["", "\\n", "\n", " ", "\t"]:
            return text
        else:
            return None
    else:
        return None