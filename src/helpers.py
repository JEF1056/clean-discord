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

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

#precompile regex
r1=re.compile(r'@Deleted User')
r2=re.compile(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=\n]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)|:[^\n\s]+?:|[\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4}|(?:\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|```.+?```\n?|(?:\\n)+|\b(?:a*ha+h[ha]*|o?l+o+l+[ol]*)\b|[^a-z0-9.,:;\'\”@!?\s\<\>\/\-\+\=\(\)\[\]*_'+''.join(emojis)+r']+|(?<=[a-z.,\':;!?\/]) +(?=[.,\'!?\/])|([,\':;\s\/\(\)\[\]\+\-\<\>\=])\1+|([_])\2{2,}|([a-z.!?*])\3{3,}|(: )(?:> (?:.*?)(?:\n+|\\n+|$))+', flags=re.DOTALL | re.IGNORECASE)
r3=re.compile(r'[\U00003000\U0000205F\U0000202F\U0000200A\U00002000-\U00002009\U00001680\U000000A0\t]+| {2,}')
r4=re.compile(r"(.{3,})\1", re.IGNORECASE | re.DOTALL)

def clean(text, author=False):
    if text.lower() == "welc" or ("welcome" in text.lower()): return None #welcome is the bane of exisitence and needs to be culled
    if "@everyone" in text.lower() or "@here" in text.lower(): return None #no need for these kinds of pings, and messages in them are even more useless.
    if text[text.find(': ')+2:].strip().lower().startswith(bot_prefixes): return None #handle bot commands
    if author and text.startswith("Deleted User"): text=gen_name(author)+text[len("Deleted User"):]
    
    text=text.translate(normal_map)#handle special chars from other langs
    text= re.sub(r1, gen_name, text.strip()) #replace "deleted users" with names
    text= re.sub(r2, r"\1\2\2\3\3\3\4", text.strip()) #remove urls, emails, code blocks, custom emojis, non-emoji, punctuation, letters, and phone numbers
    text= re.sub(r3, " ", text.strip()) #handle... interesting spaces
    text= "".join(list(map(convemojis,text.strip()))) #translate emojis to their `:text:` shorthand form
    text= "\\n".join([ln.strip().strip("\t") for ln in text.split("\n")]) #handle newlines
    if text.startswith(": "): text=gen_name(author)+text

    if not (text[text.find(':')+1:].strip() in ["", "\\n", "\n", " ", "\t"] or text[text.find(':')+1:].strip().lower().startswith(bot_prefixes)): 
        return text.lstrip(("!.,^#")).strip().replace("\t", " ")
    else: return None