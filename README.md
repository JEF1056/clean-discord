# Clean-Discord
clean-discord is a fast, efficient, and robust script for cleaning large quantities of messages from discord data generated by [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)

## Cleaning the data
The process of cleaning the data includes removing a lot of the issues that can be found in discord chat logs, including:
- Translating "special" unicode-based characters into the english alphabet (text like `T҉o҉X҉i҉C҉` to `ToXiC`)
- Converting excessive spaces and unicode spaces to traditional spaces (text like `hi  		, you!` to `hi, you`)
- Replace users who left the the server(s) without being properly cached (they show up as `Deleted User`) with a random [name](./src/names.txt) that is attached to their id (names like `@Deleted User` to `@Jake`)
- Fixing excessive punctuation 
- Removing URLs (like `https://jadeai.ml`)
- Removing emails (like `contact@j-fan.ml`)
- Removing phone numbers (like `+1 (123) 456-7890`)
- Removing custom emojis (like `:pogchamp:`)