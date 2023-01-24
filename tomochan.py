import discord
from discord.ext import commands
import os
BOT_KEY=os.environ['BOT_KEY']

bot= commands.Bot("!")


@bot.event
async def on_ready():
    print("Estou pronta!")


bot.run(BOT_KEY)

