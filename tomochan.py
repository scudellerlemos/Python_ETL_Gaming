import discord
from discord.ext import commands

bot= commands.Bot("!")


@bot.event
async def on_ready():
    print("Estou pronta!")


bot.run("MTA2NzI4MjI1MjU5MTAyNjE3Nw.G2gIyO.Kfbm1Z1WFBA9FFRCvu95HY6o0TTa1tjzW0aKrM")

