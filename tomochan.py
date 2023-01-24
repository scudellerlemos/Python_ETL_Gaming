import discord
from discord.ext import commands

bot= commands.Bot("!")


@bot.event
async def on_ready():
    print("Estou pronta!")


bot.run("MTA2NzI4MjI1MjU5MTAyNjE3Nw.Gtr59z.6_CnqJ8g_3holvhLfApU0Duo8fN-ZnrxR6tfo8")

