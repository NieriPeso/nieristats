from decouple import config
from discord.ext import commands

from tasks import transparency

# INICIO DEL BOT PARA SU FUNCIONAMIENTO
bot = commands.Bot(command_prefix='$', help_command=None)


@bot.event
async def on_ready():
    print(f'Nieribot-statistics listo y operando con el user: {bot.user}')


# EJECUCIÓN DEL BOT
tc = transparency.TransparencyCog(bot)
bot.add_cog(tc)

bot.run(config('TOKEN'))
