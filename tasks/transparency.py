import asyncio
import datetime
import io
import re
import time
from typing import List

import discord
import matplotlib.pyplot as plt
import networkx as nx
import requests
import tabulate
from discord.ext import tasks, commands

# billeteras que hayan enviado a una cantidad de billeteras menor a esta se considerarán "enviadoras" sospechosas
SENDER_SUSPICION_THRESHOLD = 5

EVENT_LOG_URL = "https://api.bscscan.com/api" \
                "?module=logs" \
                "&action=getLogs" \
                "&address=0x811496d46838ccf9bba46030168cf4d7d588d04a" \
                "&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ANALYTICS_URL = "https://bscscan.com/token/token-analytics?m=normal&contractAddress=0x811496d46838ccf9bba46030168cf4d7d588d04a&a=&lg=en"


class TransparencyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.update_transparency_data.start()
        self.start_logs = 7579130
        self.transaction_data = set()
        self.transparent_channel: List[discord.TextChannel] = None

    def cog_unload(self):
        self.update_transparency_data.cancel()

    @tasks.loop(hours=6)
    async def update_transparency_data(self):
        if self.transparent_channel is None:
            chanlist = list(self.bot.get_all_channels())
            chanlist = ([c for c in chanlist if 'transparente' in c.name])
            self.transparent_channel = chanlist
            print(self.transparent_channel)
        await self.update_wallet_connections()
        await self.wallet_transparency_messages()
        await self.statistics_messages()

    async def statistics_messages(self):
        data_x, data_y_count, data_y_marketval, data_y_recv, data_y_send = await self.analytics_data()

        def obj_range(start, end, stepval):
            while start < end:
                yield start
                start += stepval

        plt.figure(figsize=(6, 6))
        ax1 = plt.subplot(211)
        ax1.plot(data_x, data_y_send, label='Unique senders')
        ax1.plot(data_x, data_y_recv, label='Unique receivers')
        ax1.set_xticks(list(obj_range(min(data_x), max(data_x), datetime.timedelta(days=10))))
        ax1.legend()

        ax2 = plt.subplot(212)
        ax2.plot(data_x, data_y_count, label='Número de transacciones del día')
        ax22 = ax2.twinx()
        ax22.plot(data_x, data_y_marketval, label='Cantidad de NIERIs transferidos en el día',color='r')
        ax2.set_xticks(list(obj_range(min(data_x), max(data_x), datetime.timedelta(days=10))))
        h1, l1 = ax2.get_legend_handles_labels()
        h2, l2 = ax22.get_legend_handles_labels()
        ax2.legend(h1 + h2, l1 + l2, loc=2)
        bytes_stream = io.BytesIO()
        plt.savefig(bytes_stream, format='png', bbox_inches="tight", dpi=80)
        plt.close()
        bytes_stream.seek(0)
        chart = discord.File(bytes_stream, filename='estadisticas_transacciones.png')
        embed = discord.Embed(title='Estadísticas de transacciones')
        embed.set_image(url='attachment://estadisticas_transacciones.png')
        for c in self.transparent_channel:
            await c.send(
                content=f"Transacciones del día {data_x[-1]}: {data_y_count[-1]} por un valor total de Ñ$ {data_y_marketval[-1]}",
                embed=embed, file=chart)

    async def analytics_data(self):
        r = (await asyncio.get_event_loop().run_in_executor(None, requests.get,
                                                            ANALYTICS_URL))
        r = r.text.split('\n')
        infoline = next(filter(lambda x: 'plotData =' in x, r))
        analytics = re.findall(
            "(\[Date\.UTC\((20[0-9]{2}),([0-9]{1,2}),([0-9]{1,2})\),([0-9.]+),([0-9]+),([0-9]+),([0-9]+),([0-9]+)\])",
            infoline)
        analytics = sorted(analytics, key=lambda x: x[1][0] * 365 + x[1][1] * 31 + x[1][2])
        data_x = []
        data_y_send = []
        data_y_recv = []
        data_y_count = []
        data_y_marketval = []
        for string, year, month, day, trans_amount, trans_count, uniq_recv, uniq_send, tot_uniq in analytics:
            data_x.append(datetime.datetime(int(year), int(month) + 1, int(day)))
            data_y_send.append(int(uniq_send))
            data_y_recv.append(int(uniq_recv))
            data_y_count.append(int(trans_count))
            data_y_marketval.append(float(trans_amount))
        return data_x, data_y_count, data_y_marketval, data_y_recv, data_y_send

    async def wallet_transparency_messages(self):
        node_colors, node_sizes, sG_labels, sender_wallets, susp_G, suspiciousness_table = await self.calculate_suspiciousness()
        plt.figure(figsize=(6, 6))
        try:
            nx.draw_planar(susp_G, node_size=node_sizes, node_color=node_colors, with_labels=False, labels=sG_labels)
        except nx.exception.NetworkXException:
            nx.draw_spring(susp_G, node_size=node_sizes, node_color=node_colors, with_labels=False, labels=sG_labels)
        bytes_stream = io.BytesIO()
        plt.savefig(bytes_stream, format='png', bbox_inches="tight", dpi=80)
        plt.close()
        bytes_stream.seek(0)
        chart = discord.File(bytes_stream, filename='billeteras_sospechosas.png')
        susp_tab = list(suspiciousness_table.items())
        susp_tab.sort(key=lambda x: -(x[1]))
        embed = discord.Embed(title='Billeteras sospechosas')
        embed.set_image(url='attachment://billeteras_sospechosas.png')
        for (sender, count) in susp_tab[:10]:
            embed.add_field(name=sender, value=count)
        for c in self.transparent_channel:
            await c.send(embed=embed, file=chart)
        statistics_table_content = tabulate.tabulate(
            [[sender, destc, recvc] for (sender, destc, recvc) in
             sender_wallets], headers=['Billeteras', 'Destinos', 'Orígenes']
        )
        embed = discord.Embed(title='Estadísticas de transacciones', description=statistics_table_content)
        for c in self.transparent_channel:
            await c.send(embed=embed)

    async def calculate_suspiciousness(self):
        G = nx.DiGraph()
        G.add_edges_from(list(self.transaction_data))
        susp_wallets_senders = ([n for n in G.nodes if len(list(G.successors(n))) < SENDER_SUSPICION_THRESHOLD])
        susp_wallets_receivers = set(sum([list(G.successors(n)) for n in susp_wallets_senders], []))
        suspiciousness_table = {}
        susp_send = {}
        sender_wallets = sorted([(n, len(set(G.successors(n))), len(set(G.predecessors(n)))) for n in G.nodes if
                                 len(set(G.successors(n))) > 0 or len(set(G.predecessors(n))) > 0],
                                key=lambda x: -(x[2] + x[1]))[:10]
        for r in susp_wallets_receivers:
            sus = set([w for w in G.predecessors(r) if w in susp_wallets_senders])
            suspiciousness_table[r] = len(sus)
            susp_send[r] = sus
        susp_G = nx.DiGraph()
        for r in susp_send:
            for s in susp_send[r]:
                susp_G.add_edge(s, r)
        G = nx.DiGraph()
        G_labels = {s: (hex(int(s, 16))[:5] + "...") for s in
                    set([d[1] for d in self.transaction_data]) | set([d[0] for d in self.transaction_data])}
        for (s, r) in list(self.transaction_data):
            G.add_edge(s, r)
        node_sizes = [((((suspiciousness_table[n] / max(
            suspiciousness_table.values())) / 2) + 0.5 if n in suspiciousness_table else 0)) * 200 + 50 for n in
                      susp_G.nodes()]
        sG_labels = {k: v for (k, v) in G_labels.items() if k in susp_G.nodes}
        node_colors = ['r' if n in suspiciousness_table else 'b' for n in susp_G.nodes()]
        return node_colors, node_sizes, sG_labels, sender_wallets, susp_G, suspiciousness_table

    async def update_wallet_connections(self):
        have_extra = True
        # pedimos datos desde el proximo bloque hasta que no haya mas transacciones
        while have_extra:
            event_loop = asyncio.get_event_loop()
            transaction_logs = (await event_loop.run_in_executor(None, requests.get,
                                                                 EVENT_LOG_URL + "&fromBlock=" + str(
                                                                     self.start_logs))).json()
            if type(transaction_logs['result']) == str:
                print(transaction_logs['result'])
                # hay que waitear por el rate limiter
                time.sleep(5)
                continue
            tr_data = set([(t['topics'][1], t['topics'][2]) for t in transaction_logs['result']])
            if tr_data is not None and len(tr_data) > 0:
                tstamps = max([int(t['blockNumber'], 16) for t in transaction_logs['result']]) + 1
                self.start_logs = tstamps
                self.transaction_data |= tr_data
                time.sleep(5)
            else:
                have_extra = False

    @update_transparency_data.before_loop
    async def before_transparency(self):
        print('waiting...')
        await self.bot.wait_until_ready()
