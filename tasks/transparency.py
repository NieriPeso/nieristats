import asyncio
import datetime
import io
import re
import time

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

SOURCE_WALLETS = [int(w,16) for w in ["0x946d811bF9ff3AccC49555F0756CaC7f043C41a4",
                                   "0xe6d16e8300271cdd76434d17e68534c606835A3E",
                                   "0x4e39cB2e26358Bb1EBeD9823a856E18514fc58E5",
                                   "0xaE8B57B0873de9f1a7049046901D0C3Ad4334A99"]]


async def calculate_graph(td):
    G = nx.DiGraph()
    for (s, r, ts, val) in set(td):
        G.add_edge(s, r)
    G_labels = {s: (hex(s)[:5] + "...") for s in
                G.nodes}
    return G, G_labels


class TransparencyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.channel = 862455112940126241
        self.bot = bot
        self.update_transparency_data.start()
        self.start_logs = 7579130
        self.transaction_data = set()
        self.transparent_channel: discord.TextChannel = None

    def cog_unload(self):
        self.update_transparency_data.cancel()

    @tasks.loop(minutes=1)
    async def update_transparency_data(self):
        if self.transparent_channel is None:
            self.transparent_channel = self.bot.get_channel(self.channel)
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
        ax22.plot(data_x, data_y_marketval, label='Cantidad de NIERIs transferidos en el día', color='r')
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
        await self.transparent_channel.send(
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

    async def wallet_transparency_messages(self, date=None):
        if date is None:
            date = (datetime.date.today() - datetime.timedelta(days=1))
        node_colors, node_sizes, sG_labels, susp_G, suspiciousness_table = await \
            self.calculate_suspicious_wallets()
        bytes_stream = await self.graph_to_image(susp_G, sG_labels, node_sizes, node_colors)
        chart = discord.File(bytes_stream, filename='billeteras_sospechosas.png')
        embed = discord.Embed(title='Billeteras sospechosas')
        embed.set_image(url='attachment://billeteras_sospechosas.png')
        susp_tab = list(suspiciousness_table.items())
        susp_tab.sort(key=lambda x: -(x[1]))
        for (sender, count) in susp_tab[:10]:
            embed.add_field(name=hex(sender), value=count)
        await self.transparent_channel.send(embed=embed, file=chart)

        td = list(filter(lambda z: z[2].date() == date, self.transaction_data))

        sender_wallets = await self.calculate_stats(td)

        statistics_table_content = tabulate.tabulate(
            [[hex(sender), destc, recvc, mcapc] for (sender, destc, recvc, mcapc) in
             sender_wallets[:10]], headers=['Billeteras', 'Destinos', 'Orígenes', 'Valor total de transacciones'],
            tablefmt='github'
        )

        G, G_labels = await calculate_graph(td)
        bytes_stream = await self.graph_to_image(G, G_labels, 50, '#1f78b4')

        chart = discord.File(bytes_stream, 'billeteras_dia.png')
        embed = discord.Embed(title='Estadísticas de transacciones del ' + str(date),
                              description=statistics_table_content)

        embed.set_image(url='attachment://billeteras_dia.png')
        await self.transparent_channel.send(embed=embed, file=chart)

    async def calculate_stats(self, td):
        recv = {}
        dest = {}
        mcap = {}
        for (fr, to, tstamp, val) in td:
            if fr not in recv:
                recv[fr] = 0
            recv[fr] += 1
            if to not in dest:
                dest[to] = 0
            dest[to] += 1

            if fr not in mcap:
                mcap[fr] = 0
            mcap[fr] += val
            if to not in mcap:
                mcap[to] = 0
            mcap[to] += val
        sender_wallets = [(k, recv.get(k) or 0, dest.get(k) or 0, mcap.get(k) or 0) for k in set(mcap.keys())]
        sender_wallets.sort(key=lambda x: -x[3])
        return sender_wallets

    async def graph_to_image(self, susp_G, sG_labels, node_sizes, node_colors):
        plt.figure(figsize=(6, 6))
        nx.draw_spring(susp_G, node_size=node_sizes, node_color=node_colors, with_labels=False, labels=sG_labels)
        bytes_stream = io.BytesIO()
        plt.savefig(bytes_stream, format='png', bbox_inches="tight", dpi=80)
        plt.close()
        bytes_stream.seek(0)
        return bytes_stream

    async def calculate_suspicious_wallets(self):
        G, G_labels = await calculate_graph(list(self.transaction_data))

        node_colors, node_sizes, sG_labels, \
        susp_G, suspiciousness_table = await self.suspicious_wallets_graph(
            G, G_labels)
        return node_colors, node_sizes, sG_labels, susp_G, suspiciousness_table

    async def suspicious_wallets_graph(self, G, G_labels):
        susp_wallets_senders = []
        for n in G.nodes:
            pres = list(G.predecessors(n))
            suspicious = len([c for c in susp_wallets_senders + SOURCE_WALLETS if c in pres]) > 0 and len(list(G.successors(n))) < SENDER_SUSPICION_THRESHOLD
            if suspicious:
                susp_wallets_senders.append(n)
        susp_wallets_receivers = set(sum([list(G.successors(n)) for n in susp_wallets_senders], []))
        suspiciousness_table = {}
        susp_send = {}
        for r in susp_wallets_receivers:
            sus = set([w for w in G.predecessors(r) if w in set(susp_wallets_senders)])
            suspiciousness_table[r] = len(sus)
            susp_send[r] = sus
        for s in susp_wallets_senders:
            sus = set([w for w in G.predecessors(s) if w in set(SOURCE_WALLETS)])
            susp_send[s] = sus
        susp_G = nx.DiGraph()
        for r in susp_send:
            for s in susp_send[r]:
                susp_G.add_edge(s, r)
        node_sizes = [((((suspiciousness_table[n] / max(
            suspiciousness_table.values())) / 2) + 0.5 if n in suspiciousness_table else 0)) * 200 + 50 for n in
                      susp_G.nodes()]
        sG_labels = {k: v for (k, v) in G_labels.items() if k in susp_G.nodes}
        node_colors = ['g' if n in SOURCE_WALLETS else 'r' if n in suspiciousness_table else 'b' for n in
                       susp_G.nodes()]
        return node_colors, node_sizes, sG_labels, susp_G, suspiciousness_table

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
            tr_data = set([(int(t['topics'][1], 16), int(t['topics'][2], 16),
                            datetime.datetime.fromtimestamp(int(t['timeStamp'], 16)),
                            int(t['data'], 16)) for t in
                           transaction_logs['result']])
            if tr_data is not None and len(tr_data - self.transaction_data) > 0:
                tstamps = max([int(t['blockNumber'], 16) for t in transaction_logs['result']])
                self.start_logs = tstamps
                self.transaction_data |= tr_data
                time.sleep(5)
            else:
                have_extra = False

    @update_transparency_data.before_loop
    async def before_transparency(self):
        print('waiting...')
        await self.bot.wait_until_ready()
