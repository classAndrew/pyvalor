import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
import datetime
import time
import sys
import math
from log import logger
import math

EXCEPTIONS = ["Titans Valor", "The Aquarium", "Avicia", "Empire of Sindria", "KongoBoys", "Paladins United", "Nerfuria", "Empire of Sindria", "Eden", "Idiot Co"]

class GXPTrackerTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)

    @staticmethod
    def level_to_xp(level):
        if level >= 130:
            return 885689 * math.exp(0.139808 * 130)
        return 885689 * math.exp(0.139808 * level)
    
    @staticmethod
    def xp_to_float_level(xp):
        return math.log(xp/885689)/0.139808

    @staticmethod
    def level_pct_to_float(level, pct):
        xp_to_curr = GXPTrackerTask.level_to_xp(level)
        xp_to_next = GXPTrackerTask.level_to_xp(level + 1)
        curr_xp = xp_to_curr + pct*(xp_to_next - xp_to_curr)
        
        return GXPTrackerTask.xp_to_float_level(curr_xp)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def gxp_tracker_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("GXP START")
                start = time.time()

                guild_names = Connection.execute("SELECT guild, priority FROM guild_autotrack_active ORDER BY priority DESC LIMIT 50;")
                
                existing = {g for g, _ in guild_names}
                for eg in EXCEPTIONS:
                    if eg not in existing:
                        guild_names.insert(0, (eg, 3000))
                
                res = Connection.execute("SELECT uuid, value FROM player_global_stats WHERE label='gu_gxp'")
                prev_member_gxps = {}
                for uuid, value in res:
                    prev_member_gxps[uuid] = value

                active_guild_rows = []
                for guild, priority in guild_names:
                    URL = f"https://api.wynncraft.com/v3/guild/{guild}"
                    g = await Async.get(URL)
                    if not "members" in g or not "level" in g:
                        continue    

                    guild_level, guild_percent = g["level"], g["xpPercent"] * 0.01
                    gu_float_lvl = GXPTrackerTask.level_pct_to_float(guild_level, guild_percent)
                    
                    if guild_level >= 130:
                        gu_req_to_next_xp = 10367116453807 # GXPTrackerTask.level_to_xp(131) - GXPTrackerTask.level_to_xp(130)
                    else:
                        gu_req_to_next_xp = GXPTrackerTask.level_to_xp(guild_level+1) - GXPTrackerTask.level_to_xp(guild_level)
                        
                    count_raid_threshold = 1/1.15 * gu_req_to_next_xp / 1000 / 4 # 1/1.15 in case it happened on lvl up boundary
                    
                    if "xpPercent" in g:
                        active_guild_rows.append((g["name"], priority, gu_float_lvl))
                    else:
                        logger.warn(f"guild {g['name']} does not have level or xpPercent info")

                    members = []
                    insert_gxp_deltas = []
                    update_gxp_values = []

                    insert_raid_deltas = []

                    for rank in g["members"]:
                        if type(g["members"][rank]) != dict: continue
                        for member_name in g["members"][rank]:
                            member_fields = g["members"][rank][member_name]
                            members.append({"name": member_name, **g["members"][rank][member_name]})
                            gxp_delta = member_fields["contributed"] - prev_member_gxps.get(member_fields["uuid"], member_fields["contributed"])
                            update_gxp_values.append((member_fields["uuid"], member_fields["contributed"]))

                            if gxp_delta > 0:
                                member_uuid = member_fields["uuid"]
                                insert_gxp_deltas.append((member_uuid, gxp_delta))

                    if len(insert_gxp_deltas) >= 3:
                        for member_uuid, gxp_delta in insert_gxp_deltas:
                            if guild_level >= 95 and gxp_delta >= count_raid_threshold and count_raid_threshold > 0:
                                num_raids = gxp_delta // count_raid_threshold
                                insert_raid_deltas.append((member_uuid, guild, start, num_raids))

                    if guild == "Titans Valor":

                        query = Connection.execute(f"SELECT * FROM user_total_xps")
                        uuid_to_xp = {x[4]: x[:4] for x in query} 

                        new_queries = []
                        new_members = []
                        record_xps = []

                        for m in members:
                            if m["uuid"] not in uuid_to_xp:
                                # New user
                                new_members.append(
                                    f"(\"{m['name']}\",{m['contributed']},{m['contributed']},\"Titans Valor\",\"{m['uuid']}\")"
                                )
                            elif m["contributed"] < uuid_to_xp[m["uuid"]][2]:
                                # User rejoins
                                new_xp = uuid_to_xp[m["uuid"]][1] + m["contributed"]
                                new_queries.append(
                                    f"UPDATE user_total_xps SET xp={new_xp}, last_xp={m['contributed']} WHERE uuid=\"{m['uuid']}\";")
                                record_xps.append(
                                    f"(\"{m['uuid']}\", \"{m['name']}\", \"Titans Valor\", {m['contributed']}, {int(time.time())})")
                            elif m["contributed"] > uuid_to_xp[m["uuid"]][2]:
                                # User gains xp
                                delta = m["contributed"] - uuid_to_xp[m["uuid"]][2]
                                new_xp = uuid_to_xp[m["uuid"]][1] + delta
                                new_queries.append(
                                    f"UPDATE user_total_xps SET xp={new_xp}, last_xp={m['contributed']} WHERE uuid=\"{m['uuid']}\";")
                                record_xps.append(
                                    f"(\"{m['uuid']}\", \"{m['name']}\", \"Titans Valor\", {delta}, {int(time.time())})")

                        if new_members:
                            Connection.execute(f"INSERT INTO user_total_xps VALUES {','.join(new_members)};")
                        # if record_xps:
                        #     Connection.execute(f"INSERT INTO member_record_xps VALUES {','.join(record_xps)};")
                        if new_queries:
                            Connection.exec_all(new_queries)

                    formatted_members = ','.join(f"(\'{guild}\', '{member['name']}')" for member in members)
                    update_members_query_1 = f"DELETE FROM guild_member_cache WHERE guild='{guild}'"
                    update_members_query_2 = f"INSERT INTO guild_member_cache (guild, name) VALUES {formatted_members}"
                    Connection.exec_all([update_members_query_1, update_members_query_2])

                    if insert_raid_deltas:
                        query = "INSERT INTO guild_raid_records VALUES " + ("(%s, %s, %s, %s),"*len(insert_raid_deltas))[:-1]
                        Connection.execute(query, prep_values=[y for x in insert_raid_deltas for y in x])

                    if insert_gxp_deltas:
                        query = "INSERT INTO player_delta_record VALUES " +\
                            ','.join(f"(\'{uuid}\', \'{guild}\', {start}, 'gu_gxp', {gxp_delta})" for uuid, gxp_delta in insert_gxp_deltas)
                        Connection.execute(query)
                    if update_gxp_values:
                        query = "REPLACE INTO player_global_stats VALUES " +\
                            ','.join(f"(\'{uuid}\', 'gu_gxp', {value})" for uuid, value in update_gxp_values)
                        Connection.execute(query)

                    if active_guild_rows:
                        query = "REPLACE INTO guild_autotrack_active (guild, priority, level) VALUES " + \
                            ("(%s, %s, %s),"*len(active_guild_rows))[:-1] + ';'
                        Connection.execute(query, active_guild_rows, prep_values=[y for x in active_guild_rows for y in x])

                    end = time.time()
                    
                    await asyncio.sleep(0.3)
                    
                logger.info("GXP TRACKER"+f" {end-start}s")
                await asyncio.sleep(self.sleep)
        
            logger.info("GXPTrackerTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(gxp_tracker_task))
