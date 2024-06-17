import time
import asyncio
import dateutil.parser
from urllib.parse import quote, unquote
from typing import Any, Tuple, Optional, Dict, List

import aiohttp
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered
from pyrogram.raw.functions.messages import RequestWebView

from bot.utils import logger
from bot.exceptions import InvalidSession
from .headers import headers
from bot.config import settings


class Miner:
    def __init__(self, tg_client: Client):
        self.session_name = tg_client.name
        self.tg_client = tg_client

        self.toolkit_levels = []
        self.workbench_levels = []

    async def get_tg_web_data(self, proxy: str | None) -> str:
        try:
            if proxy:
                proxy = Proxy.from_str(proxy)
                proxy_dict = dict(
                    scheme=proxy.protocol,
                    hostname=proxy.host,
                    port=proxy.port,
                    username=proxy.login,
                    password=proxy.password
                )
            else:
                proxy_dict = None

            self.tg_client.proxy = proxy_dict

            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            web_view = await self.tg_client.invoke(RequestWebView(
                peer=await self.tg_client.resolve_peer('Marswallet_bot'),
                bot=await self.tg_client.resolve_peer('Marswallet_bot'),
                platform='android',
                from_bot_menu=True,
                url='https://zavod.mdaowallet.com/'
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(
                    string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0]))

            res = ''
            params = ['query_id', 'user', 'auth_date', 'hash']
            qparams = tg_web_data.split('&')
            for param in qparams:
                vals = param.split('=')
                if vals[0] in params:
                    if vals[0] == 'user':
                        res += f"{vals[0]}={quote(vals[1])}&"
                    else:
                        res += f"{vals[0]}={vals[1]}&"

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return res[:-1]

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error during Authorization: {error}")
            await asyncio.sleep(delay=7)

    async def check_proxy(self, http_client: aiohttp.ClientSession, proxy: Proxy) -> None:
        try:
            response = await http_client.get(url='https://httpbin.org/ip', timeout=aiohttp.ClientTimeout(5))
            ip = (await response.json()).get('origin')
            logger.info(f"{self.session_name} | Proxy IP: {ip}")
        except Exception as error:
            logger.error(f"{self.session_name} | Proxy: {proxy} | Error: {error}")

    async def profile(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.get(
                url='https://zavod-api.mdaowallet.com/user/profile',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            profile_info = response_json

            return profile_info
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error getting profile: {error}")
            await asyncio.sleep(delay=7)

    async def farm(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.get(
                url='https://zavod-api.mdaowallet.com/user/farm',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            farm_info = response_json

            return farm_info
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting farm: {error}")
            await asyncio.sleep(delay=7)

    async def toolkit_settings(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.get(
                url='https://zavod-api.mdaowallet.com/farm/toolkitSettings',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            settings = response_json

            return settings
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while daily getting toolkit settings: {error}")
            await asyncio.sleep(delay=7)

    async def workbench_settings(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.get(
                url='https://zavod-api.mdaowallet.com/farm/workbenchSettings',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            settings = response_json

            return settings
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while daily getting workbench settings: {error}")
            await asyncio.sleep(delay=7)

    async def claim(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.post(
                url='https://zavod-api.mdaowallet.com/user/claim',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            claim_info = response_json

            return claim_info
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while claiming: {error}")
            await asyncio.sleep(delay=7)

    async def upgrade_speed(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.post(
                url='https://zavod-api.mdaowallet.com/user/upgradeWorkbench',
                json={})
            response.raise_for_status()

            response_json = await response.json()

            return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error upgrading speed (workbench): {error}")
            await asyncio.sleep(delay=7)

    async def upgrade_storage(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        try:
            response = await http_client.post(
                url='https://zavod-api.mdaowallet.com/user/upgradeToolkit',
                json={})
            response.raise_for_status()

            response_json = await response.json()

            return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error upgrading storage (toolkit): {error}")
            await asyncio.sleep(delay=7)

    def is_claim_possible(self, farm_info: Dict[str, Any]) -> bool:
        if not farm_info:
            return False

        last_claim = farm_info['lastClaim']
        if not last_claim:
            return False

        last_claim_timestamp = dateutil.parser.parse(last_claim).timestamp()
        if not farm_info.get('claimInterval'):
            return False

        claim_interval: int = int(farm_info['claimInterval'])
        if claim_interval < 0:
            return False

        timestamp_to_claim = claim_interval / 1000 + last_claim_timestamp
        if time.time() >= timestamp_to_claim:
            return True

        return False

    def get_sleep_time_to_claim(self, farm_info: Dict[str, Any]) -> int:
        if not farm_info:
            return settings.DEFAULT_SLEEP

        last_claim = farm_info['lastClaim']
        if not last_claim:
            return settings.DEFAULT_SLEEP

        last_claim_timestamp = dateutil.parser.parse(last_claim).timestamp()
        if not farm_info.get('claimInterval'):
            return settings.DEFAULT_SLEEP

        claim_interval: int = int(farm_info['claimInterval'])
        if claim_interval < 0:
            return settings.DEFAULT_SLEEP

        timestamp_to_claim = claim_interval / 1000 + last_claim_timestamp
        res = int(timestamp_to_claim - time.time())
        if res < 0:
            return settings.DEFAULT_SLEEP

        return res

    def get_speed_level_upgrade_price(self, level: int) -> int:
        if not self.workbench_levels:
            return -1

        for w in self.workbench_levels:
            if w['level'] == level:
                return w['price']
        return -2

    def get_storage_level_upgrade_price(self, level: int) -> int:
        if not self.toolkit_levels:
            return -1

        for t in self.toolkit_levels:
            if t['level'] == level:
                return t['price']
        return -2

    async def run(self, proxy: str | None) -> None:
        logged_in = False
        sleep_time = settings.DEFAULT_SLEEP
        proxy_conn = ProxyConnector().from_url(proxy) if proxy else None

        async with (aiohttp.ClientSession(headers=headers, connector=proxy_conn) as http_client):
            if proxy:
                await self.check_proxy(http_client=http_client, proxy=proxy)

            while True:
                try:
                    if not logged_in:
                        tg_web_data = await self.get_tg_web_data(proxy=proxy)
                        logged_in = True

                        http_client.headers["Telegram-Init-Data"] = tg_web_data
                        headers["Telegram-Init-Data"] = tg_web_data

                    profile_info = await self.profile(http_client=http_client)
                    if not profile_info:
                        logger.error(f"{self.session_name} | Cannot get profile info")
                    else:
                        balance = profile_info['tokens']
                        logger.info(f"{self.session_name} | Balance is <c>{balance: .6f}</c>")

                        self.toolkit_levels = await self.toolkit_settings(http_client=http_client)
                        self.workbench_levels = await self.workbench_settings(http_client=http_client)
                        farm_info = await self.farm(http_client=http_client)

                        can_claim = self.is_claim_possible(farm_info=farm_info)
                        if can_claim:
                            claim_info = await self.claim(http_client=http_client)
                            balance = claim_info['tokens']
                            logger.info(f"{self.session_name} | Claimed successfully, new balance is <c>{balance: .6f}</c>")

                            farm_info = await self.farm(http_client=http_client)
                            sleep_time = self.get_sleep_time_to_claim(farm_info=farm_info)

                        if settings.UPGRADE_SPEED:
                            next_level = farm_info['workbenchLevel'] + 1
                            if settings.SPEED_MAX_LEVEL >= next_level:
                                w_price = self.get_speed_level_upgrade_price(level=next_level)
                                if w_price == -1:
                                    logger.error(f"{self.session_name} | Cannot upgrade speed (workbench), error in settings")
                                elif w_price == -2:
                                    logger.info(f"{self.session_name} | Speed (workbench) upgraded to maximum level")
                                else:
                                    if balance >= w_price:
                                        logger.info(f"{self.session_name} | Speed (workbench) upgrade is possible, trying to upgrade")
                                        w_upgrade_info = await self.upgrade_speed(http_client=http_client)
                                        if w_upgrade_info:
                                            profile_info = await self.profile(http_client=http_client)
                                            balance = profile_info['tokens']
                                            w_level = w_upgrade_info['workbenchLevel']
                                            logger.success(f"{self.session_name} | Speed (workbench) upgraded successfully to level <c>{w_level}</c>, new balance is <c>{balance: .6f}</c>")
                                            sleep_time = self.get_sleep_time_to_claim(farm_info=w_upgrade_info)
                                    else:
                                        logger.info(f"{self.session_name} | Cannot upgrade speed (workbench), not enough tokens")
                            else:
                                logger.info(f"{self.session_name} | Speed (workbench) upgraded to maximum level due settings")

                        if settings.UPGRADE_STORAGE:
                            next_level = farm_info['toolkitLevel'] + 1
                            if settings.STORAGE_MAX_LEVEL >= next_level:
                                t_price = self.get_storage_level_upgrade_price(level=next_level)
                                if t_price == -1:
                                    logger.error(f"{self.session_name} | Cannot upgrade storage (toolkit), error in settings")
                                elif t_price == -2:
                                    logger.info(f"{self.session_name} | Storage (toolkit) upgraded to maximum level")
                                else:
                                    if balance >= t_price:
                                        logger.info(f"{self.session_name} | Storage (toolkit) upgrade is possible, trying to upgrade")
                                        t_upgrade_info = await self.upgrade_storage(http_client=http_client)
                                        if t_upgrade_info:
                                            profile_info = await self.profile(http_client=http_client)
                                            balance = profile_info['tokens']
                                            t_level = t_upgrade_info['toolkitLevel']
                                            logger.success(f"{self.session_name} | Storage (toolkit) upgraded successfully to level <c>{t_level}</c>, new balance is <c>{balance: .6f}</c>")
                                            sleep_time = self.get_sleep_time_to_claim(farm_info=t_upgrade_info)
                                    else:
                                        logger.info(f"{self.session_name} | Cannot upgrade storage (toolkit), not enough tokens")
                            else:
                                logger.info(f"{self.session_name} | Storage (toolkit) upgraded to maximum level due settings")

                except InvalidSession as error:
                    raise error

                except Exception as error:
                    logger.error(f"{self.session_name} | Unknown error: {error}")
                    await asyncio.sleep(delay=7)

                else:
                    logger.info(f"{self.session_name} | Sleeping for the next claim {sleep_time}s")
                    await asyncio.sleep(delay=sleep_time)


async def run_miner(tg_client: Client, proxy: str | None):
    try:
        await Miner(tg_client=tg_client).run(proxy=proxy)
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")