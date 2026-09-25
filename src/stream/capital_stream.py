"""
Capital.com WebSocket Streamer for Equities, Indices, and FX.
Connects to Capital.com Streaming API, maintains heartbeats, and streams real-time market quotes.
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
import websockets
from src.api.capital import _get_session

logger = logging.getLogger(__name__)

STREAMING_URL = "wss://api-streaming-capital.backend-capital.com/connect"
MAX_EPICS_PER_SUB = 40


class CapitalStreamer:
    """Consumes real-time market data quotes from Capital.com WebSockets."""
    def __init__(self, epics=None, on_tick_callback=None):
        self.epics = list(epics or ["AAPL", "NVDA", "TSLA", "SPY", "QQQ", "AMD", "AMZN", "MSFT"])
        self.on_tick_callback = on_tick_callback
        self.running = False
        self._cst = None
        self._sec_token = None
        self._session_time = 0

    def _refresh_auth(self) -> bool:
        """Obtains fresh authentication tokens from Capital.com."""
        session = _get_session()
        if not session or not session.get("CST") or not session.get("X-SECURITY-TOKEN"):
            logger.error("Failed to authenticate with Capital.com.")
            return False

        self._cst = session["CST"]
        self._sec_token = session["X-SECURITY-TOKEN"]
        self._session_time = time.time()
        logger.info("✅ Capital.com credentials refreshed.")
        return True

    async def _send_heartbeats(self, ws):
        """Sends periodic ping heartbeats to keep the connection alive."""
        while self.running:
            try:
                await asyncio.sleep(25)
                # If session is older than 8 minutes, refresh auth
                if time.time() - self._session_time > 480:
                    self._refresh_auth()

                ping_msg = {
                    "destination": "ping",
                    "cst": self._cst,
                    "securityToken": self._sec_token
                }
                await ws.send(json.dumps(ping_msg))
            except Exception as e:
                logger.debug(f"Capital heartbeat error: {e}")
                break

    async def _subscribe_epics(self, ws):
        """Subscribes to market data for configured epics in chunks of 40."""
        for i in range(0, len(self.epics), MAX_EPICS_PER_SUB):
            chunk = self.epics[i : i + MAX_EPICS_PER_SUB]
            sub_msg = {
                "destination": "marketData.subscribe",
                "correlationId": str(i + 1),
                "cst": self._cst,
                "securityToken": self._sec_token,
                "payload": {
                    "epics": chunk
                }
            }
            await ws.send(json.dumps(sub_msg))
            logger.info(f"Subscribed to Capital.com {len(chunk)} epics: {chunk[:5]}...")

    async def start(self):
        self.running = True
        backoff = 1

        while self.running:
            try:
                if not self._refresh_auth():
                    await asyncio.sleep(10)
                    continue

                logger.info(f"Connecting to Capital.com streaming endpoint...")
                async with websockets.connect(
                    STREAMING_URL,
                    ping_interval=30,
                    ping_timeout=15,
                    close_timeout=5
                ) as ws:
                    logger.info("✅ Connected to Capital.com WebSocket stream.")
                    backoff = 1

                    # Send subscription
                    await self._subscribe_epics(ws)

                    # Start background ping task
                    ping_task = asyncio.create_task(self._send_heartbeats(ws))

                    try:
                        while self.running:
                            msg = await ws.recv()
                            data = json.loads(msg)
                            dest = data.get("destination")

                            if dest == "quote":
                                payload = data.get("payload", {})
                                epic = payload.get("epic")
                                bid = payload.get("bid")
                                ofr = payload.get("ofr")
                                ts_ms = payload.get("timestamp")

                                if epic and bid is not None and ofr is not None and ts_ms:
                                    mid_price = (float(bid) + float(ofr)) / 2.0
                                    dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                                    tick = {
                                        "epic": epic,
                                        "price": mid_price,
                                        "bid": float(bid),
                                        "ask": float(ofr),
                                        "timestamp": dt_utc
                                    }

                                    if self.on_tick_callback:
                                        try:
                                            if asyncio.iscoroutinefunction(self.on_tick_callback):
                                                await self.on_tick_callback(tick)
                                            else:
                                                self.on_tick_callback(tick)
                                        except Exception as cb_err:
                                            logger.error(f"Capital callback error: {cb_err}")

                    finally:
                        ping_task.cancel()

            except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                if not self.running:
                    break
                logger.warning(f"Capital.com connection dropped ({e}). Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                if not self.running:
                    break
                logger.error(f"Unexpected Capital.com error: {e}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self):
        self.running = False
