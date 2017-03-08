import asyncio
import logging
import time

from pit.stomp import BrokenSocketError
from pit.index import Indexer


async def listen(client):
    while True:
        frame = await client.receive_frame()
        try:
            if frame.command == 'MESSAGE':
                token = client.message(frame)
                coro = client.subscription(token)
                asyncio.ensure_future(coro(frame))
            elif frame.command == 'RECEIPT':
                pass
            elif frame.command == 'ERROR':
                pass
        except AttributeError:
            # Heartbeat
            pass


async def heartbeat(client, period, multiplier=1.0, loop=None):
    grace_period = period * multiplier
    while True:
        last = client.lastReceived
        now = time.time()
        delta = now - last
        if period < delta <= grace_period:
            wait = grace_period - delta
        elif delta <= period:
            wait = period - delta
        else:
            loop.stop()
            return
        await asyncio.sleep(wait)


async def run(client, idx, loop):
    logger = logging.getLogger(__name__)
    idxer = Indexer(idx, loop)
    try:
        await client.subscribe('/queue/fedora', idxer.on_message)
        asyncio.ensure_future(heartbeat(client, 60, 2.5, loop))
        await listen(client)
    except BrokenSocketError:
        logger.warn('Socket unexpectedly closed')
        loop.stop()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error('Exception encountered: {}'.format(e))
        loop.stop()
    finally:
        await client.disconnect()
        logger.debug('Stomp client disconnected')


async def cancel_task(task):
    task.cancel()
    await task


def cleanup(tasks, loop, timeout=0):
    for task in tasks:
        try:
            loop.run_until_complete(asyncio.wait_for(cancel_task(task),
                                                     timeout))
        except asyncio.TimeoutError:
            logging.warn('Task cancellation timed out: {}'.format(task))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error('Exception while cancelling task {} {}'
                          .format(task, e))
