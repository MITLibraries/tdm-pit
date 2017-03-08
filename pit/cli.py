import asyncio
import logging
import logging.config
import signal
import sys

from aioes import Elasticsearch
import click

from pit.es import Index
from pit.stomp import Protocol
from pit.worker import run as _run, cleanup
from pit.logging import BASE_CONFIG


@click.group()
def main():
    logging.config.dictConfig(BASE_CONFIG)


@main.command()
@click.option('--broker-host', default='localhost')
@click.option('--broker-port', default=61613)
@click.option('--index-host', default='localhost')
@click.option('--index-port', default=9200)
@click.option('--repo-host', default='localhost')
@click.option('--repo-port', default=80)
@click.option('--queue', default='/queue/fedora')
def run(broker_host, broker_port, index_host, index_port, repo_host,
        repo_port, queue):
    logger = logging.getLogger(__name__)
    es_conn = "{}:{}".format(index_host, index_port)
    loop = asyncio.get_event_loop()
    es = Elasticsearch([es_conn], loop=loop)
    idx = Index(es, 'theses')
    loop.run_until_complete(idx.initialize())
    logger.debug('Connected to Elasticsearch on {}'.format(es_conn))

    stomp = Protocol('localhost', 61613, loop)
    try:
        loop.run_until_complete(
            asyncio.wait_for(stomp.connect(), timeout=5, loop=loop))
    except asyncio.TimeoutError:
        logger.error('Connecting to ActiveMQ timed out')
        sys.exit(0)
    except Exception as e:
        logger.error('Exception while connecting to ActiveMQ: {}'.format(e))
        sys.exit(0)
    logger.info('Connected to ActiveMQ')
    asyncio.ensure_future(_run(stomp, idx, loop))
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), loop.stop)
    try:
        loop.run_forever()
    finally:
        logger.info('Cleaning up before loop exit')
        tasks = asyncio.Task.all_tasks()
        cleanup(tasks, loop, timeout=5)
        loop.close()
