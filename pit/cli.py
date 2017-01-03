import logging
import logging.config
import signal
import sys

import click
from elasticsearch_dsl.connections import connections
from rdflib import Graph
import stomp

from pit.index import Thesis, indexable, ThesisResource
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
    connections.create_connection(hosts=[es_conn], timeout=20)
    logger.debug('Connected to Elasticsearch on {}'.format(es_conn))
    Thesis.init()
    conn = stomp.Connection([(broker_host, broker_port)])
    conn.set_listener('indexer', FedoraListener())
    conn.start()
    conn.connect(wait=True)
    logger.debug('Connected to ActiveMQ on {}:{}'.format(broker_host,
                                                         broker_port))
    conn.subscribe(queue, 1)

    def shutdown(signum, stack):
        conn.disconnect()
        logger.debug('Disconnected from ActiveMQ')
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)

    while True:
        signal.pause()


class FedoraListener(stomp.ConnectionListener):
    def on_message(self, headers, message):
        logger = logging.getLogger(__name__)
        if indexable(headers):
            logger.debug('Processing message {}'\
                .format(headers['message-id']))
            handle_message(message)


def handle_message(message):
    logger = logging.getLogger(__name__)
    t = ThesisResource(Graph().parse(data=message, format='json-ld'))
    Thesis(
        uri=t.uri,
        title=t.title,
        department=t.department,
        reviewer=t.reviewer,
        author=t.author,
        issue_date=t.issue_date,
        copyright_date=t.copyright_date,
        abstract=t.abstract,
        full_text=t.full_text
    ).save()
    logger.info('Indexed {}'.format(t.resource.uri))
