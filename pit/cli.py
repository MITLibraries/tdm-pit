import logging
import logging.config
import signal
import sys

import click
from elasticsearch_dsl.connections import connections
import stomp

from pit.index import (
    create_thesis,
    delete_from_index,
    delete_from_repo,
    DocumentIndexer,
    documents,
    Index,
    Thesis,
)
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
    idx = Index('theses', Thesis)
    idx.initialize()
    conn = stomp.Connection([(broker_host, broker_port)])
    conn.set_listener('indexer', DocumentIndexer('theses'))
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


@main.command()
@click.argument('collection')
@click.option('--index-host', default='localhost')
@click.option('--index-port', default=9200)
@click.option('--index-name', default='theses')
def reindex(collection, index_host, index_port, index_name):
    logger = logging.getLogger(__name__)
    es_conn = '{}:{}'.format(index_host, index_port)
    connections.create_connection(hosts=[es_conn], timeout=20)
    idx = Index(index_name, Thesis)
    idx.initialize()
    new = idx.new_version()
    for url in documents(collection):
        try:
            thesis = create_thesis(url)
            thesis.save(index=new)
            logger.info('Indexed document: {}'.format(url))
        except Exception as e:
            logger.warn('Error while indexing document {}: {}'.format(url, e))
    idx.current = new
    logger.info('Finished reindexing collection')


@main.command()
@click.argument('thesis')
@click.option('--index-host', default='localhost')
@click.option('--index-port', default=9200)
@click.option('--index-name', default='theses')
@click.option('--fedora-url', default='http://localhost/fcrepo/rest/')
def remove(thesis, index_host, index_port, index_name, fedora_url):
    logger = logging.getLogger(__name__)
    es_conn = '{}:{}'.format(index_host, index_port)
    connections.create_connection(hosts=[es_conn], timeout=20)
    hdl = 'http://hdl.handle.net/' + thesis
    repo_uri = fedora_url + thesis
    try:
        delete_from_index(hdl)
        logger.info('Deleted {} from elasticsearch'.format(thesis))
    except Exception as e:
        logger.warn('Could not delete document from index: {}'.format(e))
        sys.exit(0)
    try:
        delete_from_repo(repo_uri)
        logger.info('Deleted {} from fedora'.format(repo_uri))
    except Exception as e:
        logger.warn('Could not delete document from fedora: {}'.format(e))
        sys.exit(0)
