import signal

import click
from elasticsearch_dsl.connections import connections
from rdflib import Graph
import stomp

from pit.index import Thesis, indexable, ThesisResource


@click.group()
def main():
    pass


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
    es_conn = "{}:{}".format(index_host, index_port)
    connections.create_connection(hosts=[es_conn], timeout=20)
    Thesis.init()
    conn = stomp.Connection([(broker_host, broker_port)])
    conn.set_listener('indexer', FedoraListener())
    conn.start()
    conn.connect(wait=True)
    conn.subscribe(queue, 1)
    while True:
        signal.pause()


class FedoraListener(stomp.ConnectionListener):
    def on_message(self, headers, message):
        if indexable(headers):
            handle_message(message)


def handle_message(message):
    t = ThesisResource(Graph().parse(data=message, format='ld-json'))
    Thesis(
        title=t.title,
        department=t.department,
        reviewer=t.reviewer,
        author=t.author,
        issue_date=t.issue_date,
        copyright_date=t.copyright_date,
        abstract=t.abstract,
        full_text=t.full_text
    ).save()
