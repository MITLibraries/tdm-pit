import click
import stomp


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
    conn = stomp.Connection([(broker_host, broker_port)])
    conn.set_listener('indexer', FedoraListener())
    conn.start()
    conn.connect(wait=True)
    conn.subscribe(queue, 1)
    while True:
        pass


class FedoraListener(stomp.ConnectionListener):
    def on_message(self, headers, message):
        print(message)
        # process message
        pass
