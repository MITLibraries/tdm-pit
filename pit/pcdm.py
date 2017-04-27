import rdflib

from pit import rewrite_host
from pit.namespaces import EBU, LDP, PCDM, RDF


PREFER_HEADER = 'return=representation; include="http://fedora.info/' \
                'definitions/v4/repository#EmbedResources"'


class PcdmBase:
    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=self.type, any=False)


def collection(graph):
    for item in graph.objects(subject=None, predicate=LDP.contains):
        yield str(item)


class PcdmObject(PcdmBase):
    type = PCDM.Object

    def __init__(self, graph, client):
        self.g = graph
        self.client = client

    @property
    def files(self):
        for o in self.g.objects(subject=None, predicate=PCDM.hasFile):
            graph = rdflib.Graph()
            graph += self.g.triples((o, None, None))
            yield PcdmFile(graph, self.client)


class PcdmFile(PcdmBase):
    type = PCDM.File

    def __init__(self, graph, client):
        self.g = graph
        self.client = client

    @property
    def mimetype(self):
        m = self.g.value(subject=self.uri, predicate=EBU.hasMimeType,
                         object=None, any=False)
        return str(m)

    async def read(self):
        url = rewrite_host(str(self.uri))
        return await self.client.get(url)
