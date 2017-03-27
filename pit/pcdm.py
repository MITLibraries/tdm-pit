import rdflib

from pit.namespaces import EBU, PCDM, RDF


PREFER_HEADER = 'return=representation; include="http://fedora.info/' \
                'definitions/v4/repository#EmbedResources"'


class PcdmBase:
    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=self.type, any=False)


class PcdmCollection(PcdmBase):
    type = PCDM.Collection

    def __init__(self, graph, client):
        self.g = graph
        self.client = client
        self._members = self.g.objects(subject=None, predicate=PCDM.hasMember)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            m = next(self._members)
        except StopIteration:
            raise StopAsyncIteration
        res = await self.client.get(str(m), headers={'Prefer': PREFER_HEADER,
                                                     'Accept': 'text/n3'})
        data = await res.text()
        graph = rdflib.Graph().parse(data=data, format='n3')
        return PcdmObject(graph, self.client)


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
        return await self.client.request(str(self.uri))
