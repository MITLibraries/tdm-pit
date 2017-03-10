import logging

import aiohttp
import rdflib

from pit.namespaces import BIBO, EBU, F4EV, MODS, MSL, PCDM, DCTERMS, RDF, RDA


def indexable(headers):
    return str(PCDM.Object) in headers['org.fcrepo.jms.resourceType'] and \
        str(F4EV.ResourceModification) in headers['org.fcrepo.jms.eventType']


async def resolve(uri):
    headers = {
        'Accept': 'text/n3',
        'Prefer': 'representation; include="http://fedora.info/definitions/v4'
                  '/repository#EmbedResources"'
    }
    r = await aiohttp.get('{}/fcr:metadata'.format(uri), headers=headers)
    return await r.text()


def uri_from_message(data, msg_format='json-ld'):
    g = rdflib.Graph().parse(data=data, format=msg_format)
    uri = g.value(subject=None, predicate=RDF.type, object=PCDM.Object,
                  any=False)
    return str(uri)


async def create_thesis(url):
    data = await resolve(url)
    graph = rdflib.Graph().parse(data=data, format='n3')
    t = ThesisResource(graph)
    full_text = await t.full_text
    return {
        'abstract': t.abstract,
        'advisor': t.advisor,
        'author': t.author,
        'copyright_date': t.copyright_date,
        'degree': t.degree,
        'department': t.department,
        'description': t.description,
        'handle': t.handle,
        'published_date': t.published_date,
        'title': t.title,
        'uri': t.uri,
        'full_text': full_text,
    }


class Indexer:
    def __init__(self, index, loop):
        self.index = index
        self.loop = loop

    async def on_message(self, frame):
        logger = logging.getLogger(__name__)
        if indexable(frame.headers):
            logger.debug('Processing message {}'
                         .format(frame.headers['message-id']))
            uri = uri_from_message(frame.body)
            try:
                thesis = await create_thesis(uri)
                await self.index.add(thesis)
                logger.info('Indexed {}'.format(uri))
            except Exception as e:
                logger.warn('Error while indexing document {}: {}'
                            .format(uri, e))


class PcdmFile(object):
    def __init__(self, graph):
        self.g = graph

    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=PCDM.File, any=False)

    @property
    def mimetype(self):
        return self.g.value(subject=self.uri, predicate=EBU.hasMimeType,
                            object=None, any=False)

    async def read(self):
        async with aiohttp.get(self.uri) as r:
            return await r.text()


class PcdmObject(object):
    def __init__(self, graph):
        self.g = graph

    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=PCDM.Object, any=False)

    @property
    def files(self):
        file_objects = []
        for o in self.g.objects(subject=self.uri, predicate=PCDM.hasFile):
            graph = rdflib.Graph()
            graph += self.g.triples((o, None, None))
            file_objects.append(PcdmFile(graph))
        return tuple(file_objects)


class ThesisResource(object):
    def __init__(self, graph):
        self.resource = PcdmObject(graph)

    @property
    def uri(self):
        return str(self.resource.uri)

    @property
    def abstract(self):
        return self._get(DCTERMS.abstract)

    @property
    def advisor(self):
        return self._get(RDA['60420'])

    @property
    def author(self):
        return self._get(DCTERMS.creator)

    @property
    def copyright_date(self):
        return self._get(DCTERMS.dateCopyrighted)

    @property
    def degree(self):
        return self._get(MSL.degreeGrantedForCompletion)

    @property
    def department(self):
        return self._get(MSL.associatedDepartment)

    @property
    def description(self):
        return self._get(MODS.note)

    @property
    def handle(self):
        return self._get(BIBO.handle)

    @property
    def published_date(self):
        return self._get(DCTERMS.issued)

    @property
    def title(self):
        return self._get(DCTERMS.title)

    @property
    async def full_text(self):
        for f in self.resource.files:
            if f.mimetype == 'text/plain':
                return await f.read()

    def _get(self, prop):
        return list(map(str, self.resource.g.objects(subject=self.resource.uri,
                                                     predicate=prop)))
