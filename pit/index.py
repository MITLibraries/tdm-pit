import asyncio
from functools import partial
import logging

import aiohttp
import rdflib

from pit import rewrite_host
from pit.namespaces import BIBO, F4EV, MODS, MSL, PCDM, DCTERMS, RDF, RDA
from pit.pcdm import PREFER_HEADER, PcdmObject, collection


def indexable(headers):
    return str(PCDM.Object) in headers['org.fcrepo.jms.resourceType'] and \
        str(F4EV.ResourceModification) in headers['org.fcrepo.jms.eventType']


def uri_from_message(data, msg_format='json-ld'):
    g = rdflib.Graph().parse(data=data, format=msg_format)
    uri = g.value(subject=None, predicate=RDF.type, object=PCDM.Object,
                  any=False)
    return str(uri)


async def create_thesis(url, client=None):
    client = aiohttp.ClientSession()
    url = rewrite_host(url)
    resp = await client.get(url, headers={'Prefer': PREFER_HEADER,
                                          'Accept': 'text/n3'})
    data = await resp.text()
    graph = rdflib.Graph().parse(data=data, format='n3')
    t = ThesisResource(graph, client)
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


async def index_collection(coll_url, index, client=None):
    logger = logging.getLogger(__name__)
    client = client or aiohttp.ClientSession()
    ex = CoroExecutor(size=10)
    idx = partial(index_thesis, index)
    resp = await client.get(coll_url, headers={'Prefer': PREFER_HEADER,
                                               'Accept': 'text/n3'})
    data = await resp.text()
    graph = rdflib.Graph().parse(data=data, format='n3')
    fs = ex.map(idx, collection(graph))
    for fut in asyncio.as_completed(fs):
        try:
            res = await fut
            logger.info('Indexed {}'.format(res))
        except Exception as e:
            logger.warn(e)


async def index_thesis(index, url):
    thesis = await create_thesis(url)
    await index.add(thesis)
    return url


class Indexer:
    def __init__(self, index, loop=None):
        self.index = index
        self.loop = loop or asyncio.get_event_loop()

    async def on_message(self, frame):
        logger = logging.getLogger(__name__)
        if indexable(frame.headers):
            logger.debug('Processing message {}'
                         .format(frame.headers['message-id']))
            uri = uri_from_message(frame.body)
            try:
                await index_thesis(self.index, uri)
                logger.info('Indexed {}'.format(uri))
            except Exception as e:
                logger.warn('Error while indexing document {}: {}'
                            .format(uri, e))


class ThesisResource(object):
    def __init__(self, graph, client):
        self.resource = PcdmObject(graph, client)

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
                resp = await f.read()
                return await resp.text()

    def _get(self, prop):
        return list(map(str, self.resource.g.objects(subject=self.resource.uri,
                                                     predicate=prop)))


class CoroExecutor:
    def __init__(self, size=1):
        self.semaphore = asyncio.BoundedSemaphore(value=size)

    def submit(self, func, *args, **kwargs):
        return asyncio.ensure_future(self._run(func, *args, **kwargs))

    def map(self, func, iterable, timeout=None):
        return [self.submit(func, arg) for arg in iterable]

    async def _run(self, func, *args, **kwargs):
        async with self.semaphore:
            return await func(*args, **kwargs)
