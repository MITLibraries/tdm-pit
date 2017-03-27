import logging
import os.path
import tempfile
import uuid

import aiohttp
import rdflib

from pit.archive import archive
from pit.pcdm import PcdmObject, PREFER_HEADER


class DocumentSet:
    def __init__(self, members, client):
        self.members = members
        self.client = client

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            doc = self.members.pop(0)
        except IndexError:
            raise StopAsyncIteration
        res = await self.client.get(doc, headers={'Prefer': PREFER_HEADER,
                                                  'Accept': 'text/n3'})
        data = await res.text()
        graph = rdflib.Graph().parse(data=data, format='n3')
        return PcdmObject(graph, self.client)


async def create_package(url, session=None):
    session = session or aiohttp.ClientSession()
    tmp = tempfile.gettempdir()
    archive_name = os.path.join(tmp, uuid.uuid4().hex) + '.zip'
    res = await session.get(url)
    docset = await res.json()
    with archive(archive_name) as arxv:
        async for doc in DocumentSet(docset.get('members'), session):
            for f in doc.files:
                if f.mimetype == 'application/pdf':
                    r = await session.get(str(f.uri))
                    with tempfile.NamedTemporaryFile() as fp:
                        async for chunk in r.content.iter_chunked(1024):
                            fp.write(chunk)
                        arxv.write(fp.name, f.uri.split('/')[-1])
    return archive_name


class Packager:
    def __init__(self, bucket, session=None):
        self.bucket = bucket
        self.session = session or aiohttp.ClientSession()

    async def on_message(self, frame):
        logger = logging.getLogger(__name__)
        docset = frame.body.strip()
        try:
            arxv = await create_package(docset, self.session)
        except Exception as e:
            logger.error('Error creating package for docset {}: {}'
                         .format(docset, e))
            return
        try:
            size = os.stat(arxv).st_size
            blob = self.bucket.create(os.path.basename(arxv))
            await blob.upload(arxv)
            # notify API
        except Exception as e:
            logger.error('Error uploading package for docset {}: {}'
                         .format(docset, e))
        finally:
            os.remove(arxv)
