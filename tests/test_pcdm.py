import aiohttp
from tests import air_mock
import pytest
import rdflib

from pit.pcdm import collection, PcdmObject, PcdmFile


def test_collection_yields_objects(theses):
    g = rdflib.Graph().parse(data=theses, format='n3')
    items = list(collection(g))
    assert 'mock://example.com/theses/1' in items
    assert 'mock://example.com/theses/2' in items


def test_object_has_uri(thesis_1):
    g = rdflib.Graph().parse(data=thesis_1, format='n3')
    assert PcdmObject(g, None).uri == \
        rdflib.URIRef('mock://example.com/theses/1')


def test_object_iterates_over_files(thesis_1):
    g = rdflib.Graph().parse(data=thesis_1, format='n3')
    o = PcdmObject(g, None)
    files = list(o.files)
    uris = [str(f.uri) for f in files]
    assert 'mock://example.com/theses/1/1.pdf' in uris
    assert 'mock://example.com/theses/1/1.txt' in uris


def test_file_has_mimetype(thesis_1):
    o = rdflib.Graph().parse(data=thesis_1, format='n3')
    g = rdflib.Graph()
    g += o.triples((rdflib.URIRef('mock://example.com/theses/1/1.pdf'), None,
                    None))
    f = PcdmFile(g, None)
    assert f.mimetype == 'application/pdf'
