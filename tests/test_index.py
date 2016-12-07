from unittest.mock import patch

import pytest
from rdflib import Graph, URIRef, Literal

from pit.index import PcdmObject, indexable
from pit.namespaces import *


resource = URIRef('http://example.com/foo')


@pytest.yield_fixture
def resolve():
    with patch('pit.index.resolve') as mock:
        mock.return_value = \
        '<http://example.com/foo> <http://purl.org/dc/terms/title> "Foobar" .'
        yield mock


@pytest.fixture
def graph():
    g = Graph()
    g.add((resource, RDF.type, PCDM.Object))
    return g


def test_pcdm_object_loads_remote_graph(graph, resolve):
    o = PcdmObject(graph)
    assert (resource, DCTERMS.title, Literal("Foobar")) in o.g


def test_pcdm_object_has_uri(graph, resolve):
    o = PcdmObject(graph)
    assert o.uri == URIRef('http://example.com/foo')


def test_pcdm_object_returns_list_of_files(graph, resolve):
    o = PcdmObject(graph)
    o.g += Graph().parse(data="""
        @prefix pcdm: <http://pcdm.org/models#> .
        <http://example.com/bar> a pcdm:File .
        <http://example.com/baz> a pcdm:File .
        <http://example.com/foo> pcdm:hasFile
            <http://example.com/bar>, <http://example.com/baz> .""", format='n3')
    files = o.files
    assert len(o.files) == 2
    assert URIRef('http://example.com/bar') in [f.uri for f in files]


def test_indexable_returns_true_for_indexable_events():
    assert indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#Object',
        'org.fcrepo.jms.eventType': 'http://fedora.info/definitions/v4/'
                                    'event#ResourceModification'})


def test_indexable_returns_false_for_nonindexable_events():
    assert not indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#File'})
