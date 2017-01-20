import pytest
from rdflib import Graph, URIRef, Literal
import requests_mock

from pit.index import PcdmObject, indexable, ThesisResource
from pit.namespaces import *


@pytest.fixture(scope="session")
def n3():
    with open('tests/fixtures/thesis.n3') as fp:
        n3 = fp.read()
    return n3


@pytest.yield_fixture(autouse=True)
def fedora(n3):
    with requests_mock.Mocker() as m:
        m.get('mock://example.com/1/fcr:metadata', text=n3)
        yield


@pytest.fixture
def graph():
    g = Graph()
    g.add((URIRef('mock://example.com/1'), RDF.type, PCDM.Object))
    return g


def test_pcdm_object_loads_remote_graph(graph):
    o = PcdmObject(graph)
    assert (URIRef('mock://example.com/1'), DCTERMS.title,
            Literal("Title 1")) in o.g


def test_pcdm_object_has_uri(graph):
    o = PcdmObject(graph)
    assert o.uri == URIRef('mock://example.com/1')


def test_pcdm_object_returns_list_of_files(graph):
    o = PcdmObject(graph)
    files = o.files
    assert len(o.files) == 2
    assert URIRef('mock://example.com/bar') in [f.uri for f in files]


def test_indexable_returns_true_for_indexable_events():
    assert indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#Object',
        'org.fcrepo.jms.eventType': 'http://fedora.info/definitions/v4/'
                                    'event#ResourceModification'})


def test_indexable_returns_false_for_nonindexable_events():
    assert not indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#File'})


def test_thesis_properties_return_mutliple():
    s = URIRef('mock://example.com/1')
    g = Graph()
    g.add((s, RDF.type, PCDM.Object))
    t = ThesisResource(g)
    assert all([m in t.title for m in ['Title 1', 'Title 2']])


def test_thesis_resource_returns_properties():
    g = Graph()
    g.add((URIRef('mock://example.com/1'), RDF.type, PCDM.Object))
    t = ThesisResource(g)
    assert 'This is an abstract' in t.abstract
    assert 'Baz, Foo' in t.advisor
    assert 'Bar' in t.author
    assert '2001' in t.copyright_date
    assert 'Engineering' in t.degree
    assert 'Comp Sci' in t.department
    assert 'This is a thesis' in t.description
    assert 'http://handle.org/1' in t.handle
    assert '2002' in t.published_date
    assert 'Title 1' in t.title
    assert t.uri == 'mock://example.com/1'
