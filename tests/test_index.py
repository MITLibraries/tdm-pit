from operator import attrgetter
from unittest.mock import MagicMock

from elasticsearch_dsl.connections import connections
import pytest
from rdflib import Graph, URIRef
import requests_mock

from pit.index import (
    create_thesis,
    delete_from_index,
    delete_from_repo,
    documents,
    Index,
    indexable,
    PcdmObject,
    Thesis,
    ThesisResource,
    uri_from_message,
)


@pytest.fixture(scope="session")
def n3():
    with open('tests/fixtures/thesis.n3') as fp:
        n3 = fp.read()
    return n3


@pytest.fixture
def graph(n3):
    g = Graph().parse(data=n3, format='n3')
    return g


@pytest.yield_fixture
def es():
    client = MagicMock()
    connections.add_connection('default', client)
    yield client
    connections.remove_connection('default')


def test_uri_from_message_returns_uri():
    msg = ('{"@id": "mock://example.com/1", "@type": ['
           '"http://pcdm.org/models#Object"]}')
    assert uri_from_message(msg) == 'mock://example.com/1'


def test_create_thesis_loads_data_remotely(n3):
    with requests_mock.Mocker() as m:
        m.get('mock://example.com/1/fcr:metadata', text=n3)
        t = create_thesis('mock://example.com/1')
        assert t.abstract == ['This is an abstract']


def test_documents_generates_list_pcdm_members():
    coll = """
        @prefix pcdm: <http://pcdm.org/models#> .
        <mock://example.com/> pcdm:hasMember <mock://example.com/1> ,
                                             <mock://example.com/2> .
    """
    with requests_mock.Mocker() as m:
        m.get('mock://example.com/', text=coll)
        assert sorted(documents('mock://example.com/')) == \
            ['mock://example.com/1', 'mock://example.com/2']


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


def test_delete_from_index_deletes_document(es):
    es.search.return_value = {'hits': {'hits': [{'_id': 'foobar'}]}}
    delete_from_index('123.4', 'theses')
    es.search.assert_called_with('theses', 'thesis',
                                 {'query': {'term': {'handle': '123.4'}}})
    es.delete.assert_called_with('theses', 'thesis', 'foobar')


def test_delete_from_repo_deletes_thesis_and_files(n3):
    with requests_mock.Mocker() as m:
        m.get('mock://example.com/1/fcr:metadata', text=n3)
        m.delete('mock://example.com/bar')
        m.delete('mock://example.com/baz')
        m.delete('mock://example.com/1')
        delete_from_repo('mock://example.com/1')
        r_getter = attrgetter('method', 'url')
        reqs = [r_getter(r) for r in m.request_history]
        assert all([r in reqs for r in [
                    ('DELETE', 'mock://example.com/bar'),
                    ('DELETE', 'mock://example.com/baz'),
                    ('DELETE', 'mock://example.com/1')]])


def test_thesis_properties_return_mutliple(graph):
    t = ThesisResource(graph)
    assert all([m in t.title for m in ['Title 1', 'Title 2']])


def test_thesis_resource_returns_properties(graph):
    t = ThesisResource(graph)
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


def test_index_initialize_ensures_index_alias_exists(es):
    es.indices.exists_alias.return_value = False
    idx = Index('theses', Thesis)
    idx.initialize()
    assert es.indices.create.called
    assert es.indices.update_aliases.called


def test_index_versions_lists_current_index_versions(es):
    es.indices.get_alias.return_value = {'v1': 'foo', 'v2': 'bar'}
    idx = Index('theses', Thesis)
    assert 'v1' in idx.versions
    assert 'v2' in idx.versions


def test_index_versions_returns_empty_list_if_no_alias(es):
    es.indices.return_value = False
    assert Index('theses', Thesis).versions == []


def test_index_new_version_creates_new_index_version(es):
    vrs = Index('theses', Thesis).new_version()
    assert es.indices.create.called
    assert vrs.startswith('theses-')


def test_current_returns_current_version(es):
    es.indices.get_alias.return_value = {'v1': 'foo'}
    idx = Index('theses', Thesis)
    assert idx.current == 'v1'


def test_index_current_sets_new_version(es):
    es.indices.exists_alias.return_value = False
    idx = Index('theses', Thesis)
    idx.current = 'v3'
    es.indices.update_aliases.assert_called_with({'actions': [
        {'add': {'index': 'v3', 'alias': 'theses'}}]})


def test_index_current_removes_old_versions(es):
    es.indices.get_alias.return_value = {'v1': 'foo'}
    idx = Index('theses', Thesis)
    idx.current = 'v2'
    es.indices.update_aliases.assert_called_with({'actions': [
        {'remove': {'index': 'v1', 'alias': 'theses'}},
        {'add': {'index': 'v2', 'alias': 'theses'}}]})
    es.indices.delete.assert_called_with(index='v1')
