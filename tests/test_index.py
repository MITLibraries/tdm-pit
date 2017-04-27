from asyncio import coroutine
import json
from unittest.mock import Mock

import pytest
from rdflib import Graph

from tests import air_mock
from pit.index import (create_thesis,
                       indexable,
                       index_collection,
                       index_thesis,
                       Indexer,
                       ThesisResource,
                       uri_from_message,)


@pytest.fixture
def graph(thesis_1):
    g = Graph().parse(data=thesis_1, format='n3')
    return g


def test_uri_from_message_returns_uri():
    msg = ('{"@id": "mock://example.com/1", "@type": ['
           '"http://pcdm.org/models#Object"]}')
    assert uri_from_message(msg) == 'mock://example.com/1'


def test_indexable_returns_true_for_indexable_events():
    assert indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#Object',
        'org.fcrepo.jms.eventType': 'http://fedora.info/definitions/v4/'
                                    'event#ResourceModification'})


def test_indexable_returns_false_for_nonindexable_events():
    assert not indexable({
        'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#File'})


@pytest.mark.asyncio
async def test_create_thesis_fetches_full_text(thesis_1):
    with air_mock.Mock() as m:
        m.get('mock://example.com/theses/1', text=thesis_1)
        m.get('mock://example.com/theses/1/1.txt', text='FOOBAR')
        t = await create_thesis('mock://example.com/theses/1')
        assert t['full_text'] == 'FOOBAR'


@pytest.mark.asyncio
async def test_index_thesis_adds_thesis(thesis_1):
    with air_mock.Mock() as m:
        es = Mock()
        es.add.side_effect = coroutine(Mock())
        m.get('mock://example.com/theses/1', text=thesis_1)
        m.get('mock://example.com/theses/1/1.txt', text='FOOBAR')
        t = await index_thesis(es, 'mock://example.com/theses/1')
        args = es.add.call_args[0][0]
        assert args['handle'] == ['http://handle.org/1']
        assert t == 'mock://example.com/theses/1'


@pytest.mark.asyncio
async def test_index_collection_adds_each_item(theses, thesis_1, thesis_2):
    es = Mock()
    es.add.side_effect = coroutine(Mock())
    with air_mock.Mock() as m:
        m.get('mock://example.com/theses', text=theses)
        m.get('mock://example.com/theses/1', text=thesis_1)
        m.get('mock://example.com/theses/1/1.txt', text='FOOBAR')
        m.get('mock://example.com/theses/2', text=thesis_2)
        m.get('mock://example.com/theses/2/2.txt', text='FOOBAZ')
        await index_collection('mock://example.com/theses', es)
        assert es.add.call_count == 2
        ft = [a[0][0]['full_text'] for a in es.add.call_args_list]
        assert 'FOOBAR' in ft
        assert 'FOOBAZ' in ft


@pytest.mark.asyncio
async def test_indexer_adds_indexable_item(thesis_1):
    es = Mock()
    es.add.side_effect = coroutine(Mock())
    headers = {'org.fcrepo.jms.resourceType': 'http://pcdm.org/models#Object',
               'org.fcrepo.jms.eventType': 'http://fedora.info/definitions/'
                                           'v4/event#ResourceModification',
               'message-id': '1234'}
    body = {'@id': 'mock://example.com/theses/1',
            '@type': 'http://pcdm.org/models#Object'}
    with air_mock.Mock() as m:
        frame = Mock(headers=headers, body=json.dumps(body))
        idxer = Indexer(es)
        m.get('mock://example.com/theses/1', text=thesis_1)
        m.get('mock://example.com/theses/1/1.txt', text='FOOBAR')
        await idxer.on_message(frame)
        assert es.add.called


def test_thesis_properties_return_mutliple(graph):
    t = ThesisResource(graph, None)
    assert 'Title 1' in t.title
    assert 'Title 2' in t.title


def test_thesis_resource_returns_properties(graph):
    t = ThesisResource(graph, None)
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
    assert t.uri == 'mock://example.com/theses/1'
