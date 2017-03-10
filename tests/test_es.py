from asyncio import coroutine
from unittest.mock import Mock

import pytest

from pit.es import Index


@pytest.mark.asyncio
async def test_initialize_ensures_index_alias_exists():
    es = Mock()
    es.indices.exists_alias.side_effect = coroutine(Mock(return_value=False))
    es.indices.create.side_effect = coroutine(Mock())
    es.indices.update_aliases.side_effect = coroutine(Mock())
    idx = Index(es, 'theses')
    await idx.initialize()
    assert es.indices.create.called
    assert es.indices.update_aliases.called


@pytest.mark.asyncio
async def test_new_version_creates_new_version():
    es = Mock()
    es.indices.create.side_effect = coroutine(Mock())
    idx = Index(es, 'theses')
    v = await idx.new_version()
    assert es.indices.create.called
    assert v.startswith('theses-')


@pytest.mark.asyncio
async def test_versions_lists_current_versions():
    es = Mock()
    es.indices.exists_alias.side_effect = coroutine(Mock(return_value=True))
    es.indices.get_alias.side_effect = coroutine(Mock(return_value={'v1': 'foo', 'v2': 'bar'}))
    idx = Index(es, 'theses')
    assert 'v1' in await idx.versions
    assert 'v2' in await idx.versions


@pytest.mark.asyncio
async def test_versions_is_empty_if_no_aliases():
    es = Mock()
    es.indices.exists_alias.side_effect = coroutine(Mock(return_value=False))
    idx = Index(es, 'theses')
    assert await idx.versions == []


@pytest.mark.asyncio
async def test_set_current_sets_new_version():
    es = Mock()
    es.indices.exists_alias.side_effect = coroutine(Mock(return_value=False))
    es.indices.update_aliases.side_effect = coroutine(Mock())
    idx = Index(es, 'theses')
    await idx.set_current('v3')
    es.indices.update_aliases.assert_called_with({'actions': [
                {'add': {'index': 'v3', 'alias': 'theses'}}]})


@pytest.mark.asyncio
async def test_set_current_removes_old_versions():
    es = Mock()
    es.indices.exists_alias.side_effect = coroutine(Mock(return_value=True))
    es.indices.get_alias.side_effect = coroutine(Mock(return_value={'v1': 'foo'}))
    es.indices.update_aliases.side_effect = coroutine(Mock())
    es.indices.delete.side_effect = coroutine(Mock())
    idx = Index(es, 'theses')
    await idx.set_current('v2')
    es.indices.update_aliases.assert_called_with({'actions': [
                {'remove': {'index': 'v1', 'alias': 'theses'}},
                {'add': {'index': 'v2', 'alias': 'theses'}}]})
    es.indices.delete.assert_called_with(index='v1')
