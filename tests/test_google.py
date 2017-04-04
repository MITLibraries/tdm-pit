import pytest
from tests import air_mock

from pit.google import AuthorizedSession, Bucket, BucketObject, Client


@pytest.yield_fixture
def google():
    with air_mock.Mock() as m:
        m.post('mock://example.com/b/foo/o',
               headers={'Location': 'mock://example.com/b/foo/o?upload_id=1'})
        m.put('mock://example.com/b/foo/o')
        yield m


@pytest.fixture
def package():
    return ('tests/fixtures/27722c0c-16f1-4c91-bf7d-9a5db7124673.zip')


@pytest.fixture(scope="session")
def private_key():
    with open('tests/fixtures/id_rsa') as fp:
        key = fp.read()
    return key


@pytest.fixture(scope="session")
def public_key():
    with open('tests/fixtures/id_rsa.pub') as fp:
        key = fp.read()
    return key


@pytest.mark.asyncio
async def test_auth_session_sets_token(private_key):
    with air_mock.Mock() as m:
        m.post('mock://example.com/auth', json={'access_token': 'foobar'})
        s = AuthorizedSession('mock://example.com/auth', 'foo@example.com',
                              private_key, ['mock://example.com/scope'],
                              'mock://example.com/aud')
        await s.authorize()
        assert s._token == 'foobar'


@pytest.mark.asyncio
async def test_auth_session_sets_header(private_key):
    with air_mock.Mock() as m:
        m.post('mock://example.com/auth', json={'access_token': 'foobar'})
        m.get('mock://example.com/foo')
        s = AuthorizedSession('mock://example.com/auth', 'foo@example.com',
                              private_key, ['mock://example.com/scope'],
                              'mock://example.com/aud')
        await s.authorize()
        await s.request('GET', 'mock://example.com/foo')
        assert m.request_history[1].headers == \
            {'Authorization': 'Bearer foobar'}


@pytest.mark.asyncio
async def test_auth_session_authorizes_on_401(private_key):
    with air_mock.Mock() as m:
        m.post('mock://example.com/auth', json={'access_token': 'foobar'})
        m.get('mock://example.com/foo', status=401)
        s = AuthorizedSession('mock://example.com/auth', 'foo@example.com',
                              private_key, ['mock://example.com/scope'],
                              'mock://example.com/aud')
        s._token = 'foobar'
        await s.request('GET', 'mock://example.com/foo')
        assert m.request_history[1].url == 'mock://example.com/auth'


def test_bucket_has_path(google):
    assert Bucket('foo', Client()).path == '/b/foo'


def test_bucket_has_upload_url(google):
    assert Bucket('foo', Client()).upload_url == \
        'https://www.googleapis.com/upload/storage/v1/b/foo/o'


def test_bucket_creates_bucket_object(google):
    obj = Bucket('foo', Client()).create('bar')
    assert isinstance(obj, BucketObject)


def test_bucket_object_has_url(google):
    assert BucketObject('bar', Bucket('foo', Client())).url == \
        'https://www.googleapis.com/storage/v1/b/foo/o/bar'


@pytest.mark.asyncio
async def test_bucket_uploads_from_filename(package):
    with air_mock.Mock() as m:
        m.post('mock://example.com/b/foo/o',
               headers={'Location': 'mock://example.com/b/foo/o?upload_id=1'})
        m.put('mock://example.com/b/foo/o?upload_id=1')
        c = Client(url='mock://example.com', upload_url='mock://example.com')
        obj = BucketObject('bar', Bucket('foo', c))
        await obj.upload(package)
        assert m.request_history[1].url == \
            'mock://example.com/b/foo/o?upload_id=1'


@pytest.mark.asyncio
async def test_bucket_uploads_from_file_object(package):
    with air_mock.Mock() as m:
        m.post('mock://example.com/b/foo/o',
               headers={'Location': 'mock://example.com/b/foo/o?upload_id=1'})
        m.put('mock://example.com/b/foo/o?upload_id=1')
        c = Client(url='mock://example.com', upload_url='mock://example.com')
        obj = BucketObject('bar', Bucket('foo', c))
        with open(package) as fp:
            await obj.upload(fp)
        assert m.request_history[1].url == \
            'mock://example.com/b/foo/o?upload_id=1'
