import os
import time

import aiohttp
import jwt


def grant_token(email, key, scopes, audience):
    now = int(time.time())
    exp = now + 3600
    claims = {
        'iss': email,
        'scope': ' '.join(scopes),
        'aud': audience,
        'iat': now,
        'exp': exp,
    }
    msg = jwt.encode(claims, key, algorithm='RS256')
    return {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': msg,
    }


class AuthorizedSession:
    def __init__(self, auth_url, email, key, scopes, audience):
        self.auth_url, self.email, self.key, self.scopes, self.audience = \
            auth_url, email, key, scopes, audience
        self.session = aiohttp.ClientSession()
        self._token = None

    async def request(self, method, url, *args, **kwargs):
        if not self._token:
            await self.authorize()
        headers = kwargs.pop('headers', {})
        headers.update({'Authorization': 'Bearer {}'.format(self._token)})
        r = await self.session.request(method, url, *args, headers=headers,
                                       **kwargs)
        if r.status == 401:
            await r.release()
            await self.authorize()
            headers.update({'Authorization': 'Bearer {}'.format(self._token)})
            r = await self.session.request(method, url, headers=headers,
                                           **kwargs)
        return r

    async def authorize(self):
        data = grant_token(self.email, self.key, self.scopes, self.audience)
        r = await self.session.request('POST', self.auth_url, data=data)
        resp = await r.json()
        self._token = resp['access_token']


class Client:
    def __init__(self, session=None,
                 url='https://www.googleapis.com/storage/v1',
                 upload_url='https://www.googleapis.com/upload/storage/v1'):
        self.session = session or aiohttp.ClientSession()
        self.url, self.upload_url = url, upload_url

    def get(self, bucket):
        return Bucket(bucket, client=self)

    async def request(self, method, url, *args, **kwargs):
        return await self.session.request(method, url, *args, **kwargs)


class Bucket:
    def __init__(self, name, client):
        self.name = name
        self.client = client

    @property
    def path(self):
        return '/b/' + self.name

    @property
    def url(self):
        return self.client.url + self.path

    @property
    def upload_url(self):
        return self.client.upload_url + self.path + '/o'

    def create(self, obj_name):
        return BucketObject(obj_name, bucket=self)


class BucketObject:
    def __init__(self, name, bucket):
        self.name = name
        self.bucket = bucket
        self.client = bucket.client

    @property
    def url(self):
        return self.bucket.url + '/o/' + self.name

    async def upload(self, file_obj):
        if isinstance(file_obj, str):
            with open(file_obj, 'rb') as f:
                await self._upload_bytes(f)
        else:
            await self._upload_bytes(file_obj)

    async def _upload_bytes(self, fp):
        size = os.fstat(fp.fileno()).st_size
        location = await self._resumable_session(size)
        resp = await self.client.request('PUT', location, data=fp)
        resp.raise_for_status()

    async def _resumable_session(self, filesize):
        headers = {
            'X-Upload-Content-Type': 'application/zip',
            'X-Upload-Content-Length': str(filesize),
            'Content-Length': '0',
        }
        params = {
            'uploadType': 'resumable',
            'name': self.name,
        }
        resp = await self.client.request('POST', self.bucket.upload_url,
                                         headers=headers, params=params)
        if resp.status != 200:
            raise Exception('Error creating resumable session')
        location = resp.headers['Location']
        await resp.release()
        return location
