from datetime import datetime


thesis_map = {
    'mappings': {
        'thesis': {
            'properties': {
                'abstract': {'type': 'string'},
                'advisor': {'type': 'string', 'index': 'not_analyzed'},
                'author': {'type': 'string', 'index': 'not_analyzed'},
                'copyright_date': {'type': 'integer'},
                'degree': {'type': 'string'},
                'department': {'type': 'string', 'index': 'not_analyzed'},
                'description': {'type': 'string'},
                'handle': {'type': 'string', 'index': 'not_analyzed'},
                'published_date': {'type': 'integer'},
                'title': {'type': 'string'},
                'uri': {'type': 'string', 'index': 'not_analyzed'},
                'full_text': {'type': 'string'},
            }
        }
    }
}


class Index:
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    async def initialize(self):
        if not await self.conn.indices.exists_alias(self.name):
            version = await self.new_version()
            await self.set_current(version)

    async def new_version(self):
        version = '{}-{}'.format(self.name, datetime.utcnow().timestamp())
        await self.conn.indices.create(version, thesis_map)
        return version

    @property
    async def versions(self):
        if await self.conn.indices.exists_alias(self.name):
            indices = await self.conn.indices.get_alias(name=self.name)
            return list(indices.keys())
        return []

    async def set_current(self, version):
        body = {"actions": []}
        versions = await self.versions
        for idx in versions:
            body['actions'].append(
                {"remove": {"index": idx, "alias": self.name}})
        body['actions'].append(
            {"add": {"index": version, "alias": self.name}})
        await self.conn.indices.update_aliases(body)
        if versions:
            await self.conn.indices.delete(index=",".join(versions))

    async def add(self, document):
        await self.conn.index(self.name, 'thesis', document, op_type='index')
