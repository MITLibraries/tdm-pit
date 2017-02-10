from datetime import datetime
import logging

from elasticsearch_dsl import DocType, String, Integer, Index as _Index
import rdflib
import requests
import stomp

from pit.namespaces import BIBO, EBU, F4EV, MODS, MSL, PCDM, DCTERMS, RDF, RDA


class Thesis(DocType):
    abstract = String()
    advisor = String(index='not_analyzed')
    author = String(index='not_analyzed')
    copyright_date = Integer()
    degree = String(index='not_analyzed')
    department = String(index='not_analyzed')
    description = String()
    handle = String(index='not_analyzed')
    published_date = Integer()
    title = String()
    uri = String(index='not_analyzed')
    full_text = String()


def indexable(headers):
    return str(PCDM.Object) in headers['org.fcrepo.jms.resourceType'] and \
        str(F4EV.ResourceModification) in headers['org.fcrepo.jms.eventType']


def resolve(uri):
    headers = {
        'Accept': 'text/n3',
        'Prefer': 'include="http://fedora.info/definitions/v4/repository'
                  '#EmbedResources"'
    }
    r = requests.get('{}/fcr:metadata'.format(uri), headers=headers)
    return r.text


def uri_from_message(data, msg_format='json-ld'):
    g = rdflib.Graph().parse(data=data, format=msg_format)
    uri = g.value(subject=None, predicate=RDF.type, object=PCDM.Object,
                  any=False)
    return str(uri)


def create_thesis(url):
    data = resolve(url)
    graph = rdflib.Graph().parse(data=data, format='n3')
    t = ThesisResource(graph)
    return Thesis(
        abstract=t.abstract,
        advisor=t.advisor,
        author=t.author,
        copyright_date=t.copyright_date,
        degree=t.degree,
        department=t.department,
        description=t.description,
        handle=t.handle,
        published_date=t.published_date,
        title=t.title,
        uri=t.uri,
        full_text=t.full_text
    )


def documents(url):
    r = requests.get(url, headers={'Accept': 'text/n3'})
    r.raise_for_status()
    graph = rdflib.Graph().parse(data=r.text, format='n3')
    for doc in graph.objects(rdflib.URIRef(url), PCDM.hasMember):
        yield str(doc)


class DocumentIndexer(stomp.ConnectionListener):
    def __init__(self, index):
        self.index = index

    def on_message(self, headers, message):
        logger = logging.getLogger(__name__)
        if indexable(headers):
            logger.debug('Processing message {}'.format(headers['message-id']))
            uri = uri_from_message(message)
            try:
                thesis = create_thesis(uri)
                thesis.save(index=self.index)
                logger.info('Indexed {}'.format(uri))
            except Exception as e:
                logger.warn('Error while indexing document {}: {}'\
                    .format(url, e))


class PcdmFile(object):
    def __init__(self, graph):
        self.g = graph

    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=PCDM.File, any=False)

    @property
    def mimetype(self):
        return self.g.value(subject=self.uri, predicate=EBU.hasMimeType,
                            object=None, any=False)

    def read(self):
        r = requests.get(self.uri)
        return r.text


class PcdmObject(object):
    def __init__(self, graph):
        self.g = graph

    @property
    def uri(self):
        return self.g.value(subject=None, predicate=RDF.type,
                            object=PCDM.Object, any=False)

    @property
    def files(self):
        file_objects = []
        for o in self.g.objects(subject=self.uri, predicate=PCDM.hasFile):
            graph = rdflib.Graph()
            graph += self.g.triples((o, None, None))
            file_objects.append(PcdmFile(graph))
        return tuple(file_objects)


class ThesisResource(object):
    def __init__(self, graph):
        self.resource = PcdmObject(graph)

    @property
    def uri(self):
        return str(self.resource.uri)

    @property
    def abstract(self):
        return self._get(DCTERMS.abstract)

    @property
    def advisor(self):
        return self._get(RDA['60420'])

    @property
    def author(self):
        return self._get(DCTERMS.creator)

    @property
    def copyright_date(self):
        return self._get(DCTERMS.dateCopyrighted)

    @property
    def degree(self):
        return self._get(MSL.degreeGrantedForCompletion)

    @property
    def department(self):
        return self._get(MSL.associatedDepartment)

    @property
    def description(self):
        return self._get(MODS.note)

    @property
    def handle(self):
        return self._get(BIBO.handle)

    @property
    def published_date(self):
        return self._get(DCTERMS.issued)

    @property
    def title(self):
        return self._get(DCTERMS.title)

    @property
    def full_text(self):
        for f in self.resource.files:
            if f.mimetype == 'text/plain':
                return f.read()

    def _get(self, prop):
        return list(map(str, self.resource.g.objects(subject=self.resource.uri,
                                                     predicate=prop)))


class Index:
    def __init__(self, name, doc_type):
        self.name = name
        self.idx = _Index(name)
        self.idx.doc_type(doc_type)

    def initialize(self):
        if not self.idx.connection.indices.exists_alias(name=self.name):
            version = self.new_version()
            self.current = version

    def new_version(self):
        version = "{}-{}".format(self.name, datetime.utcnow().timestamp())
        self.idx.connection.indices.create(index=version,
                                           body=self.idx.to_dict())
        return version

    @property
    def versions(self):
        if self.idx.connection.indices.exists_alias(name=self.name):
            indices = self.idx.connection.indices.get_alias(name=self.name)
            return list(indices.keys())
        return []

    @property
    def current(self):
        if self.versions:
            return self.versions[0]

    @current.setter
    def current(self, value):
        body = {"actions": []}
        versions = self.versions
        for idx in versions:
            body['actions'].append(
                {"remove": {"index": idx, "alias": self.name}})
        body['actions'].append(
            {"add": {"index": value, "alias": self.name}})
        self.idx.connection.indices.update_aliases(body)
        if versions:
            self.idx.connection.indices.delete(index=",".join(versions))
