from elasticsearch_dsl import DocType, String, Integer

import rdflib
import requests

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

    class Meta:
        index = 'theses'


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
        self.g.parse(data=resolve(self.uri), format='n3')

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
