from elasticsearch_dsl import DocType, String, Integer

import rdflib
import requests

from pit.namespaces import EBU, F4EV, MSL, PCDM, DCTERMS, RDF


class Thesis(DocType):
    title = String()
    department = String(index='not_analyzed')
    reviewer = String(index='not_analyzed')
    author = String(index='not_analyzed')
    uri = String(index='not_analyzed')
    issue_date = Integer()
    copyright_date = Integer()
    abstract = String()
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
    def title(self):
        return self._get(DCTERMS.title)

    @property
    def department(self):
        return self._get(MSL.associatedDepartment)

    @property
    def reviewer(self):
        return self._get(MSL.reviewedBy)

    @property
    def author(self):
        return self._get(DCTERMS.creator)

    @property
    def issue_date(self):
        return self._get(DCTERMS.dateIssued)

    @property
    def copyright_date(self):
        return self._get(DCTERMS.dateCopyrighted)

    @property
    def abstract(self):
        return self._get(DCTERMS.abstract)

    @property
    def full_text(self):
        for f in self.resource.files:
            if f.mimetype == 'text/plain':
                return f.read()

    def _get(self, prop):
        return self.resource.g.value(subject=self.resource.uri,
                                     predicate=prop,
                                     object=None,
                                     any=False)
