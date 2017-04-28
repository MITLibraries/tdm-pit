# Process for Indexing Thesis

This is the main worker process used for indexing items from Fedora in Elasticsearch.

## Installing

pit uses [Pipenv](http://pipenv.org/) to manage dependencies. Once you have pipenv installed:

```bash
$ git clone https://github.com/mitlib-tdm/pit.git
$ cd pit
$ pipenv install --dev
```

## Using pit

The `pit run` subcommand will start the worker process. This will watch Fedora's ActiveMQ instance for newly added items and add them to the Elasticsearch index. Type `$ pit run --help` for a full description.

The `pit reindex` subcommand will reindex the full theses collection. Type `$ pit reindex --help` for a full description.

## Developing

There are several Makefile targets that can be used for developing. `make test` and `make coverage` will run the tests and output the test coverage. `make update` will update all the dependencies. `make release` will increase the version number, create a new tag and build a new docker image with a corresponding tag.
