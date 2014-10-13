# Elasticsearch Solr API

## Overview

This plugin allows you to use elasticsearch with Solr interfaces.
The original project is [mocksolrplugin](https://github.com/mattweber/elasticsearch-mocksolrplugin), this project forked from it and was renamed in order to avoid confusion about each projects.

## Supported Solr features

* Update handlers
 * XML Update Handler (ie. /update)
 * JavaBin Update Handler (ie. /update/javabin)
* Search handler (ie. /select)
 * Basic lucene queries using the q paramter
 * start, rows, and fl parameters
 * sorting
 * filter queries (fq parameters)
 * hit highlighting (hl, hl.fl, hl.snippets, hl.fragsize, hl.simple.pre, hl.simple.post)
 * faceting (facet, facet.field, facet.query, facet.sort, facet.limit)
* XML and JavaBin request and response formats

## Install Solr API plugin

Type the following command:

    $ ./bin/plugin --install org.codelibs/elasticsearch-solr-api/1.6.0

## Versions

| Solr API | elasticsearch | Lucene/Solr |
|:--------:|:-------------:|:-----------:|
| master   | 1.4.x         | 4.10.1      |
| 1.6.0    | 1.4.0.Beta1   | 4.10.1      |
| 1.5.2    | 1.3.4         | 4.9.1       |
| 1.4.0    | 1.2.0         | 4.8.1       |
| 1.3.0    | 1.0.0         | 4.6.1       |
| 1.2.2    | 0.90.5        | 4.4.0       |


### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-solr-api/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## How to use this plugin.

Just point your Solr client/tool to your elasticsearch instance and appending /_solr to the url.

    http://localhost:9200/[index]/[type]/_solr/[select|update]

* [index] - the elasticsearch index you want to index/search against. Default "solr".
* [type] - the elasticsearch type you want to index/search against. Default "docs".


