package org.codelibs.elasticsearch.solr.plugin;

import org.codelibs.elasticsearch.solr.index.mapper.date.RegisterSolrDateType;
import org.elasticsearch.common.inject.AbstractModule;

public class SolrIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        this.bind(RegisterSolrDateType.class).asEagerSingleton();
    }
}
