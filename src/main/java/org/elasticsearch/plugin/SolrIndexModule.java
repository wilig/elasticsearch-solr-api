package org.elasticsearch.plugin;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.mapper.date.RegisterSolrDateType;

public class SolrIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        this.bind(RegisterSolrDateType.class).asEagerSingleton();
    }
}
