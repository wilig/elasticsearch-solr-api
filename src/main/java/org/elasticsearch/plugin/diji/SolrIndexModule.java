package org.elasticsearch.plugin.diji;

import org.elasticsearch.common.inject.AbstractModule;

import co.diji.index.mapper.date.RegisterSolrDateType;

public class SolrIndexModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(RegisterSolrDateType.class).asEagerSingleton();
	}
}
