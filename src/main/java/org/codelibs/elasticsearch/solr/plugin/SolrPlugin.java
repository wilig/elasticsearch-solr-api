package org.codelibs.elasticsearch.solr.plugin;

import java.util.ArrayList;
import java.util.Collection;

import org.codelibs.elasticsearch.solr.rest.SolrSearchRestAction;
import org.codelibs.elasticsearch.solr.rest.SolrUpdateRestAction;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class SolrPlugin extends AbstractPlugin {

    /*
     * (non-Javadoc)
     * @see org.elasticsearch.plugins.Plugin#name()
     */
    @Override
    public String name() {
        return "SolrAPIPlugin";
    }

    /*
     * (non-Javadoc)
     * @see org.elasticsearch.plugins.Plugin#description()
     */
    @Override
    public String description() {
        return "This plugin provides Solr interface on the elasticsearch.";
    }

    public void onModule(final RestModule module) {
        module.addRestAction(SolrUpdateRestAction.class);
        module.addRestAction(SolrSearchRestAction.class);
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        final Collection<Class<? extends Module>> modules = new ArrayList<Class<? extends Module>>();
        modules.add(SolrIndexModule.class);
        return modules;
    }
}
