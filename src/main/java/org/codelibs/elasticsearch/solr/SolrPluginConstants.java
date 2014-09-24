package org.codelibs.elasticsearch.solr;

import java.nio.charset.Charset;

public class SolrPluginConstants {

    public static final String DEFAULT_TYPE_NAME = "docs";

    public static final String DEFAULT_INDEX_NAME = "solr";

    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static final String XML_FORMAT_TYPE = "xml";

    public static final String JAVABIN_FORMAT_TYPE = "javabin";

    public static final String NONE_FORMAT_TYPE = "none";

    public static final String FACET_FIELD_PREFIX = "facet_field_";

    public static final String FACET_QUERY_PREFIX = "facet_query_";

    private SolrPluginConstants() {
    }
}
