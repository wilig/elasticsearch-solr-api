package org.codelibs.elasticsearch.solr.plugin;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class SolrPluginTest extends TestCase {

    private static final String DATE_FORMATE = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
            }
        }).build(newConfigs().numOfNode(1).ramIndexStore());

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_SolrDocument() throws Exception {
        final String index = "sample";
        final String type = "data";
        final String url = "http://localhost:9201/" + index + "/" + type
                + "/_solr";
        final SolrServer server = new HttpSolrServer(url);

        createIndex(index, type, server);

        //		test_search(server);
        //		test_search_start10_rows20(server);
        //		test_search_sort(server);
        //		test_search_sort_request(server);
        //		test_search_query(server);
        //		test_search_score(server);
        //		test_search_trackscores(server);
        //		test_search_explain(server);
        test_search_facet(server);
    }

    private void test_search_facet(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSort("sort_order", SolrQuery.ORDER.asc);
        query.addFacetField("price");
        query.addFacetQuery("price:1000 AND name:single");
        query.addFacetQuery("price:1000 AND name:collection");

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 1; i <= 10; i++) {
            final SolrDocument doc = resultsDocs.get(i - 1);
            assertEquals("id" + i, doc.getFieldValue("id"));
            assertEquals(String.valueOf(i % 10 * 1000),
                    doc.getFieldValue("price"));
            assertEquals("doc" + i + " from single", doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(i), doc.getFieldValue("sort_order"));
        }
        assertNull(rsp.getExplainMap());
        assertNull(rsp.getDebugMap());
        List<FacetField> facetFields = rsp.getFacetFields();
        assertEquals(1, facetFields.size());
        FacetField facetField = facetFields.get(0);
        assertEquals("price", facetField.getName());
        assertEquals(10, facetField.getValues().size());
        Map<String, Integer> facetQuery = rsp.getFacetQuery();
        assertEquals(2, facetQuery.size());
        assertEquals(100, facetQuery.get("price:1000 AND name:single")
                .intValue());
        assertEquals(100, facetQuery.get("price:1000 AND name:collection")
                .intValue());
    }

    private void test_search_explain(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSort("sort_order", SolrQuery.ORDER.asc);
        query.add("debug", "true");

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertNotSame(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 1; i <= 10; i++) {
            final SolrDocument doc = resultsDocs.get(i - 1);
            assertEquals("id" + i, doc.getFieldValue("id"));
            assertEquals(String.valueOf(i % 10 * 1000),
                    doc.getFieldValue("price"));
            assertEquals("doc" + i + " from single", doc.getFieldValue("name"));
            assertNotSame(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(i), doc.getFieldValue("sort_order"));
        }
        assertEquals(1, rsp.getDebugMap().size());
        assertEquals(10, rsp.getExplainMap().size());
    }

    private void test_search_trackscores(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSort("sort_order", SolrQuery.ORDER.asc);
        query.add("track_scores", "true");

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertNotSame(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 1; i <= 10; i++) {
            final SolrDocument doc = resultsDocs.get(i - 1);
            assertEquals("id" + i, doc.getFieldValue("id"));
            assertEquals(String.valueOf(i % 10 * 1000),
                    doc.getFieldValue("price"));
            assertEquals("doc" + i + " from single", doc.getFieldValue("name"));
            assertNotSame(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(i), doc.getFieldValue("sort_order"));
        }
    }

    private void test_search_score(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("id:id1");

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(1, resultsDocs.size());
        assertEquals(1, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertNotSame(Float.NaN, resultsDocs.getMaxScore());
        final SolrDocument doc = resultsDocs.get(0);
        assertEquals("id1", doc.getFieldValue("id"));
        assertEquals("1000", doc.getFieldValue("price"));
        assertEquals("doc1 from single", doc.getFieldValue("name"));
        assertNotSame(Float.NaN, doc.getFieldValue("score"));
        assertEquals("1", doc.getFieldValue("sort_order"));
    }

    private void test_search_query(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("price:1000 AND name:single");
        query.addSort("sort_order", SolrQuery.ORDER.asc);

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(100, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 0; i < 10; i++) {
            final int num = i * 10 + 1;
            final SolrDocument doc = resultsDocs.get(i);
            assertEquals("id" + num, doc.getFieldValue("id"));
            assertEquals(String.valueOf(1000), doc.getFieldValue("price"));
            assertEquals("doc" + num + " from single",
                    doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(num), doc.getFieldValue("sort_order"));
        }
    }

    private void test_search_sort_request(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("sort", "price asc", "sort_order desc");

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 0; i < 10; i++) {
            final SolrDocument doc = resultsDocs.get(i);
            assertEquals("id" + (2000 - i * 10), doc.getFieldValue("id"));
            assertEquals("0", doc.getFieldValue("price"));
            assertEquals("doc" + (2000 - i * 10) + " from collection",
                    doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(2000 - i * 10),
                    doc.getFieldValue("sort_order"));
        }
    }

    private void test_search_sort(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSort("price", SolrQuery.ORDER.asc);
        query.addSort("sort_order", SolrQuery.ORDER.desc);

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 0; i < 10; i++) {
            final SolrDocument doc = resultsDocs.get(i);
            assertEquals("id" + (2000 - i * 10), doc.getFieldValue("id"));
            assertEquals("0", doc.getFieldValue("price"));
            assertEquals("doc" + (2000 - i * 10) + " from collection",
                    doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(2000 - i * 10),
                    doc.getFieldValue("sort_order"));
        }
    }

    private void test_search_start10_rows20(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setStart(10);
        query.setRows(20);
        query.setQuery("*:*");
        query.addSort("sort_order", SolrQuery.ORDER.asc);

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(20, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(10, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 0; i < 20; i++) {
            final int num = i + 11;
            final SolrDocument doc = resultsDocs.get(i);
            assertEquals("id" + num, doc.getFieldValue("id"));
            assertEquals(String.valueOf(num % 10 * 1000),
                    doc.getFieldValue("price"));
            assertEquals("doc" + num + " from single",
                    doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(num), doc.getFieldValue("sort_order"));
        }
    }

    private void test_search(final SolrServer server)
            throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSort("sort_order", SolrQuery.ORDER.asc);

        final QueryResponse rsp = server.query(query);
        final SolrDocumentList resultsDocs = rsp.getResults();
        assertEquals(10, resultsDocs.size());
        assertEquals(2000, resultsDocs.getNumFound());
        assertEquals(0, resultsDocs.getStart());
        assertEquals(Float.NaN, resultsDocs.getMaxScore());
        for (int i = 1; i <= 10; i++) {
            final SolrDocument doc = resultsDocs.get(i - 1);
            assertEquals("id" + i, doc.getFieldValue("id"));
            assertEquals(String.valueOf(i % 10 * 1000),
                    doc.getFieldValue("price"));
            assertEquals("doc" + i + " from single", doc.getFieldValue("name"));
            assertEquals(Float.NaN, doc.getFieldValue("score"));
            assertEquals(String.valueOf(i), doc.getFieldValue("sort_order"));
        }
        assertNull(rsp.getExplainMap());
        assertNull(rsp.getDebugMap());
    }

    private void createIndex(final String index, final String type,
            final SolrServer server) throws IOException, SolrServerException {
        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//
                // id
                .startObject("id")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // name
                .startObject("name")//
                .field("type", "string")//
                .endObject()//
                // price
                .startObject("price")//
                .field("type", "integer")//
                .endObject()//
                // @timestamp
                .startObject("@timestamp")//
                .field("type", "solr_date")//
                .field("format", DATE_FORMATE)//
                .endObject()//
                // sort_order
                .startObject("sort_order")//
                .field("type", "integer")//
                .endObject()//
                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        for (int i = 1; i <= 1000; i++) {
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "id" + i, 1.0f);
            doc.addField("name", "doc" + i + " from single", 1.0f);
            doc.addField("price", i % 10 * 1000);
            doc.addField("@timestamp", i % 2 == 0 ? "NOW"
                    : "2000-01-01T00:00:00.000+0900");
            doc.addField("sort_order", i);
            server.add(doc);
        }

        final Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        for (int i = 1001; i <= 2000; i++) {
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "id" + i, 1.0f);
            doc.addField("name", "doc" + i + " from collection", 1.0f);
            doc.addField("price", i % 10 * 1000);
            doc.addField("@timestamp", i % 2 == 0 ? "NOW"
                    : "2000-01-01T00:00:00.000+0900");
            doc.addField("sort_order", i);
            docs.add(doc);
        }
        server.add(docs);

        server.commit();
    }
}
