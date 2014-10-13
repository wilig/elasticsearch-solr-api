package org.codelibs.elasticsearch.solr.rest;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.queryFilter;

import java.io.IOException;

import org.apache.commons.codec.Charsets;
import org.codelibs.elasticsearch.solr.SolrPluginConstants;
import org.codelibs.elasticsearch.solr.solr.SolrResponseUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.io.BaseEncoding;

public class SolrSearchRestAction extends BaseRestHandler {

    private final String defaultIndexName;

    private final String defaultTypeName;

    private final boolean lowercaseExpandedTerms;

    private final boolean autoGeneratePhraseQueries;

    /**
     * Rest actions that mocks the Solr search handler
     *
     * @param settings
     *            ES settings
     * @param client
     *            ES client
     * @param restController
     *            ES rest controller
     */
    @Inject
    public SolrSearchRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, restController, client);

        defaultIndexName = settings.get("solr.default.index",
                SolrPluginConstants.DEFAULT_INDEX_NAME);
        defaultTypeName = settings.get("solr.default.type",
                SolrPluginConstants.DEFAULT_TYPE_NAME);

        lowercaseExpandedTerms = settings.getAsBoolean(
                "solr.lowercaseExpandedTerms", false);
        autoGeneratePhraseQueries = settings.getAsBoolean(
                "solr.autoGeneratePhraseQueries", true);

        // register search handler
        // specifying and index and type is optional
        restController.registerHandler(RestRequest.Method.GET, "/_solr/select",
                this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/_solr/select", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/{type}/_solr/select", this);
        // SolrServer#query also supports POST method.
        restController.registerHandler(RestRequest.Method.POST,
                "/_solr/select", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_solr/select", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_solr/select", this);

    }

    @Override
    protected void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) {
        final RestRequest requestEx = new ExtendedRestRequest(request);

        // generate the search request
        final SearchRequest searchRequest = getSearchRequest(requestEx);
        searchRequest.listenerThreaded(false);

        // execute the search
        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse response) {
                try {
                    // write response
                    SolrResponseUtils.writeResponse(SolrResponseUtils
                            .createSearchResponse(requestEx, response),
                            requestEx, channel);
                } catch (final Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Throwable t) {
                logger.error("Error processing executing search", t);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, t));
                } catch (final IOException e) {
                    logger.error("Failed to send failure response", e);
                }
            }
        });
    }

    /**
     * Generates an ES SearchRequest based on the Solr Input Parameters
     *
     * @param request
     *            the ES RestRequest
     * @return the generated ES SearchRequest
     */
    private SearchRequest getSearchRequest(final RestRequest request) {
        // get solr search parameters
        final String q = request.param("q", null);
        final int start = request.paramAsInt("start", 0);
        final int rows = request.paramAsInt("rows", 10);
        final String[] fl = request.paramAsStringArray("fl",
                Strings.EMPTY_ARRAY);
        final String[] sort = request.paramAsStringArray("sort",
                Strings.EMPTY_ARRAY);
        final String[] fqs = request.paramAsStringArray("fq",
                Strings.EMPTY_ARRAY);
        final boolean hl = request.paramAsBoolean("hl", false);
        final boolean facet = request.paramAsBoolean("facet", false);
        final boolean trackScores = request.paramAsBoolean("track_scores",
                false);
        final String debug = request.param("debug");

        final boolean qDsl = request.paramAsBoolean("q.dsl", false);
        final boolean fqDsl = request.paramAsBoolean("fq.dsl", false);

        // build the query
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (q != null) {
            if (qDsl) {
                searchSourceBuilder.query(QueryBuilders.wrapperQuery(q));
            } else {
                QueryBuilder queryBuilder;
                // handler filters
                if (fqs.length > 0) {
                    FilterBuilder filterBuilder = null;

                    // if there is more than one filter specified build
                    // an and filter of query filters, otherwise just
                    // build a single query filter.
                    if (fqs.length > 1) {
                        final AndFilterBuilder fqAnd = andFilter();
                        for (final String fq : fqs) {
                            fqAnd.add(queryFilter(fqDsl ? QueryBuilders
                                    .wrapperQuery(fq) : QueryBuilders
                                    .queryString(fq)));
                        }
                        filterBuilder = fqAnd;
                    } else {
                        filterBuilder = queryFilter(fqDsl ? QueryBuilders
                                .wrapperQuery(fqs[0]) : QueryBuilders
                                .queryString(fqs[0]));
                    }

                    queryBuilder = QueryBuilders.filteredQuery(
                            QueryBuilders
                                    .queryString(q)
                                    .lowercaseExpandedTerms(
                                            lowercaseExpandedTerms)
                                    .autoGeneratePhraseQueries(
                                            autoGeneratePhraseQueries),
                            filterBuilder);
                } else {
                    queryBuilder = QueryBuilders
                            .queryString(q)
                            .lowercaseExpandedTerms(lowercaseExpandedTerms)
                            .autoGeneratePhraseQueries(
                                    autoGeneratePhraseQueries);
                }
                searchSourceBuilder.query(queryBuilder);
            }
        }

        searchSourceBuilder.from(start);
        searchSourceBuilder.size(rows);

        // parse fl into individual fields
        // solr supports separating by comma or spaces
        if (fl.length > 0) {
            for (final String field : fl) {
                if (Strings.hasText(field)) {
                    searchSourceBuilder.fields(field.trim());
                }
            }
        }

        // handle sorting
        if (sort.length > 0) {
            for (final String s : sort) {
                final String sortStr = s.trim();
                final int delimiter = sortStr.lastIndexOf(' ');
                if (delimiter != -1) {
                    String sortField = sortStr.substring(0, delimiter);
                    if ("score".equals(sortField)) {
                        sortField = "_score";
                    }
                    final String reverse = sortStr.substring(delimiter + 1);
                    if ("asc".equals(reverse)) {
                        searchSourceBuilder.sort(SortBuilders
                                .fieldSort(sortField).order(SortOrder.ASC)
                                .ignoreUnmapped(true));
                    } else if ("desc".equals(reverse)) {
                        searchSourceBuilder.sort(SortBuilders
                                .fieldSort(sortField).order(SortOrder.DESC)
                                .ignoreUnmapped(true));
                    }
                } else {
                    searchSourceBuilder.sort(SortBuilders.fieldSort(sortStr)
                            .ignoreUnmapped(true));
                }
            }
        } else {
            // default sort by descending score
            searchSourceBuilder.sort("_score", SortOrder.DESC);
        }

        // handle highlighting
        if (hl) {
            // get supported highlighting parameters if they exist
            final String hlfl = request.param("hl.fl", null);
            final int hlsnippets = request.paramAsInt("hl.snippets", 1);
            final int hlfragsize = request.paramAsInt("hl.fragsize", 100);
            final String hlTagPre = request.param("hl.tag.pre",
                    request.param("hl.simple.pre", null));
            final String hlTagPost = request.param("hl.tag.post",
                    request.param("hl.simple.post", null));
            final boolean requireFieldMatch = request.paramAsBoolean(
                    "hl.requireFieldMatch", false);

            final HighlightBuilder highlightBuilder = new HighlightBuilder();
            if (hlfl == null) {
                // run against default _all field
                highlightBuilder.field("_all", hlfragsize, hlsnippets);
            } else {
                final String[] hlfls = hlfl.split("\\s|,");
                for (final String hlField : hlfls) {
                    // skip wildcarded fields
                    if (!hlField.contains("*")) {
                        highlightBuilder.field(hlField, hlfragsize, hlsnippets);
                    }
                }
            }

            // pre tags
            if (hlTagPre != null) {
                highlightBuilder.preTags(hlTagPre);
            }

            // post tags
            if (hlTagPost != null) {
                highlightBuilder.postTags(hlTagPost);
            }

            highlightBuilder.requireFieldMatch(requireFieldMatch);

            searchSourceBuilder.highlight(highlightBuilder);

        }

        // handle faceting
        if (facet) {
            // get supported facet parameters if they exist
            final String[] facetFields = request.paramAsStringArray(
                    "facet.field", new String[0]);
            final String facetSort = request.param("facet.sort", null);
            int facetLimit = request.paramAsInt("facet.limit", 100);
            if (facetLimit < 0) {
                facetLimit = Integer.MAX_VALUE;
            }
            if (facetFields.length > 0) {
                for (final String facetField : facetFields) {
                    Order order;
                    if (facetSort != null && "index".equals(facetSort)) {
                        order = Order.term(true);
                    } else {
                        order = Order.count(false);
                    }
                    String encodedField = BaseEncoding.base64().encode(
                            facetField.getBytes(Charsets.UTF_8));
                    TermsBuilder termsBuilder = AggregationBuilders
                            .terms(SolrPluginConstants.FACET_FIELD_PREFIX
                                    + encodedField).field(facetField)
                            .size(facetLimit).order(order);
                    searchSourceBuilder.aggregation(termsBuilder);
                }
            }

            final String[] facetQueries = request.paramAsStringArray(
                    "facet.query", new String[0]);
            if (facetQueries.length > 0) {
                for (int i = 0; i < facetQueries.length; i++) {
                    final String facetQuery = facetQueries[i];
                    final String encodedFacetQuery = BaseEncoding.base64()
                            .encode(facetQuery.getBytes(Charsets.UTF_8));
                    FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders
                            .filter(SolrPluginConstants.FACET_QUERY_PREFIX + i
                                    + '_' + encodedFacetQuery).filter(
                                    FilterBuilders.queryFilter(QueryBuilders
                                            .queryString(facetQuery)));
                    searchSourceBuilder.aggregation(filterAggregationBuilder);
                }
            }
        }

        // track score
        searchSourceBuilder.trackScores(trackScores);

        // explain
        if ("true".equalsIgnoreCase(debug) || "query".equalsIgnoreCase(debug)
                || "timing".equalsIgnoreCase(debug)
                || "results".equalsIgnoreCase(debug)) {
            searchSourceBuilder.explain(true);
        }

        // get index and type we want to search against
        final String index = request.param("index", defaultIndexName);
        final String type = request.param("type", defaultTypeName);

        // Build the search Request
        final String[] indices = Strings.splitStringByCommaToArray(index);
        final SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.extraSource(searchSourceBuilder);
        searchRequest.types(Strings.splitStringByCommaToArray(type));

        return searchRequest;
    }
}
