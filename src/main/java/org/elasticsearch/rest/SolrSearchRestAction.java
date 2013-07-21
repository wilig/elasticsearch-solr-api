package org.elasticsearch.rest;

import java.io.IOException;

import org.elasticsearch.SolrPluginConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.solr.SolrResponseUtils;

import static org.elasticsearch.index.query.FilterBuilders.*;

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
        super(settings, client);

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

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.elasticsearch.rest.RestHandler#handleRequest(org.elasticsearch.rest
     * .RestRequest, org.elasticsearch.rest.RestChannel)
     */
    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {
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
                    channel.sendResponse(new XContentThrowableRestResponse(
                            requestEx, t));
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
        final String fl = request.param("fl", null);
        final String sort = request.param("sort", null);
        final String[] fqs = request.paramAsStringArray("fq", new String[0]);
        final boolean hl = request.paramAsBoolean("hl", false);
        final boolean facet = request.paramAsBoolean("facet", false);

        final boolean qDsl = request.paramAsBoolean("q.dsl", false);
        final boolean fqDsl = request.paramAsBoolean("fq.dsl", false);

        // build the query
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (q != null) {
            QueryBuilder queryBuilder;
            if (qDsl) {
                queryBuilder = QueryBuilders.wrapperQuery(q);
            } else {
                queryBuilder = QueryBuilders.queryString(q)
                        .lowercaseExpandedTerms(lowercaseExpandedTerms)
                        .autoGeneratePhraseQueries(autoGeneratePhraseQueries);
            }
            searchSourceBuilder.query(queryBuilder);
        }

        searchSourceBuilder.from(start);
        searchSourceBuilder.size(rows);

        // parse fl into individual fields
        // solr supports separating by comma or spaces
        if (fl != null) {
            if (!Strings.hasText(fl)) {
                searchSourceBuilder.noFields();
            } else {
                searchSourceBuilder.fields(fl.split("\\s|,"));
            }
        }

        // handle sorting
        if (sort != null) {
            final String[] sorts = Strings.splitStringByCommaToArray(sort);
            for (final String sort2 : sorts) {
                final String sortStr = sort2.trim();
                final int delimiter = sortStr.lastIndexOf(" ");
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

        // handler filters
        if (fqs.length > 0) {
            FilterBuilder filterBuilder = null;

            // if there is more than one filter specified build
            // an and filter of query filters, otherwise just
            // build a single query filter.
            if (fqs.length > 1) {
                final AndFilterBuilder fqAnd = andFilter();
                for (final String fq : fqs) {
                    final QueryBuilder queryBuilder = fqDsl ? QueryBuilders
                            .wrapperQuery(fq) : QueryBuilders.queryString(fq);
                    fqAnd.add(queryFilter(queryBuilder));
                }
                filterBuilder = fqAnd;
            } else {
                final QueryBuilder queryBuilder = fqDsl ? QueryBuilders
                        .wrapperQuery(fqs[0]) : QueryBuilders
                        .queryString(fqs[0]);
                filterBuilder = queryFilter(queryBuilder);
            }

            searchSourceBuilder.filter(filterBuilder);
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
            final int facetLimit = request.paramAsInt("facet.limit", 100);

            final String[] facetQueries = request.paramAsStringArray(
                    "facet.query", new String[0]);

            if (facetFields.length > 0) {
                for (final String facetField : facetFields) {
                    final TermsFacetBuilder termsFacetBuilder = new TermsFacetBuilder(
                            facetField);
                    termsFacetBuilder.size(facetLimit);
                    termsFacetBuilder.field(facetField);

                    if (facetSort != null && facetSort.equals("index")) {
                        termsFacetBuilder.order(TermsFacet.ComparatorType.TERM);
                    } else {
                        termsFacetBuilder
                                .order(TermsFacet.ComparatorType.COUNT);
                    }

                    searchSourceBuilder.facet(termsFacetBuilder);
                }
            }

            if (facetQueries.length > 0) {
                for (final String facetQuery : facetQueries) {
                    final QueryFacetBuilder queryFacetBuilder = new QueryFacetBuilder(
                            facetQuery);
                    queryFacetBuilder.query(QueryBuilders
                            .queryString(facetQuery));
                    searchSourceBuilder.facet(queryFacetBuilder);
                }
            }
        }

        // get index and type we want to search against
        final String index = request.param("index", defaultIndexName);
        final String type = request.param("type", defaultTypeName);

        // Build the search Request
        final String[] indices = RestActions.splitIndices(index);
        final SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.extraSource(searchSourceBuilder);
        searchRequest.types(RestActions.splitTypes(type));

        return searchRequest;
    }
}
