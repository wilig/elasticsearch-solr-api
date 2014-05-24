package org.codelibs.elasticsearch.solr.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.codelibs.elasticsearch.solr.SolrPluginConstants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.highlight.HighlightField;

import com.google.common.base.Charsets;

public class SolrResponseUtils {

    private static final ESLogger logger = Loggers
            .getLogger(SolrResponseUtils.class);

    private static final String CONTENT_TYPE_OCTET = "application/octet-stream";

    private static final String CONTENT_TYPE_XML = "application/xml; charset=UTF-8";

    // regex and date format to detect ISO8601 date formats
    private static final Pattern ISO_DATE_PATTERN = Pattern
            .compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z");;

    private static final String YYYY_MM_DD_T_HH_MM_SS_Z = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final String YYYY_MM_DD_T_HH_MM_SS_SSS_Z = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    protected SolrResponseUtils() {
    }

    private static Date parseISODateFormat(final String value) {
        try {
            return new SimpleDateFormat(YYYY_MM_DD_T_HH_MM_SS_SSS_Z)
                    .parse(value);
        } catch (final ParseException e) {
            try {
                return new SimpleDateFormat(YYYY_MM_DD_T_HH_MM_SS_Z)
                        .parse(value);
            } catch (final ParseException e1) {
                throw new ElasticsearchException("Could not parse " + value, e);
            }
        }
    }

    /**
     * Converts the search response into a NamedList that the Solr Response
     * Writer can use.
     *
     * @param request
     *            the ES RestRequest
     * @param response
     *            the ES SearchResponse
     * @return a NamedList of the response
     */
    public static NamedList<Object> createSearchResponse(
            final RestRequest request, final SearchResponse response) {
        final NamedList<Object> resp = new SimpleOrderedMap<Object>();
        resp.add("responseHeader", createResponseHeader(request, response));
        resp.add("response", convertToSolrDocumentList(request, response));

        // add highlight node if highlighting was requested
        final NamedList<Object> highlighting = createHighlightResponse(request,
                response);
        if (highlighting != null) {
            resp.add("highlighting", highlighting);
        }

        // add faceting node if faceting was requested
        final NamedList<Object> faceting = createFacetResponse(request,
                response);
        if (faceting != null) {
            resp.add("facet_counts", faceting);
        }

        return resp;
    }

    /**
     * Creates the Solr response header based on the search response.
     *
     * @param request
     *            the ES RestRequest
     * @param response
     *            the ES SearchResponse
     * @return the response header as a NamedList
     */
    public static NamedList<Object> createResponseHeader(
            final RestRequest request, final SearchResponse response) {
        // generate response header
        final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
        responseHeader.add("status", 0);
        responseHeader.add("QTime", (int) response.getTookInMillis());

        // echo params in header
        final NamedList<Object> solrParams = new SimpleOrderedMap<Object>();
        for (final String key : request.params().keySet()) {
            final String[] values = request.paramAsStringArray(key,
                    new String[0]);
            if (values.length > 0) {
                for (final String value : values) {
                    solrParams.add(key, value);
                }
            }
        }

        responseHeader.add("params", solrParams);

        return responseHeader;
    }

    /**
     * Creates a NamedList for the for document highlighting response
     *
     * @param request
     *            the ES RestRequest
     * @param response
     *            the ES SearchResponse
     * @return a NamedList if highlighting was requested, null if not
     */
    public static NamedList<Object> createHighlightResponse(
            final RestRequest request, final SearchResponse response) {
        NamedList<Object> highlightResponse = null;

        // if highlighting was requested create the NamedList for the highlights
        if (request.paramAsBoolean("hl", false)) {
            highlightResponse = new SimpleOrderedMap<Object>();
            final SearchHits hits = response.getHits();
            // for each hit, get each highlight field and put the list
            // of highlight fragments in a NamedList specific to the hit
            for (final SearchHit hit : hits.getHits()) {
                final NamedList<Object> docHighlights = new SimpleOrderedMap<Object>();
                final Map<String, HighlightField> highlightFields = hit
                        .getHighlightFields();
                for (final Map.Entry<String, HighlightField> entry : highlightFields
                        .entrySet()) {
                    final String fieldName = entry.getKey();
                    final HighlightField highlightField = entry.getValue();
                    final Text[] fragments = highlightField.getFragments();
                    final List<String> fragmentList = new ArrayList<String>(
                            fragments.length);
                    for (final Text fragment : fragments) {
                        fragmentList.add(fragment.string());
                    }
                    docHighlights.add(fieldName, fragmentList
                            .toArray(new String[fragmentList.size()]));
                }

                // highlighting by placing the doc highlights in the response
                // based on the document id
                highlightResponse.add(hit.id(), docHighlights);
            }
        }

        // return the highlight response
        return highlightResponse;
    }

    public static NamedList<Object> createFacetResponse(
            final RestRequest request, final SearchResponse response) {
        NamedList<Object> facetResponse = null;

        if (request.paramAsBoolean("facet", false)) {
            facetResponse = new SimpleOrderedMap<Object>();

            // create NamedLists for field and query facets
            final NamedList<Object> termFacets = new SimpleOrderedMap<Object>();
            final NamedList<Object> queryFacets = new SimpleOrderedMap<Object>();

            // loop though all the facets populating the NamedLists we just
            // created
            final Iterator<Facet> facetIter = response.getFacets().iterator();
            while (facetIter.hasNext()) {
                final Facet facet = facetIter.next();
                if (facet.getType().equals(TermsFacet.TYPE)) {
                    // we have term facet, create NamedList to store terms
                    final TermsFacet termFacet = (TermsFacet) facet;
                    final NamedList<Object> termFacetObj = new SimpleOrderedMap<Object>();
                    for (final TermsFacet.Entry tfEntry : termFacet
                            .getEntries()) {
                        termFacetObj.add(tfEntry.getTerm().string(),
                                tfEntry.getCount());
                    }

                    termFacets.add(facet.getName(), termFacetObj);
                } else if (facet.getType().equals(QueryFacet.TYPE)) {
                    final QueryFacet queryFacet = (QueryFacet) facet;
                    queryFacets.add(queryFacet.getName(),
                            (int) queryFacet.getCount());
                }
            }

            facetResponse.add("facet_fields", termFacets);
            facetResponse.add("facet_queries", queryFacets);

            // add dummy facet_dates and facet_ranges since we dont support them
            // yet
            facetResponse.add("facet_dates", new SimpleOrderedMap<Object>());
            facetResponse.add("facet_ranges", new SimpleOrderedMap<Object>());

        }

        return facetResponse;
    }

    /**
     * Converts the search results into a SolrDocumentList that can be
     * serialized by the Solr Response Writer.
     *
     * @param request
     *            the ES RestRequest
     * @param response
     *            the ES SearchResponse
     * @return search results as a SolrDocumentList
     */
    public static SolrDocumentList convertToSolrDocumentList(
            final RestRequest request, final SearchResponse response) {
        final SolrDocumentList results = new SolrDocumentList();

        // get the ES hits
        final SearchHits hits = response.getHits();

        // set the result information on the SolrDocumentList
        results.setMaxScore(hits.getMaxScore());
        results.setNumFound(hits.getTotalHits());
        results.setStart(request.paramAsInt("start", 0));

        // loop though the results and convert each
        // one to a SolrDocument
        for (final SearchHit hit : hits.getHits()) {
            final SolrDocument doc = new SolrDocument();

            // always add score to document
            doc.addField("score", hit.score());

            // attempt to get the returned fields
            // if none returned, use the source fields
            final Map<String, SearchHitField> fields = hit.getFields();
            final Map<String, Object> source = hit.sourceAsMap();
            if (fields.isEmpty()) {
                if (source != null) {
                    for (final Map.Entry<String, Object> entry : source
                            .entrySet()) {
                        final String sourceField = entry.getKey();
                        Object fieldValue = entry.getValue();

                        // ES does not return date fields as Date Objects
                        // detect if the string is a date, and if so
                        // convert it to a Date object
                        if (fieldValue instanceof String
                                && ISO_DATE_PATTERN.matcher(
                                        fieldValue.toString()).matches()) {
                            fieldValue = parseISODateFormat(fieldValue
                                    .toString());
                        }

                        doc.addField(sourceField, fieldValue);
                    }
                }
            } else {
                for (final String fieldName : fields.keySet()) {
                    final SearchHitField field = fields.get(fieldName);
                    Object fieldValue = field.getValue();

                    // ES does not return date fields as Date Objects
                    // detect if the string is a date, and if so
                    // convert it to a Date object
                    if (fieldValue instanceof String
                            && ISO_DATE_PATTERN.matcher(fieldValue.toString())
                                    .matches()) {
                        fieldValue = parseISODateFormat(fieldValue.toString());
                    }

                    doc.addField(fieldName, fieldValue);
                }
            }

            // add the SolrDocument to the SolrDocumentList
            results.add(doc);
        }

        return results;
    }

    /**
     * Serializes the NamedList in the specified output format and sends it to
     * the Solr Client.
     *
     * @param obj
     *            the NamedList response to serialize
     * @param request
     *            the ES RestRequest
     * @param channel
     *            the ES RestChannel
     */
    public static void writeResponse(final NamedList<Object> obj,
            final RestRequest request, final RestChannel channel) {
        // determine what kind of output writer the Solr client is expecting
        final String wt = request.hasParam("wt") ? request.param("wt")
                .toLowerCase() : SolrPluginConstants.XML_FORMAT_TYPE;

        // determine what kind of response we need to send
        if (wt.equals(SolrPluginConstants.XML_FORMAT_TYPE)) {
            writeXmlResponse(obj, channel);
        } else if (wt.equals(SolrPluginConstants.JAVABIN_FORMAT_TYPE)) {
            writeJavaBinResponse(obj, channel);
        } else {
            // default xml response
            writeXmlResponse(obj, channel);
        }
    }

    /**
     * Write the response object in JavaBin format.
     *
     * @param obj
     *            the response object
     * @param channel
     *            the ES RestChannel
     */
    private static void writeJavaBinResponse(final NamedList<Object> obj,
            final RestChannel channel) {
        final ByteArrayOutputStream bo = new ByteArrayOutputStream();

        // try to marshal the data
        try {
            new JavaBinCodec().marshal(obj, bo);
        } catch (final IOException e) {
            logger.error("Error writing JavaBin response", e);
        }

        final Object errorResponse = obj.get("error");
        // send the response
        channel.sendResponse(new BytesRestResponse(
                errorResponse != null ? RestStatus.INTERNAL_SERVER_ERROR
                        : RestStatus.OK, CONTENT_TYPE_OCTET, bo.toByteArray()));
    }

    private static void writeXmlResponse(final NamedList<Object> obj,
            final RestChannel channel) {
        final Writer writer = new StringWriter();

        // try to serialize the data to xml
        try {
            writer.write(XMLWriter.XML_START1);
            writer.write(XMLWriter.XML_START2_NOSCHEMA);

            // initialize the xml writer
            final XMLWriter xw = new XMLWriter(writer);

            // loop though each object and convert it to xml
            final int sz = obj.size();
            for (int i = 0; i < sz; i++) {
                xw.writeVal(obj.getName(i), obj.getVal(i));
            }

            writer.write("\n</response>\n");
            writer.close();
        } catch (final IOException e) {
            logger.error("Error writing XML response", e);
        }

        // send the response
        final Object errorResponse = obj.get("error");
        channel.sendResponse(new BytesRestResponse(
                errorResponse != null ? RestStatus.INTERNAL_SERVER_ERROR
                        : RestStatus.OK, CONTENT_TYPE_XML, writer.toString()
                        .getBytes(Charsets.UTF_8)));
    }
}
