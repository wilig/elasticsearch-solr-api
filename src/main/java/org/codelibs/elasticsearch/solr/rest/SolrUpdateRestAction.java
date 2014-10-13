package org.codelibs.elasticsearch.solr.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.codelibs.elasticsearch.solr.SolrPluginConstants;
import org.codelibs.elasticsearch.solr.solr.JavaBinUpdateRequestCodec;
import org.codelibs.elasticsearch.solr.solr.SolrResponseUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class SolrUpdateRestAction extends BaseRestHandler {

    private static final String TRUE = "true";

    // fields in the Solr input document to scan for a document id
    private static final String[] DEFAULT_ID_FIELDS = { "id", "docid",
            "documentid", "contentid", "uuid", "url" };

    // the xml input factory
    private final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    // Set this flag to false if you want to disable the hashing of id's as they
    // are provided by the Solr Input document
    // , which is the default behaviour.
    // You can configure this by adding 'plugin.diji.MockSolrPlugin.hashIds:
    // false' to elasticsearch.yml
    private final boolean hashIds;

    private final boolean commitAsFlush;

    private final boolean optimizeAsOptimize;

    private final String defaultIndexName;

    private final String defaultTypeName;

    private final String[] idFields;

    /**
     * Rest actions that mock Solr update handlers
     *
     * @param settings
     *            ES settings
     * @param client
     *            ES client
     * @param restController
     *            ES rest controller
     */
    @Inject
    public SolrUpdateRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, restController, client);

        hashIds = settings.getAsBoolean("solr.hashIds", false);
        commitAsFlush = settings.getAsBoolean("solr.commitAsFlush", true);
        optimizeAsOptimize = settings.getAsBoolean("solr.optimizeAsOptimize",
                true);
        logger.info("Solr input document id's will " + (hashIds ? "" : "not ")
                + "be hashed to created Elasticsearch document id's");

        defaultIndexName = settings.get("solr.default.index",
                SolrPluginConstants.DEFAULT_INDEX_NAME);
        defaultTypeName = settings.get("solr.default.type",
                SolrPluginConstants.DEFAULT_TYPE_NAME);

        idFields = settings.getAsArray("solr.idFields", DEFAULT_ID_FIELDS);

        // register update handlers
        // specifying and index and type is optional
        restController.registerHandler(RestRequest.Method.GET, "/_solr/update",
                this);
        restController.registerHandler(RestRequest.Method.GET,
                "/_solr/update/{handler}", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/_solr/update", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/{type}/_solr/update", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/_solr/update", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/_solr/update/{handler}", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_solr/update", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_solr/update", this);
    }

    @Override
    protected void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) {
        final long startTime = System.currentTimeMillis();

        final RestRequest requestEx = new ExtendedRestRequest(request);

        boolean isCommit = false;
        boolean isOptimize = false;

        // get the type of Solr update handler we want to mock, default to xml
        final String contentType = request.header("Content-Type");
        String requestType = null;
        if (contentType != null) {
            if (contentType.indexOf("application/javabin") >= 0) {
                requestType = SolrPluginConstants.JAVABIN_FORMAT_TYPE;
            } else if (contentType.indexOf("application/x-www-form-urlencoded") >= 0) {
                isCommit = requestEx.paramAsBoolean("commit", false);
                isOptimize = requestEx.paramAsBoolean("optimize", false);
                requestType = SolrPluginConstants.NONE_FORMAT_TYPE;
            }
        }
        if (requestType == null) {
            requestType = SolrPluginConstants.XML_FORMAT_TYPE;
        }

        // Requests are typically sent to Solr in batches of documents
        // We can copy that by submitting batch requests to Solr
        final BulkRequest bulkRequest = Requests.bulkRequest();
        final List<DeleteByQueryRequest> deleteQueryList = new ArrayList<DeleteByQueryRequest>();

        // parse and handle the content
        final BytesReference content = requestEx.content();
        if (content.length() == 0) {
            if (TRUE.equalsIgnoreCase(requestEx.param("commit"))
                    || TRUE.equalsIgnoreCase(requestEx.param("softCommit"))
                    || TRUE.equalsIgnoreCase(requestEx.param("prepareCommit"))
                    || StringUtils.isNotBlank(requestEx.param("commitWithin"))) {
                isCommit = true;
            } else if (TRUE.equalsIgnoreCase(requestEx.param("optimize"))) {
                isOptimize = true;
            } else if (TRUE.equalsIgnoreCase(requestEx.param("rollback"))) {
                isCommit = true; // rollback is not supported
            }
        } else if (SolrPluginConstants.XML_FORMAT_TYPE.equals(requestType)) {
            // XML Content
            XMLStreamReader parser = null;
            try {
                // create parser for the content
                parser = inputFactory.createXMLStreamReader(new StringReader(
                        content.toUtf8()));

                // parse the xml
                // we only care about doc and delete tags for now
                boolean stop = false;
                while (!stop) {
                    // get the xml "event"
                    final int event = parser.next();
                    switch (event) {
                    case XMLStreamConstants.END_DOCUMENT:
                        // this is the end of the document
                        // close parser and exit while loop
                        stop = true;
                        break;
                    case XMLStreamConstants.START_ELEMENT:
                        // start of an xml tag
                        // determine if we need to add or delete a document
                        final String currTag = parser.getLocalName();
                        if ("doc".equals(currTag)) {
                            // add a document
                            final Map<String, Object> doc = parseXmlDoc(parser);
                            if (doc != null) {
                                bulkRequest
                                        .add(getIndexRequest(doc, requestEx));
                            }
                        } else if ("delete".equals(currTag)) {
                            // delete a document
                            final List<ActionRequest<?>> requestList = parseXmlDelete(
                                    parser, requestEx);
                            for (final ActionRequest<?> req : requestList) {
                                if (req instanceof DeleteRequest) {
                                    bulkRequest.add(req);
                                } else if (req instanceof DeleteByQueryRequest) {
                                    deleteQueryList
                                            .add((DeleteByQueryRequest) req);
                                }
                            }
                        } else if ("commit".equals(currTag)) {
                            isCommit = true;
                        } else if ("optimize".equals(currTag)) {
                            isOptimize = true;
                        }
                        // rollback is not supported at the moment..
                        break;
                    default:
                        break;
                    }
                }
            } catch (final Exception e) {
                // some sort of error processing the xml input
                logger.error("Error processing xml input", e);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (final IOException e1) {
                    logger.error("Failed to send error response", e1);
                }
                return;
            } finally {
                if (parser != null) {
                    try {
                        parser.close();
                    } catch (final XMLStreamException e) {
                        logger.warn("Failed to close a parser.", e);
                    }
                }
            }
        } else if (SolrPluginConstants.JAVABIN_FORMAT_TYPE.equals(requestType)) {
            // JavaBin Content
            try {
                // We will use the JavaBin codec from solrj
                // unmarshal the input to a SolrUpdate request
                final JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
                final UpdateRequest req = codec.unmarshal(
                        new ByteArrayInputStream(content.toBytes()), null);

                // Get the list of documents to index out of the UpdateRequest
                // Add each document to the bulk request
                // convert the SolrInputDocument into a map which will be used
                // as the ES source field
                final List<SolrInputDocument> docs = req.getDocuments();
                if (docs != null) {
                    for (final SolrInputDocument doc : docs) {
                        bulkRequest.add(getIndexRequest(convertToMap(doc),
                                requestEx));
                    }
                }

                // See if we have any documents to delete
                // if yes, add them to the bulk request
                final List<String> deleteIds = req.getDeleteById();
                if (deleteIds != null) {
                    for (final String id : deleteIds) {
                        bulkRequest.add(getDeleteIdRequest(id, requestEx));
                    }
                }

                final List<String> deleteQueries = req.getDeleteQuery();
                if (deleteQueries != null) {
                    for (final String query : deleteQueries) {
                        deleteQueryList.add(getDeleteQueryRequest(query,
                                requestEx));
                    }
                }

                isCommit = req.getAction() == ACTION.COMMIT;
                isOptimize = req.getAction() == ACTION.OPTIMIZE;
            } catch (final Exception e) {
                // some sort of error processing the javabin input
                logger.error("Error processing javabin input", e);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (final IOException e1) {
                    logger.error("Failed to send error response", e1);
                }
                return;
            }
        }

        // only submit the bulk request if there are index/delete actions
        // it is possible not to have any actions when parsing xml due to the
        // commit and optimize messages that will not generate documents
        if (bulkRequest.numberOfActions() > 0) {
            client.bulk(bulkRequest, new ActionListener<BulkResponse>() {

                // successful bulk request
                @Override
                public void onResponse(final BulkResponse response) {
                    logger.info("Bulk request completed");
                    StringBuilder failureBuf = null;
                    for (final BulkItemResponse itemResponse : response) {
                        final Failure failure = itemResponse.getFailure();
                        if (failure != null) {
                            final String msg = "Index request failed {index:"
                                    + failure.getIndex() + ", type:"
                                    + failure.getType() + ", id:"
                                    + failure.getId() + ", reason:"
                                    + failure.getMessage() + "}";
                            if (failureBuf == null) {
                                failureBuf = new StringBuilder();
                            }
                            failureBuf.append(msg).append('\n');
                        }
                    }

                    if (failureBuf == null) {
                        if (deleteQueryList.isEmpty()) {
                            SolrUpdateRestAction.this.sendResponse(requestEx,
                                    channel, 0, System.currentTimeMillis()
                                            - startTime, null);
                        } else {
                            SolrUpdateRestAction.this.deleteByQueries(client,
                                    requestEx, channel, startTime,
                                    deleteQueryList);
                        }
                    } else {
                        final String failureMsg = failureBuf.toString();
                        logger.error(failureMsg);
                        final NamedList<Object> errorResponse = new SimpleOrderedMap<Object>();
                        errorResponse.add("code", 500);
                        errorResponse.add("msg", failureMsg);
                        SolrUpdateRestAction.this.sendResponse(requestEx,
                                channel, 500, System.currentTimeMillis()
                                        - startTime, errorResponse);
                    }
                }

                // failed bulk request
                @Override
                public void onFailure(final Throwable e) {
                    logger.error("Bulk request failed", e);

                    final NamedList<Object> errorResponse = new SimpleOrderedMap<Object>();
                    errorResponse.add("code", 500);
                    errorResponse.add("msg", e.getMessage());
                    SolrUpdateRestAction.this.sendResponse(requestEx, channel,
                            500, System.currentTimeMillis() - startTime,
                            errorResponse);
                }
            });
        } else if (!deleteQueryList.isEmpty()) {
            deleteByQueries(client, requestEx, channel, startTime,
                    deleteQueryList);
        } else if (isCommit) {
            if (commitAsFlush) {
                final String index = request.hasParam("index") ? request
                        .param("index") : defaultIndexName;
                final FlushRequest flushRequest = new FlushRequest(index);
                client.admin()
                        .indices()
                        .flush(flushRequest,
                                new ActionListener<FlushResponse>() {

                                    @Override
                                    public void onResponse(
                                            final FlushResponse response) {
                                        sendResponse(requestEx, channel, 0,
                                                System.currentTimeMillis()
                                                        - startTime, null);
                                    }

                                    @Override
                                    public void onFailure(final Throwable t) {
                                        try {
                                            channel.sendResponse(new BytesRestResponse(
                                                    channel, t));
                                        } catch (final IOException e) {
                                            logger.error(
                                                    "Failed to send error response",
                                                    e);
                                        }
                                    }
                                });
            } else {
                sendResponse(requestEx, channel, 0, System.currentTimeMillis()
                        - startTime, null);
            }
        } else if (isOptimize) {
            if (optimizeAsOptimize) {
                final String index = request.hasParam("index") ? request
                        .param("index") : defaultIndexName;
                final OptimizeRequest optimizeRequest = new OptimizeRequest(
                        index);
                client.admin()
                        .indices()
                        .optimize(optimizeRequest,
                                new ActionListener<OptimizeResponse>() {

                                    @Override
                                    public void onResponse(
                                            final OptimizeResponse response) {
                                        sendResponse(requestEx, channel, 0,
                                                System.currentTimeMillis()
                                                        - startTime, null);
                                    }

                                    @Override
                                    public void onFailure(final Throwable t) {
                                        try {
                                            channel.sendResponse(new BytesRestResponse(
                                                    channel, t));
                                        } catch (final IOException e) {
                                            logger.error(
                                                    "Failed to send error response",
                                                    e);
                                        }
                                    }
                                });
            } else {
                sendResponse(requestEx, channel, 0, System.currentTimeMillis()
                        - startTime, null);
            }
        } else {
            try {
                channel.sendResponse(new BytesRestResponse(channel,
                        new UnsupportedOperationException(
                                "Unsupported request: " + requestEx.toString())));
            } catch (final IOException e) {
                logger.error("Failed to send error response", e);
            }
        }
    }

    private void deleteByQueries(final Client client,
            final RestRequest request, final RestChannel channel,
            final long startTime,
            final List<DeleteByQueryRequest> deleteQueryList) {
        final AtomicInteger counter = new AtomicInteger(deleteQueryList.size());
        final StringBuilder failureBuf = new StringBuilder(200);
        for (final DeleteByQueryRequest deleteQueryRequest : deleteQueryList) {
            client.deleteByQuery(deleteQueryRequest,
                    new ActionListener<DeleteByQueryResponse>() {

                        @Override
                        public void onResponse(
                                final DeleteByQueryResponse response) {
                            if (counter.decrementAndGet() == 0) {
                                if (failureBuf.length() == 0) {
                                    SolrUpdateRestAction.this.sendResponse(
                                            request, channel, 0,
                                            System.currentTimeMillis()
                                                    - startTime, null);
                                } else {
                                    final NamedList<Object> errorResponse = new SimpleOrderedMap<Object>();
                                    errorResponse.add("code", 500);
                                    errorResponse.add("msg",
                                            failureBuf.toString());
                                    SolrUpdateRestAction.this.sendResponse(
                                            request, channel, 500,
                                            System.currentTimeMillis()
                                                    - startTime, errorResponse);
                                }
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            logger.error("DeleteByQuery request failed", t);
                            if (counter.decrementAndGet() == 0) {
                                failureBuf.append(t.getMessage());
                            }
                        }
                    });
        }
    }

    /**
     * Sends a dummy response to the Solr client
     *
     * @param request
     *            ES rest request
     * @param channel
     *            ES rest channel
     */
    private void sendResponse(final RestRequest request,
            final RestChannel channel, final int status, final long qTime,
            final NamedList<Object> errorResponse) {
        // create NamedList with dummy Solr response
        final NamedList<Object> solrResponse = new SimpleOrderedMap<Object>();
        final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
        responseHeader.add("status", status);
        responseHeader.add("QTime", (int) qTime);
        solrResponse.add("responseHeader", responseHeader);
        if (errorResponse != null) {
            solrResponse.add("error", errorResponse);
        }

        // send the dummy response
        SolrResponseUtils.writeResponse(solrResponse, request, channel);
    }

    /**
     * Generates an ES DeleteRequest object based on the Solr document id
     *
     * @param id
     *            the Solr document id
     * @param request
     *            the ES rest request
     * @return the ES delete request
     */
    private DeleteRequest getDeleteIdRequest(final String id,
            final RestRequest request) {

        // get the index and type we want to execute this delete request on
        final String index = request.hasParam("index") ? request.param("index")
                : "solr";
        final String type = request.hasParam("type") ? request.param("type")
                : "docs";

        // create the delete request object
        final DeleteRequest deleteRequest = new DeleteRequest(index, type,
                getId(id));
        deleteRequest.parent(request.param("parent"));

        // TODO: this was causing issues, do we need it?
        // deleteRequest.version(RestActions.parseVersion(request));
        // deleteRequest.versionType(VersionType.fromString(request.param("version_type"),
        // deleteRequest.versionType()));

        deleteRequest.routing(request.param("routing"));

        return deleteRequest;
    }

    private DeleteByQueryRequest getDeleteQueryRequest(final String query,
            final RestRequest request) {

        // get the index and type we want to execute this delete request on
        final String index = request.hasParam("index") ? request.param("index")
                : "solr";

        // create the delete request object
        final DeleteByQueryRequest deleteRequest = Requests
                .deleteByQueryRequest(index);
        deleteRequest
                .source("{\"query_string\":{\"query\":\"" + query + "\"}}");

        deleteRequest.routing(request.param("routing"));

        return deleteRequest;
    }

    /**
     * Converts a SolrInputDocument into an ES IndexRequest
     *
     * @param doc
     *            the Solr input document to convert
     * @param request
     *            the ES rest request
     * @return the ES index request object
     */
    private IndexRequest getIndexRequest(final Map<String, Object> doc,
            final RestRequest request) {
        // get the index and type we want to index the document in
        final String index = request.hasParam("index") ? request.param("index")
                : defaultIndexName;
        final String type = request.hasParam("type") ? request.param("type")
                : defaultTypeName;

        // Get the id from request or if not available generate an id for the
        // document
        final String id = request.hasParam("id") ? request.param("id")
                : getIdForDoc(doc);

        // create an IndexRequest for this document
        final IndexRequest indexRequest = new IndexRequest(index, type, id);
        indexRequest.routing(request.param("routing"));
        indexRequest.parent(request.param("parent"));
        indexRequest.source(doc);
        indexRequest.timeout(request.paramAsTime("timeout",
                ShardReplicationOperationRequest.DEFAULT_TIMEOUT));
        indexRequest.refresh(request.paramAsBoolean("refresh",
                indexRequest.refresh()));

        // TODO: this caused issues, do we need it?
        // indexRequest.version(RestActions.parseVersion(request));
        // indexRequest.versionType(VersionType.fromString(request.param("version_type"),
        // indexRequest.versionType()));

        indexRequest.opType(IndexRequest.OpType.INDEX);

        // TODO: force creation of index, do we need it?
        // indexRequest.create(true);

        final String replicationType = request.param("replication");
        if (replicationType != null) {
            indexRequest.replicationType(ReplicationType
                    .fromString(replicationType));
        }

        final String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            indexRequest.consistencyLevel(WriteConsistencyLevel
                    .fromString(consistencyLevel));
        }

        // we just send a response, no need to fork
        indexRequest.listenerThreaded(true);

        // we don't spawn, then fork if local
        indexRequest.operationThreaded(true);

        return indexRequest;
    }

    /**
     * Generates document id. A Solr document id may not be a valid ES id, so we
     * attempt to find the Solr document id and convert it into a valid ES
     * document id. We keep the original Solr id so the document can be found
     * and deleted later if needed.
     *
     * We check for Solr document id's in the following fields: id, docid,
     * documentid, contentid, uuid, url
     *
     * If no id is found, we generate a random one.
     *
     * @param doc
     *            the input document
     * @return the generated document id
     */
    private String getIdForDoc(final Map<String, Object> doc) {
        // start with a random id
        String id = null;

        // scan the input document for an id
        for (final String idField : idFields) {
            if (doc.containsKey(idField)) {
                id = doc.get(idField).toString();
                break;
            }
        }

        if (id == null) {
            id = UUID.randomUUID().toString();
        }

        // always store the id back into the "id" field
        // so we can get it back in results
        doc.put("id", id);

        // return the id which is the md5 of either the
        // random uuid or id found in the input document.
        return getId(id);
    }

    /**
     * Return the given id or a hashed version thereof, based on the plugin
     * configuration
     *
     * @param id
     * @return
     */
    private final String getId(final String id) {
        return hashIds ? getMD5(id) : id;
    }

    /**
     * Calculates the md5 hex digest of the given input string
     *
     * @param input
     *            the string to md5
     * @return the md5 hex digest
     */
    private String getMD5(final String input) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            final byte[] bytes = input
                    .getBytes(SolrPluginConstants.CHARSET_UTF8);
            final byte[] digest = md.digest(bytes);
            final char[] encodeHex = Hex.encodeHex(digest);
            return String.valueOf(encodeHex);
        } catch (final NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Failed to encode " + input, e);
        }
    }

    /**
     * Converts a SolrInputDocument into a Map
     *
     * @param doc
     *            the SolrInputDocument to convert
     * @return the input document as a map
     */
    private Map<String, Object> convertToMap(final SolrInputDocument doc) {
        // create the Map we will put the fields in
        final Map<String, Object> newDoc = new HashMap<String, Object>();

        // loop though all the fields and insert them into the map
        final Collection<SolrInputField> fields = doc.values();
        if (fields != null) {
            for (final SolrInputField field : fields) {
                newDoc.put(field.getName(), field.getValue());
            }
        }

        return newDoc;
    }

    /**
     * Reads a SolrXML document into a map of fields
     *
     * @param parser
     *            the xml parser
     * @return the document as a map
     * @throws XMLStreamException
     */
    private Map<String, Object> parseXmlDoc(final XMLStreamReader parser)
            throws XMLStreamException {
        Map<String, Object> doc = new HashMap<String, Object>();
        final StringBuilder buf = new StringBuilder();
        String name = null;
        boolean stop = false;
        // infinite loop until we are done parsing the document or an error
        // occurs
        while (!stop) {
            final int event = parser.next();
            switch (event) {
            case XMLStreamConstants.START_ELEMENT:
                buf.setLength(0);
                final String localName = parser.getLocalName();
                // we are looking for field elements only
                if (!"field".equals(localName)) {
                    logger.warn("unexpected xml tag /doc/" + localName);
                    doc = null;
                    stop = true;
                }

                // get the name attribute of the field
                String attrName = "";
                String attrVal = "";
                for (int i = 0; i < parser.getAttributeCount(); i++) {
                    attrName = parser.getAttributeLocalName(i);
                    attrVal = parser.getAttributeValue(i);
                    if ("name".equals(attrName)) {
                        name = attrVal;
                    }
                }
                break;
            case XMLStreamConstants.END_ELEMENT:
                if ("doc".equals(parser.getLocalName())) {
                    // we are done parsing the doc
                    // break out of loop
                    stop = true;
                } else if ("field".equals(parser.getLocalName())) {
                    // put the field value into the map
                    // handle multiple values by putting them into a list
                    if (doc.containsKey(name) && doc.get(name) instanceof List) {
                        @SuppressWarnings("unchecked")
                        final List<String> vals = (List<String>) doc.get(name);
                        vals.add(buf.toString());
                        doc.put(name, vals);
                    } else if (doc.containsKey(name)) {
                        final List<String> vals = new ArrayList<String>();
                        vals.add((String) doc.get(name));
                        vals.add(buf.toString());
                        doc.put(name, vals);
                    } else {
                        doc.put(name, buf.toString());
                    }
                }
                break;
            case XMLStreamConstants.SPACE:
            case XMLStreamConstants.CDATA:
            case XMLStreamConstants.CHARACTERS:
                // save all text data
                buf.append(parser.getText());
                break;
            default:
                break;
            }
        }

        // return the parsed doc
        return doc;
    }

    /**
     * Parse the document id out of the SolrXML delete command
     *
     * @param parser
     *            the xml parser
     * @return the document id to delete
     * @throws XMLStreamException
     */
    private List<ActionRequest<?>> parseXmlDelete(final XMLStreamReader parser,
            final RestRequest request) throws XMLStreamException {
        final StringBuilder buf = new StringBuilder();
        boolean stop = false;
        final List<ActionRequest<?>> requestList = new ArrayList<ActionRequest<?>>();
        // infinite loop until we get docid or error
        while (!stop) {
            final int event = parser.next();
            switch (event) {
            case XMLStreamConstants.START_ELEMENT:
                buf.setLength(0);
                break;
            case XMLStreamConstants.END_ELEMENT:
                final String currTag = parser.getLocalName();
                if ("id".equals(currTag)) {
                    final String docid = buf.toString();
                    requestList.add(getDeleteIdRequest(docid, request));
                } else if ("query".equals(currTag)) {
                    final String query = buf.toString();
                    requestList.add(getDeleteQueryRequest(query, request));
                } else if ("delete".equals(currTag)) {
                    // done parsing, exit loop
                    stop = true;
                } else {
                    logger.warn("unexpected xml tag /delete/" + currTag);
                }
                break;
            case XMLStreamConstants.SPACE:
            case XMLStreamConstants.CDATA:
            case XMLStreamConstants.CHARACTERS:
                // save all text data (this is the id)
                buf.append(parser.getText());
                break;
            }
        }

        // return the extracted docid
        return requestList;
    }
}
