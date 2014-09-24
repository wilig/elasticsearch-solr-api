package org.codelibs.elasticsearch.solr.rest;

import static org.elasticsearch.common.unit.ByteSizeValue.*;
import static org.elasticsearch.common.unit.TimeValue.*;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

/**
 * @author shinsuke
 *
 */
public class ExtendedRestRequest extends RestRequest {
    private static ESLogger logger = ESLoggerFactory
            .getLogger(ExtendedRestRequest.class.getName());

    private RestRequest parent;

    private volatile Map<String, List<String>> paramMap;

    public ExtendedRestRequest(final RestRequest request) {
        parent = request;
    }

    private String getPath(final String uri) {
        final int pathEndPos = uri.indexOf('?');
        if (pathEndPos < 0) {
            return uri;
        } else {
            return uri.substring(0, pathEndPos);
        }
    }

    private Map<String, List<String>> decodeParams(final String s,
            final String charset) {
        final Map<String, List<String>> params = new LinkedHashMap<String, List<String>>();
        String name = null;
        int pos = 0; // Beginning of the unprocessed region
        int i; // End of the unprocessed region
        char c = 0; // Current character
        for (i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            if (c == '=' && name == null) {
                if (pos != i) {
                    name = decodeComponent(s.substring(pos, i), charset);
                }
                pos = i + 1;
            } else if (c == '&') {
                if (name == null && pos != i) {
                    // We haven't seen an `=' so far but moved forward.
                    // Must be a param of the form '&a&' so add it with
                    // an empty value.
                    addParam(params,
                            decodeComponent(s.substring(pos, i), charset), "");
                } else if (name != null) {
                    addParam(params, name,
                            decodeComponent(s.substring(pos, i), charset));
                    name = null;
                }
                pos = i + 1;
            }
        }

        if (pos != i) { // Are there characters we haven't dealt with?
            if (name == null) { // Yes and we haven't seen any `='.
                addParam(params, decodeComponent(s.substring(pos, i), charset),
                        "");
            } else { // Yes and this must be the last value.
                addParam(params, name,
                        decodeComponent(s.substring(pos, i), charset));
            }
        } else if (name != null) { // Have we seen a name without value?
            addParam(params, name, "");
        }

        return params;
    }

    private String decodeComponent(final String s, final String charset) {
        if (s == null) {
            return "";
        }

        try {
            return URLDecoder.decode(s, charset);
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to decode {} by {}", s, charset);
            }
            return s;
        }
    }

    private static void addParam(final Map<String, List<String>> params,
            final String name, final String value) {
        List<String> values = params.get(name);
        if (values == null) {
            values = new ArrayList<String>(1);
            params.put(name, values);
        }
        values.add(value);
    }

    @Override
    public String param(final String key, final String defaultValue) {
        final List<String> list = getParameterMap().get(key);
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return defaultValue;
    }

    @Override
    public Method method() {
        return parent.method();
    }

    @Override
    public String uri() {
        return parent.uri();
    }

    @Override
    public String rawPath() {
        return parent.rawPath();
    }

    @Override
    public boolean hasContent() {
        return parent.hasContent();
    }

    @Override
    public boolean contentUnsafe() {
        return parent.contentUnsafe();
    }

    @Override
    public BytesReference content() {
        return parent.content();
    }

    @Override
    public String header(final String name) {
        return parent.header(name);
    }

    @Override
    public Iterable<Map.Entry<String, String>> headers() {
        return parent.headers();
    }

    @Override
    public boolean hasParam(final String key) {
        return getParameterMap().containsKey(key);
    }

    @Override
    public String param(final String key) {
        return param(key, null);
    }

    @Override
    public String[] paramAsStringArray(final String key,
            final String[] defaultValue) {
        final List<String> list = getParameterMap().get(key);
        if (list != null) {
            if (list.size() == 1) {
                return Strings.splitStringByCommaToArray(list.get(0));
            }
            return list.toArray(new String[list.size()]);
        }
        return defaultValue;
    }

    @Override
    public float paramAsFloat(final String key, final float defaultValue) {
        final String value = param(key, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Float.parseFloat(value);
        } catch (final NumberFormatException e) {
            throw new ElasticsearchIllegalArgumentException(
                    "Failed to parse float parameter [" + key
                            + "] with value [" + value + "]", e);
        }
    }

    @Override
    public int paramAsInt(final String key, final int defaultValue) {
        final String value = param(key, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new ElasticsearchIllegalArgumentException(
                    "Failed to parse int parameter [" + key + "] with value ["
                            + value + "]", e);
        }
    }

    @Override
    public long paramAsLong(final String key, final long defaultValue) {
        final String value = param(key, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            throw new ElasticsearchIllegalArgumentException(
                    "Failed to parse long parameter [" + key + "] with value ["
                            + value + "]", e);
        }
    }

    @Override
    public boolean paramAsBoolean(final String key, final boolean defaultValue) {
        final String value = param(key, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Boolean.parseBoolean(value);
        } catch (final NumberFormatException e) {
            throw new ElasticsearchIllegalArgumentException(
                    "Failed to parse boolean parameter [" + key
                            + "] with value [" + value + "]", e);
        }
    }

    @Override
    public Boolean paramAsBooleanOptional(final String key,
            final Boolean defaultValue) {
        final String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        return !("false".equals(sValue) || "0".equals(sValue) || "off"
                .equals(sValue));
    }

    @Override
    public TimeValue paramAsTime(final String key, final TimeValue defaultValue) {
        return parseTimeValue(param(key), defaultValue);
    }

    @Override
    public ByteSizeValue paramAsSize(final String key,
            final ByteSizeValue defaultValue) {
        return parseBytesSizeValue(param(key), defaultValue);
    }

    @Override
    public Map<String, String> params() {
        final Map<String, String> map = new HashMap<String, String>();
        for (final Map.Entry<String, List<String>> entry : getParameterMap()
                .entrySet()) {
            final List<String> valueList = entry.getValue();
            if (valueList != null) {
                final StringBuilder buf = new StringBuilder();
                for (final String value : valueList) {
                    if (buf.length() != 0) {
                        buf.append(',');
                    }
                    buf.append(value);
                }
                map.put(entry.getKey(), buf.toString());
            } else {
                map.put(entry.getKey(), null);
            }
        }
        return map;
    }

    private Map<String, List<String>> getParameterMap() {
        if (paramMap == null) {
            synchronized (this) {
                initParameterMap();
            }
        }
        return paramMap;
    }

    private boolean isAppendPostData(final String contentType) {
        if (contentType == null) {
            return true;
        }
        return contentType.indexOf("application/javabin") < 0
                && contentType.indexOf("application/xml") < 0;
    }

    private void initParameterMap() {
        final StringBuilder uriBuf = new StringBuilder(200);
        uriBuf.append(parent.uri());
        final boolean isPost = parent.method() == RestRequest.Method.POST;
        if (isPost && isAppendPostData(parent.header("Content-Type"))) {
            if (parent.uri().indexOf('?') >= 0) {
                uriBuf.append('&');
            } else {
                uriBuf.append('?');
            }
            uriBuf.append(parent.content().toUtf8());
        }

        Map<String, List<String>> requestParamMap;
        final String uri = uriBuf.toString();
        final int pathLength = getPath(uri).length();
        if (uri.length() == pathLength) {
            requestParamMap = new LinkedHashMap<String, List<String>>();
        } else {

            String charset = parent.header("Accept-Charset");
            if (charset == null) {
                charset = "UTF-8";
            }

            requestParamMap = decodeParams(uri.substring(pathLength + 1),
                    charset);
        }

        for (final Map.Entry<String, String> entry : parent.params().entrySet()) {
            if (!requestParamMap.containsKey(entry.getKey())) {
                final List<String> list = new ArrayList<String>(1);
                list.add(entry.getValue());
                requestParamMap.put(entry.getKey(), list);
            }
        }
        paramMap = requestParamMap;
    }

}
