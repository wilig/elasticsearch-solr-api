/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.solr.solr;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;
import org.apache.solr.response.SolrQueryResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

/**
 * Writes objects to xml. This class is taken directly out of the Solr source
 * code and modified to remove the stuff we do not need for the plugin.
 *
 */
final public class XMLWriter {

    // //////////////////////////////////////////////////////////
    // request instance specific (non-static, not shared between threads)
    // //////////////////////////////////////////////////////////

    private final Writer writer;

    private final DateTimeFormatter dateFormat = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public XMLWriter(final Writer writer) {
        this.writer = writer;
    }

    /**
     * Writes the XML attribute name/val. A null val means that the attribute is
     * missing.
     */
    public void writeAttr(final String name, final String val)
            throws IOException {
        this.writeAttr(name, val, true);
    }

    public void writeAttr(final String name, final String val,
            final boolean escape) throws IOException {
        if (val != null) {
            writer.write(' ');
            writer.write(name);
            writer.write("=\"");
            if (escape) {
                XML.escapeAttributeValue(val, writer);
            } else {
                writer.write(val);
            }
            writer.write('"');
        }
    }

    /**
     * Writes a tag with attributes
     *
     * @param tag
     * @param attributes
     * @param closeTag
     * @param escape
     * @throws IOException
     */
    public void startTag(final String tag,
            final Map<String, String> attributes, final boolean closeTag,
            final boolean escape) throws IOException {
        writer.write('<');
        writer.write(tag);
        if (!attributes.isEmpty()) {
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.writeAttr(entry.getKey(), entry.getValue(), escape);
            }
        }
        if (closeTag) {
            writer.write("/>");
        } else {
            writer.write('>');
        }
    }

    /**
     * Write a complete tag w/ attributes and cdata (the cdata is not enclosed
     * in $lt;!CDATA[]!&gt;
     *
     * @param tag
     * @param attributes
     * @param cdata
     * @param escapeCdata
     * @param escapeAttr
     * @throws IOException
     */
    public void writeCdataTag(final String tag,
            final Map<String, String> attributes, final String cdata,
            final boolean escapeCdata, final boolean escapeAttr)
            throws IOException {
        writer.write('<');
        writer.write(tag);
        if (!attributes.isEmpty()) {
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.writeAttr(entry.getKey(), entry.getValue(), escapeAttr);
            }
        }
        writer.write('>');
        if (cdata != null && cdata.length() > 0) {
            if (escapeCdata) {
                XML.escapeCharData(cdata, writer);
            } else {
                writer.write(cdata, 0, cdata.length());
            }
        }
        writer.write("</");
        writer.write(tag);
        writer.write('>');
    }

    public void startTag(final String tag, final String name,
            final boolean closeTag) throws IOException {
        writer.write('<');
        writer.write(tag);
        if (name != null) {
            this.writeAttr("name", name);
            if (closeTag) {
                writer.write("/>");
            } else {
                writer.write(">");
            }
        } else {
            if (closeTag) {
                writer.write("/>");
            } else {
                writer.write('>');
            }
        }
    }

    /**
     * @since solr 1.3
     */
    final void writeDoc(final String name, final SolrDocument doc,
            final Set<String> returnFields, final boolean includeScore)
            throws IOException {
        this.startTag("doc", name, false);

        if (includeScore && returnFields != null) {
            returnFields.add("score");
        }

        for (final String fname : doc.getFieldNames()) {
            if (returnFields != null && !returnFields.contains(fname)) {
                continue;
            }
            final Object val = doc.getFieldValue(fname);

            writeVal(fname, val);
        }

        writer.write("</doc>");
    }

    private static interface DocumentListInfo {
        Float getMaxScore();

        int getCount();

        long getNumFound();

        long getStart();

        void writeDocs(boolean includeScore, Set<String> fields)
                throws IOException;
    }

    private final void writeDocuments(final String name,
            final DocumentListInfo docs, final Set<String> fields) throws IOException {
        boolean includeScore = false;
        Set<String> value = fields;
        if (fields != null) {
            includeScore = fields.contains("score");
            if (fields.size() == 0 || fields.size() == 1 && includeScore
                    || fields.contains("*")) {
                value = null; // null means return all stored fields
            }
        }

        final int sz = docs.getCount();

        writer.write("<result");
        this.writeAttr("name", name);
        this.writeAttr("numFound", Long.toString(docs.getNumFound())); // TODO:
        // change
        // to
        // long
        this.writeAttr("start", Long.toString(docs.getStart())); // TODO: change
        // to long
        if (includeScore && docs.getMaxScore() != null) {
            this.writeAttr("maxScore", Float.toString(docs.getMaxScore()));
        }
        if (sz == 0) {
            writer.write("/>");
            return;
        } else {
            writer.write('>');
        }

        docs.writeDocs(includeScore, value);

        writer.write("</result>");
    }

    public final void writeSolrDocumentList(final String name,
            final SolrDocumentList docs, final Set<String> fields)
            throws IOException {
        writeDocuments(name, new DocumentListInfo() {
            @Override
            public int getCount() {
                return docs.size();
            }

            @Override
            public Float getMaxScore() {
                return docs.getMaxScore();
            }

            @Override
            public long getNumFound() {
                return docs.getNumFound();
            }

            @Override
            public long getStart() {
                return docs.getStart();
            }

            @Override
            public void writeDocs(final boolean includeScore,
                    final Set<String> fields) throws IOException {
                for (final SolrDocument doc : docs) {
                    XMLWriter.this.writeDoc(null, doc, fields, includeScore);
                }
            }
        }, fields);
    }

    public void writeVal(final String name, final Object val)
            throws IOException {

        // if there get to be enough types, perhaps hashing on the type
        // to get a handler might be faster (but types must be exact to do
        // that...)

        // go in order of most common to least common
        if (val == null) {
            writeNull(name);
        } else if (val instanceof String) {
            writeStr(name, (String) val);
        } else if (val instanceof Integer) {
            // it would be slower to pass the int ((Integer)val).intValue()
            this.writeInt(name, val.toString());
        } else if (val instanceof Boolean) {
            // could be optimized... only two vals
            this.writeBool(name, val.toString());
        } else if (val instanceof Long) {
            this.writeLong(name, val.toString());
        } else if (val instanceof Date) {
            this.writeDate(name, (Date) val);
        } else if (val instanceof Float) {
            // we pass the float instead of using toString() because
            // it may need special formatting. same for double.
            this.writeFloat(name, ((Float) val).floatValue());
        } else if (val instanceof Double) {
            this.writeDouble(name, ((Double) val).doubleValue());
        } else if (val instanceof SolrDocumentList) {
            // requires access to IndexReader
            writeSolrDocumentList(name, (SolrDocumentList) val, null);
        } else if (val instanceof Map) {
            writeMap(name, (Map) val);
        } else if (val instanceof NamedList) {
            writeNamedList(name, (NamedList) val);
        } else if (val instanceof Iterable) {
            this.writeArray(name, ((Iterable) val).iterator());
        } else if (val instanceof Object[]) {
            this.writeArray(name, (Object[]) val);
        } else if (val instanceof Iterator) {
            this.writeArray(name, (Iterator) val);
        } else {
            // default...
            writeStr(name, val.getClass().getName() + ':' + val.toString());
        }
    }

    //
    // Generic compound types
    //

    public void writeNamedList(final String name, final NamedList val)
            throws IOException {
        final int sz = val.size();
        this.startTag("lst", name, sz <= 0);

        for (int i = 0; i < sz; i++) {
            writeVal(val.getName(i), val.getVal(i));
        }

        if (sz > 0) {
            writer.write("</lst>");
        }
    }

    /**
     * writes a Map in the same format as a NamedList, using the stringification
     * of the key Object when it's non-null.
     *
     * @param name
     * @param map
     * @throws IOException
     * @see SolrQueryResponse Note on Returnable Data
     */
    public void writeMap(final String name, final Map<Object, Object> map)
            throws IOException {
        final int sz = map.size();
        this.startTag("lst", name, sz <= 0);

        for (final Map.Entry<Object, Object> entry : map.entrySet()) {
            final Object k = entry.getKey();
            final Object v = entry.getValue();
            // if (sz<indentThreshold) indent();
            writeVal(null == k ? null : k.toString(), v);
        }

        if (sz > 0) {
            writer.write("</lst>");
        }
    }

    public void writeArray(final String name, final Object[] val)
            throws IOException {
        this.writeArray(name, Arrays.asList(val).iterator());
    }

    public void writeArray(final String name, final Iterator iter)
            throws IOException {
        if (iter.hasNext()) {
            this.startTag("arr", name, false);

            while (iter.hasNext()) {
                writeVal(null, iter.next());
            }

            writer.write("</arr>");
        } else {
            this.startTag("arr", name, true);
        }
    }

    //
    // Primitive types
    //

    public void writeNull(final String name) throws IOException {
        writePrim("null", name, "", false);
    }

    public void writeStr(final String name, final String val)
            throws IOException {
        writePrim("str", name, val, true);
    }

    public void writeInt(final String name, final String val)
            throws IOException {
        writePrim("int", name, val, false);
    }

    public void writeInt(final String name, final int val) throws IOException {
        this.writeInt(name, Integer.toString(val));
    }

    public void writeLong(final String name, final String val)
            throws IOException {
        writePrim("long", name, val, false);
    }

    public void writeLong(final String name, final long val) throws IOException {
        this.writeLong(name, Long.toString(val));
    }

    public void writeBool(final String name, final String val)
            throws IOException {
        writePrim("bool", name, val, false);
    }

    public void writeBool(final String name, final boolean val)
            throws IOException {
        this.writeBool(name, Boolean.toString(val));
    }

    public void writeShort(final String name, final String val)
            throws IOException {
        writePrim("short", name, val, false);
    }

    public void writeShort(final String name, final short val)
            throws IOException {
        this.writeInt(name, Short.toString(val));
    }

    public void writeByte(final String name, final String val)
            throws IOException {
        writePrim("byte", name, val, false);
    }

    public void writeByte(final String name, final byte val) throws IOException {
        this.writeInt(name, Byte.toString(val));
    }

    public void writeFloat(final String name, final String val)
            throws IOException {
        writePrim("float", name, val, false);
    }

    public void writeFloat(final String name, final float val)
            throws IOException {
        this.writeFloat(name, Float.toString(val));
    }

    public void writeDouble(final String name, final String val)
            throws IOException {
        writePrim("double", name, val, false);
    }

    public void writeDouble(final String name, final double val)
            throws IOException {
        this.writeDouble(name, Double.toString(val));
    }

    public void writeDate(final String name, final Date val) throws IOException {
        // updated to use Joda time
        this.writeDate(name, new DateTime(val).toString(dateFormat));
    }

    public void writeDate(final String name, final String val)
            throws IOException {
        writePrim("date", name, val, false);
    }

    //
    // OPT - specific writeInt, writeFloat, methods might be faster since
    // there would be less write calls (write("<int name=\"" + name + ... +
    // </int>)
    //
    public void writePrim(final String tag, final String name,
            final String val, final boolean escape) throws IOException {
        // OPT - we could use a temp char[] (or a StringBuilder) and if the
        // size was small enough to fit (if escape==false we can calc exact
        // size)
        // then we could put things directly in the temp buf.
        // need to see what percent of CPU this takes up first though...
        // Could test a reusable StringBuilder...

        // is this needed here???
        // Only if a fieldtype calls writeStr or something
        // with a null val instead of calling writeNull
        /***
         * if (val==null) { if (name==null) writer.write("<null/>"); else
         * writer.write("<null name=\"" + name + "/>"); }
         ***/

        final int contentLen = val.length();

        this.startTag(tag, name, contentLen == 0);
        if (contentLen == 0) {
            return;
        }

        if (escape) {
            XML.escapeCharData(val, writer);
        } else {
            writer.write(val, 0, contentLen);
        }

        writer.write("</");
        writer.write(tag);
        writer.write('>');
    }

}
