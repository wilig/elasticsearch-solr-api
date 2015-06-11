package org.codelibs.elasticsearch.solr.solr;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.DateField;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;

/**
 * Writes a NamedList&lt;Object&gt; to json. This class is derived from Solr original classes
 * org.apache.solr.response.JSONWriter and org.apache.solr.response.
 * 
 * This is an example if a typical use of this class:
 * <pre>
 * {@code
 * // Let's say you already have a NamedList instance and want to writ it as a JSON string:
 * Writer writer = new StringWriter();
 * JSONWriter jw = new JSONWriter(writer);
 * jw.write(namedListInstance);
 * writer.close();
 * String strJson =writer.toString(); 
 * }</pre>
 * 
 * @author Paulo Amaral
 */
@SuppressWarnings("deprecation")
public class JSONWriter {


	
	/*****************
	 **  CONSTANTS  **
	 *****************/

	// NAMEDLIST STYLES
	
	private static final String JSON_NL_STYLE = "json.nl";
	private static final String JSON_NL_MAP = "map";
	private static final String JSON_NL_FLAT = "flat";
	private static final String JSON_NL_ARROFARR = "arrarr";
	private static final String JSON_NL_ARROFMAP = "arrmap";
	private static final String JSON_WRAPPER_FUNCTION = "json.wrf";
	
	// STRUCTURE TOKENS
	
	private static final String MAP_OPENER = "{";
	private static final String MAP_SEPARATOR = ",";
	private static final String MAP_CLOSER = "}";
	private static final String MAP_KEY_ATTRIBUTION = ":";
	private static final String ARRAY_OPENER = "[";
	private static final String ARRAY_SEPARATOR = ",";
	private static final String ARRAY_CLOSER = "]";
	
	
	
	
	/******************
	 **  ATTRIBUTES  **
	 ******************/

	private Writer _writer = null;
	private String _namedListStyle = "";
	private ReturnFields _returnFields = null;
	
	
	
	/***************************
	 **  GETTERS 'n' SETTERS  **
	 ***************************/
	
	/**
	 * The Writer where the JSON will be rendered. It is set in the constructor.
	 * @return
	 */
	public Writer getWriter() {
		return _writer;
	}
	
	public void setNamedListStyle(String value) {
		_namedListStyle = value;
	}
	
	public String getNamedListStyle() {
		return _namedListStyle;
	}

	
	
	/********************
	 **  CONSTRUCTION  **
	 ********************/
	
	/**
	 * Constructor
	 * @param writer Writer used to render the JSON. A StringWriter instance in most cases.
	 */
	public JSONWriter(Writer writer) {
		this._writer = writer;
	}
	
	
	
	
	/*****************************************
	 **  ENTRY POINT FOR NamedList WRITING  **
	 *****************************************/

	/**
	 * Writes the named list as a JSON in the Writer provided in constructor
	 * @param namedList The Named List to write
	 * @throws IOException
	 */
	public void write(NamedList<Object> namedList) throws IOException {
		writeNamedList(namedList);
	}

	/**
	 * Writes a named list as a JSON attribute, using the style given by setNamedListStyle()
	 * @param val the NamedList object
	 * @throws IOException
	 */
	protected void writeNamedList(NamedList<Object> val) throws IOException {
		if (_namedListStyle.isEmpty() && val instanceof SimpleOrderedMap) {
			writeNamedListAsMap(val);
		} else if (_namedListStyle.equals(JSON_NL_FLAT)) {
			writeNamedListAsFlat(val);
		} else if (_namedListStyle.equals(JSON_NL_MAP)) {
			writeNamedListAsMap(val);
		} else if (_namedListStyle.equals(JSON_NL_ARROFARR)) {
			writeNamedListAsArrArr(val);
		} else if (_namedListStyle.equals(JSON_NL_ARROFMAP)) {
			writeNamedListAsArrMap(val);
		}
	}
	

	/**
	 * Write a Named List as a Map (Json style). If repeated keys are found, they will be rendered
	 * together as a single array. 
	 * @param val the NamedList instance
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	protected void writeNamedListAsMap(NamedList<Object> val)
			throws IOException {
		int sz = val.size();
		int size = 0;

		// This map will have the NamedList reorganized: if the key is present just once,
		// store the key's value. if the key repeats, the map will have a single entry
		// for this key and associated to it an array.
		Map<String, Object> map = new LinkedHashMap<String, Object>(sz);
		for (int i = 0; i < sz; i++) {
			// gets key and value
			String key = val.getName(i);
			if (key == null) key = "";
			Object value = val.getVal(i);

			if (!map.containsKey(key)) {
				map.put(key, value);
				size++;
			} else {
				// we are going through this key again... Time to use an array!
				if (map.get(key) instanceof ArrayList<?>) {
					// The map's value is an array already. Just add the item to it.
					((ArrayList<Object>)map.get(key)).add(value);
				} else {
					// This is the second time reading this key, so the map's value is still
					// a single object. Need to be converted into an array.
					Object oldValueStored = map.get(key);
					ArrayList<Object> newArray = new ArrayList<Object>();
					newArray.add(oldValueStored);
					newArray.add(value);
					map.remove(key);
					map.put(key, newArray);
				}
			}
		}
		
		// Now that this NamedList is better organized, lets write everything properly
		writeMapOpener();
		Boolean first = true;
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			
			// Don't want to read empty slots in the map
			if (size-- == 0) break;
			
			if (first) {
				first = false;
			} else {
				writeMapSeparator();
			}
			
			writeKey(entry.getKey(), true);
			writeVal(entry.getValue());
		}
		writeMapCloser();
		
	}

	/**
	 * Represents a NamedList directly as a JSON Object (essentially a Map) Map
	 * null to "" and name mangle any repeated keys to avoid repeats in the
	 * output.
	 * @param val the NamedList instance
	 * @throws IOException
	 */
	protected void writeNamedListAsMapMangled(NamedList<Object> val)
			throws IOException {
		
		int sz = val.size();
		writeMapOpener();
		
		// ---------------------------------------------------------------------------
		// COMMENTS FROM THE ORIGINAL CLASS: 
		// In JSON objects (maps) we can't have null keys or duplicates...
		// map null to "" and append a qualifier to duplicates.
		//
		// a=123,a=456 will be mapped to {a=1,a__1=456}
		// Disad: this is ambiguous since a real key could be called a__1
		//
		// Another possible mapping could aggregate multiple keys to an array:
		// a=123,a=456 maps to a=[123,456]
		// Disad: this is ambiguous with a real single value that happens to be
		// an array
		//
		// Both of these mappings have ambiguities.
		// ---------------------------------------------------------------------------
		HashMap<String, Integer> repeats = new HashMap<>(4);

		boolean first = true;
		for (int i = 0; i < sz; i++) {
			String key = val.getName(i);
			if (key == null)
				key = "";

			if (first) {
				first = false;
				repeats.put(key, 0);
			} else {
				writeMapSeparator();

				Integer repeatCount = repeats.get(key);
				if (repeatCount == null) {
					repeats.put(key, 0);
				} else {
					String newKey = key;
					int newCount = repeatCount;
					do { // avoid generated key clashing with a real key
						newKey = key + ' ' + (++newCount);
						repeatCount = repeats.get(newKey);
					} while (repeatCount != null);

					repeats.put(key, newCount);
					key = newKey;
				}
			}

			writeKey(key, true);
			writeVal(val.getVal(i));
		}

		writeMapCloser();
	}

	/**
	 * Represents a NamedList directly as a JSON Object (essentially a Map) Map
	 * repeating any keys if they are repeated in the NamedList. null is mapped
	 * to "".
	 * @param val the NamedList instance
	 * @throws IOException
	 */	
	protected void writeNamedListAsMapWithDups(NamedList<Object> val)
			throws IOException {
		int sz = val.size();
		writeMapOpener();
		for (int i = 0; i < sz; i++) {
			if (i != 0) {
				writeMapSeparator();
			}
			String key = val.getName(i);
			if (key == null)
				key = "";
			writeKey(key, true);
			writeVal(val.getVal(i));
		}
		writeMapCloser();
	}

	/**
	 * Represents a NamedList directly as an array of JSON objects.
	 * NamedList("a"=1,"b"=2,null=3) =&gt; [{"a":1},{"b":2},3]
	 * @param val the NamedList instance
	 * @throws IOException
	 */		
	protected void writeNamedListAsArrMap(NamedList<Object> val)
			throws IOException {
		int sz = val.size();
		writeArrayOpener();
		boolean first = true;
		for (int i = 0; i < sz; i++) {
			String key = val.getName(i);

			if (first) {
				first = false;
			} else {
				writeArraySeparator();
			}
			if (key == null) {
				writeVal(val.getVal(i));
			} else {
				writeMapOpener();
				writeKey(key, true);
				writeVal(val.getVal(i));
				writeMapCloser();
			}
		}
		writeArrayCloser();
	}

	/**
	 * Represents a NamedList directly as an array of arrays
	 * NamedList("a"=1,"b"=2,null=3) =&gt; [["a",1],["b",2],[null,3]]
	 * @param val the NamedList instance
	 * @throws IOException
	 */	
	protected void writeNamedListAsArrArr(NamedList<Object> val)
			throws IOException {
		int sz = val.size();
		writeArrayOpener();

		boolean first = true;
		for (int i = 0; i < sz; i++) {
			String key = val.getName(i);

			if (first) {
				first = false;
			} else {
				writeArraySeparator();
			}

			/***
			 * if key is null, just write value??? if (key==null) {
			 * writeVal(null,val.getVal(i)); } else {
			 ***/

			writeArrayOpener();
			if (key == null) {
				writeNull();
			} else {
				writeStr(key, true);
			}
			writeArraySeparator();
			writeVal(val.getVal(i));
			writeArrayCloser();
		}
		writeArrayCloser();
	}

	/**
	 * Represents a NamedList directly as an array with keys/values
	 * interleaved.
	 * NamedList("a"=1,"b"=2,null=3) =&gt; ["a",1,"b",2,null,3]
	 * @param val the NamedList instance
	 * @throws IOException
	 */	
	protected void writeNamedListAsFlat(NamedList<Object> val)
			throws IOException {
		int sz = val.size();
		writeArrayOpener();
		for (int i = 0; i < sz; i++) {
			if (i != 0) {
				writeArraySeparator();
			}
			String key = val.getName(i);
			if (key == null) {
				writeNull();
			} else {
				writeStr(key, true);
			}
			writeArraySeparator();
			writeVal(val.getVal(i));
		}
		writeArrayCloser();
	}

	
	
	
	/**************************************************
	 **  GENERIC FUNCTIONS TO WRITE KEYS AND VALUES  **
	 **************************************************/

	/**
	 * Write a key name
	 * @param fname Attribute's name.
	 * @param needsEscaping Indicate if escaping is needed. If not writing hard-coded string, set as true.
	 * @throws IOException
	 */
	protected void writeKey(String fname, boolean needsEscaping)
			throws IOException {
		writeStr(fname, needsEscaping);
		_writer.write(MAP_KEY_ATTRIBUTION);
	}

	/**
	 * Write a generic value (detects the value's type and call the specific function to proper write it).
	 * @param val
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final void writeVal(Object val) throws IOException {

		// if there get to be enough types, perhaps hashing on the type
		// to get a handler might be faster (but types must be exact to do
		// that...)

		// go in order of most common to least common
		if (val == null) {
			writeNull();
		} else if (val instanceof String) {
			writeStr(val.toString(), true);
			// micro-optimization... using toString() avoids a cast first
		} else if (val instanceof IndexableField) {
			IndexableField f = (IndexableField) val;
			writeStr(f.stringValue(), true);
		} else if (val instanceof Number) {
			if (val instanceof Integer) {
				writeInt(val.toString());
			} else if (val instanceof Long) {
				writeLong(val.toString());
			} else if (val instanceof Float) {
				// we pass the float instead of using toString() because
				// it may need special formatting. same for double.
				writeFloat(((Float) val).floatValue());
			} else if (val instanceof Double) {
				writeDouble(((Double) val).doubleValue());
			} else if (val instanceof Short) {
				writeInt(val.toString());
			} else if (val instanceof Byte) {
				writeInt(val.toString());
			} else {
				// default... for debugging only
				writeStr(val.getClass().getName() + ':' + val.toString(),
						true);
			}
		} else if (val instanceof Boolean) {
			writeBool(val.toString());
		} else if (val instanceof Date) {
			writeDate((Date) val);
			
			
			// TODO read Document type
		    // } else if (val instanceof Document) {
			// SolrDocument doc = toSolrDocument( (Document)val );
			// DocTransformer transformer = returnFields.getTransformer();
			// if( transformer != null ) {
			// TransformContext context = new TransformContext();
			// context.req = req;
			// transformer.setContext(context);
			// transformer.transform(doc, -1);
			// }
			// writeSolrDocument(name, doc, returnFields, 0 );
			
			
		} else if (val instanceof SolrDocument) {
			 writeSolrDocument((SolrDocument)val, _returnFields, 0);
			 
			// TODO read ResultContext type
			// } else if (val instanceof ResultContext) {
			// // requires access to IndexReader
			// writeDocuments(name, (ResultContext)val, returnFields);
			 
			// TODO read DocList type
			// } else if (val instanceof DocList) {
			// // Should not happen normally
			// ResultContext ctx = new ResultContext();
			// ctx.docs = (DocList)val;
			// writeDocuments(name, ctx, returnFields);
			
			// TODO read DocSet type
			// * this was already commented on Solr original classes.
			// } else if (val instanceof DocSet) {
			// how do we know what fields to read?
			// todo: have a DocList/DocSet wrapper that
			// restricts the fields to write...?
			
		} else if (val instanceof SolrDocumentList) {
			writeSolrDocumentList((SolrDocumentList)val);
		} else if (val instanceof Map) {
			writeMap((Map) val, false, true);
		} else if (val instanceof NamedList) {
			writeNamedList((NamedList) val);
		} else if (val instanceof Iterable) {
			writeArray(((Iterable) val).iterator());
		} else if (val instanceof Object[]) {
			writeArray((Object[]) val);
		} else if (val instanceof Iterator) {
			writeArray((Iterator) val);
		} else if (val instanceof byte[]) {
			byte[] arr = (byte[]) val;
			writeByteArr(arr, 0, arr.length);
		} else if (val instanceof BytesRef) {
			BytesRef arr = (BytesRef) val;
			writeByteArr(arr.bytes, arr.offset, arr.length);
		} else {
			// default... for debugging only
			writeStr(val.getClass().getName() + ':' + val.toString(),
					true);
		}
	}
	
	 
	

	

	/******************************************************************
	 **  COMPLEX TYPES - they need some special logic to be written  **
	 ******************************************************************/

	/**
	 * Write a String value, that can be escaped
	 * @param val The string to write
	 * @param needsEscaping indicates if must escape special chars or just write the string as it is
	 * @throws IOException
	 */
	public void writeStr(String val, boolean needsEscaping)
			throws IOException {
		// it might be more efficient to use a stringbuilder or write substrings
		// if writing chars to the stream is slow.
		if (needsEscaping) {

			/*
			 * http://www.ietf.org/internet-drafts/draft-crockford-jsonorg-json-04.
			 * txt All Unicode characters may be placed within the quotation
			 * marks except for the characters which must be escaped: quotation
			 * mark, reverse solidus, and the control characters (U+0000 through
			 * U+001F).
			 */
			_writer.write('"');

			for (int i = 0; i < val.length(); i++) {
				char ch = val.charAt(i);
				if ((ch > '#' && ch != '\\' && ch < '\u2028') || ch == ' ') { // fast
																				// path
					_writer.write(ch);
					continue;
				}
				switch (ch) {
				case '"':
				case '\\':
					_writer.write('\\');
					_writer.write(ch);
					break;
				case '\r':
					_writer.write('\\');
					_writer.write('r');
					break;
				case '\n':
					_writer.write('\\');
					_writer.write('n');
					break;
				case '\t':
					_writer.write('\\');
					_writer.write('t');
					break;
				case '\b':
					_writer.write('\\');
					_writer.write('b');
					break;
				case '\f':
					_writer.write('\\');
					_writer.write('f');
					break;
				case '\u2028': // fall through
				case '\u2029':
					unicodeEscape(_writer, ch);
					break;
				// case '/':
				default: {
					if (ch <= 0x1F) {
						unicodeEscape(_writer, ch);
					} else {
						_writer.write(ch);
					}
				}
				}
			}

			_writer.write('"');
		} else {
			_writer.write('"');
			_writer.write(val);
			_writer.write('"');
		}
	}

	/**
	 * Writes an array of bytes based 64
	 * @param buf
	 * @param offset
	 * @param len
	 * @throws IOException
	 */
	public void writeByteArr(byte[] buf, int offset, int len)
			throws IOException {
		writeStr(Base64.byteArrayToBase64(buf, offset, len), false);
	}

	/**
	 * Write a map
	 * @param val
	 * @param excludeOuter
	 * @param isFirstVal
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void writeMap(Map val, boolean excludeOuter,
			boolean isFirstVal) throws IOException {
		if (!excludeOuter) {
			writeMapOpener();

			isFirstVal = true;
		}

		boolean doIndent = excludeOuter || val.size() > 1;

		for (Map.Entry entry : (Set<Map.Entry>) val.entrySet()) {
			Object e = entry.getKey();
			String k = e == null ? "" : e.toString();
			Object v = entry.getValue();

			if (isFirstVal) {
				isFirstVal = false;
			} else {
				writeMapSeparator();
			}

			if (doIndent)
				writeKey(k, true);
			writeVal(v);
		}

		if (!excludeOuter) {

			writeMapCloser();
		}
	}

	/**
	 * Writes an array
	 * @param val A instance of Iterator
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public void writeArray(Iterator val) throws IOException {
		writeArrayOpener(); // no trivial way to determine array size

		boolean first = true;
		while (val.hasNext()) {
			if (!first) {
				writeArraySeparator();
			}
			writeVal(val.next());
			first = false;
		}

		writeArrayCloser();
	}

	/**
	 * Writes an array
	 * @param val Array of Object
	 * @throws IOException
	 */
	public void writeArray(Object[] val) throws IOException {
		writeArray(Arrays.asList(val).iterator());
	}
	
	/**
	 * Writes a SolrDocumentList
	 * @param docs SolrDocumentList instance
	 * @throws IOException
	 */
	public final void writeSolrDocumentList(SolrDocumentList docs)
			throws IOException {
		writeStartDocumentList(docs.getStart(), docs.size(),
				docs.getNumFound(), docs.getMaxScore());
		for (int i = 0; i < docs.size(); i++) {
			writeSolrDocument(docs.get(i), _returnFields, i);
		}
		writeEndDocumentList();
	}

	/**
	 * Writes the first fields for a SolrDocumentList block in the map
	 * @param start
	 * @param size
	 * @param numFound
	 * @param maxScore
	 * @throws IOException
	 */
	public void writeStartDocumentList(long start, int size,
			long numFound, Float maxScore) throws IOException {
		writeMapOpener();
		writeKey("numFound", false);
		writeLong(numFound);
		writeMapSeparator();
		writeKey("start", false);
		writeLong(start);

		if (maxScore != null) {
			writeMapSeparator();
			writeKey("maxScore", false);
			writeFloat(maxScore);
		}
		writeMapSeparator();
		writeKey("docs", false);
		writeArrayOpener();
	}

	/**
	 * Close the a SolrDocumentList in the map
	 * @throws IOException
	 */
	public void writeEndDocumentList() throws IOException {
		writeArrayCloser();
		writeMapCloser();
	}

	@SuppressWarnings("rawtypes")
	public void writeSolrDocument(SolrDocument doc,
			ReturnFields returnFields, int idx) throws IOException {
		if (idx > 0) {
			writeArraySeparator();
		}

		writeMapOpener();

		boolean first = true;
		for (String fname : doc.getFieldNames()) {
			if (returnFields != null && !returnFields.wantsField(fname)) {
				continue;
			}

			if (first) {
				first = false;
			} else {
				writeMapSeparator();
			}

			writeKey(fname, true);
			Object val = doc.getFieldValue(fname);

			// SolrDocument will now have multiValued fields represented as a
			// Collection,
			// even if only a single value is returned for this document.
			if (val instanceof List) {
				// shortcut this common case instead of going through writeVal
				// again
				writeArray(((Iterable) val).iterator());
			} else {
				writeVal(val);
			}
		}

		if (doc.hasChildDocuments()) {
			if (first == false) {
				writeMapSeparator();
			}
			writeKey("_childDocuments_", true);
			writeArrayOpener();
			List<SolrDocument> childDocs = doc.getChildDocuments();
			ReturnFields rf = new SolrReturnFields();
			for (int i = 0; i < childDocs.size(); i++) {
				writeSolrDocument(childDocs.get(i), rf, i);
			}
			writeArrayCloser();
		}
		writeMapCloser();
	}
	
	
	
	/**********************************************
	 **  BASIC TYPES - Just write and that's it  **
	 **********************************************/

	public void writeNull() throws IOException {
		_writer.write("null");
	}

	public void writeInt(String val) throws IOException {
		_writer.write(val);
	}

	public void writeLong(String val) throws IOException {
		_writer.write(val);
	}

	public void writeLong(long val) throws IOException {
		writeLong(Long.toString(val));
	}

	public void writeBool(String val) throws IOException {
		_writer.write(val);
	}

	public void writeFloat(String val) throws IOException {
		_writer.write(val);
	}

	public void writeFloat(float val) throws IOException {
		String s = Float.toString(val);
		// If it's not a normal number, write the value as a string instead.
		// The following test also handles NaN since comparisons are always
		// false.
		if (val > Float.NEGATIVE_INFINITY && val < Float.POSITIVE_INFINITY) {
			writeFloat(s);
		} else {
			writeStr(s, false);
		}
	}

	public void writeDouble(String val) throws IOException {
		_writer.write(val);
	}

	public void writeDouble(double val) throws IOException {
		String s = Double.toString(val);
		// If it's not a normal number, write the value as a string instead.
		// The following test also handles NaN since comparisons are always
		// false.
		if (val > Double.NEGATIVE_INFINITY && val < Double.POSITIVE_INFINITY) {
			writeDouble(s);
		} else {
			writeStr(s, false);
		}
	}

	public void writeDate(String val) throws IOException {
		writeStr(val, false);
	}

	public void writeDate(Date val) throws IOException {
		writeDate(DateField.formatExternal(val));
	}
	
	
	
	
	/************************
	 **  STRUCTURE TOKENS  **
	 ************************/

	public void writeMapOpener() throws IOException {
		_writer.write(MAP_OPENER);
	}

	public void writeMapSeparator() throws IOException {
		_writer.write(MAP_SEPARATOR);
	}

	public void writeMapCloser() throws IOException {
		_writer.write(MAP_CLOSER);
	}

	public void writeArrayOpener() throws IOException {
		_writer.write(ARRAY_OPENER);
	}

	public void writeArraySeparator() throws IOException {
		_writer.write(ARRAY_SEPARATOR);
	}

	public void writeArrayCloser() throws IOException {
		_writer.write(ARRAY_CLOSER);
	}

	
	
	
	/*********************************
	 **  UNICODE ESCAPING HANDLING  **
	 *********************************/

	private static char[] hexdigits = { '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	protected static void unicodeEscape(Appendable out, int ch)
			throws IOException {
		out.append('\\');
		out.append('u');
		out.append(hexdigits[(ch >>> 12)]);
		out.append(hexdigits[(ch >>> 8) & 0xf]);
		out.append(hexdigits[(ch >>> 4) & 0xf]);
		out.append(hexdigits[(ch) & 0xf]);
	}

}
