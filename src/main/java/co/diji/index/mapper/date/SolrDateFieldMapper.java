package co.diji.index.mapper.date;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

public class SolrDateFieldMapper extends NumberFieldMapper<Long> {

	private static final String NOW = "NOW";

	private static final char Z = 'Z';
	
    public static final String CONTENT_TYPE = "solr_date";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime");

        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;

        public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
        public static final boolean PARSE_UPPER_INCLUSIVE = true;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, SolrDateFieldMapper> {

        protected TimeUnit timeUnit = Defaults.TIME_UNIT;

        protected String nullValue = Defaults.NULL_VALUE;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override
        public SolrDateFieldMapper build(BuilderContext context) {
            boolean parseUpperInclusive = Defaults.PARSE_UPPER_INCLUSIVE;
            if (context.indexSettings() != null) {
                parseUpperInclusive = context.indexSettings().getAsBoolean("index.mapping.date.parse_upper_inclusive", Defaults.PARSE_UPPER_INCLUSIVE);
            }
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            SolrDateFieldMapper fieldMapper = new SolrDateFieldMapper(buildNames(context), dateTimeFormatter,
                    precisionStep, boost, fieldType, nullValue,
                    timeUnit, parseUpperInclusive, ignoreMalformed(context), provider, similarity, fieldDataSettings);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
        	SolrDateFieldMapper.Builder builder =  new SolrDateFieldMapper.Builder(name);//dateField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString().toUpperCase()));
                }
            }
            return builder;
        }
    }

    protected final FormatDateTimeFormatter dateTimeFormatter;

    private final boolean parseUpperInclusive;

    private final DateMathParser dateMathParser;

    private String nullValue;

    protected final TimeUnit timeUnit;

    protected SolrDateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep, float boost, FieldType fieldType,
                              String nullValue, TimeUnit timeUnit, boolean parseUpperInclusive, Explicit<Boolean> ignoreMalformed,
                              PostingsFormatProvider provider, SimilarityProvider similarity, @Nullable Settings fieldDataSettings) {
        super(names, precisionStep, boost, fieldType,
                ignoreMalformed, new NamedAnalyzer("_solr_date/" + precisionStep,
                new NumericDateAnalyzer(precisionStep, dateTimeFormatter.parser())),
                new NamedAnalyzer("_solr_date/max", new NumericDateAnalyzer(Integer.MAX_VALUE, dateTimeFormatter.parser())),
                provider, similarity, fieldDataSettings);
        this.dateTimeFormatter = dateTimeFormatter;
        this.nullValue = nullValue;
        this.timeUnit = timeUnit;
        this.parseUpperInclusive = parseUpperInclusive;
        this.dateMathParser = new DateMathParser(dateTimeFormatter, timeUnit);
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected int maxPrecisionStep() {
        return 64;
    }

    @Override
    public Long value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToLong((BytesRef) value);
        }
        return parseStringValue(value.toString(), System.currentTimeMillis());
    }

    /**
     * Dates should return as a string.
     */
    @Override
    public Object valueForSearch(Object value) {
        if (value instanceof String) {
            // assume its the string that was indexed, just return it... (for example, with get)
            return value;
        }
        Long val = value(value);
        if (val == null) {
            return null;
        }
        return dateTimeFormatter.printer().print(val);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        BytesRef bytesRef = new BytesRef();
        NumericUtils.longToPrefixCoded(parseValue(value), precisionStep(), bytesRef);
        return bytesRef;
    }

    private long parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return dateTimeFormatter.parser().parseMillis(((BytesRef) value).utf8ToString());
        }
        return dateTimeFormatter.parser().parseMillis(value.toString());
    }

    private String convertToString(Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions, boolean transpositions) {
        long iValue = parseStringValue(value, System.currentTimeMillis());
        long iSim;
        try {
            iSim = TimeValue.parseTimeValue(minSim, null).millis();
        } catch (Exception e) {
            // not a time format
            iSim = (long) Double.parseDouble(minSim);
        }
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        long lValue = parseStringValue(convertToString(value), now);
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lValue, lValue, true, true);
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        long lValue = parseStringValue(convertToString(value), now);
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                lValue, lValue, true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseStringValue(convertToString(lowerTerm), now),
                upperTerm == null ? null : (includeUpper && parseUpperInclusive) ? parseStringValue(convertToString(upperTerm), now) : parseStringValue(convertToString(upperTerm), now),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseStringValue(convertToString(lowerTerm), now),
                upperTerm == null ? null : (includeUpper && parseUpperInclusive) ? parseStringValue(convertToString(upperTerm), now) : parseStringValue(convertToString(upperTerm), now),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(IndexFieldDataService fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        long now = context == null ? System.currentTimeMillis() : context.nowInMillis();
        return NumericRangeFieldDataFilter.newLongRange((IndexNumericFieldData) fieldData.getForField(this),
                lowerTerm == null ? null : parseStringValue(convertToString(lowerTerm), now),
                upperTerm == null ? null : (includeUpper && parseUpperInclusive) ? parseStringValue(convertToString(upperTerm), now) : parseStringValue(convertToString(upperTerm), now),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        long value = parseStringValue(nullValue, System.currentTimeMillis());
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                value,
                value,
                true, true);
    }


    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected Field innerParseCreateField(ParseContext context) throws IOException {
        String dateAsString = null;
        Long value = null;
        float boost = this.boost;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue instanceof Number) {
                value = ((Number) externalValue).longValue();
            } else {
                dateAsString = (String) externalValue;
                if (dateAsString == null) {
                    dateAsString = nullValue;
                }
            }
        } else {
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                dateAsString = nullValue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.longValue();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                dateAsString = nullValue;
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                value = parser.longValue();
                            } else {
                                dateAsString = parser.text();
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticSearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                dateAsString = parser.text();
            }
        }

        if (value != null) {
            LongFieldMapper.CustomLongNumericField field = new LongFieldMapper.CustomLongNumericField(this, timeUnit.toMillis(value), fieldType);
            field.setBoost(boost);
            return field;
        }

        if (dateAsString == null) {
            return null;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(names.fullName(), dateAsString, boost);
        }

        value = parseStringValue(dateAsString, System.currentTimeMillis());
        LongFieldMapper.CustomLongNumericField field = new LongFieldMapper.CustomLongNumericField(this, value, fieldType);
        field.setBoost(boost);
        return field;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.nullValue = ((SolrDateFieldMapper) mergeWith).nullValue;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (precisionStep != Defaults.PRECISION_STEP) {
            builder.field("precision_step", precisionStep);
        }
        builder.field("format", dateTimeFormatter.format());
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
        if (timeUnit != Defaults.TIME_UNIT) {
            builder.field("numeric_resolution", timeUnit.name().toLowerCase());
        }
    }

    private long parseStringValue(String value, long now) {
        try {
			return parseSolrDateMath(value.toUpperCase(Locale.ROOT), now).getTime();
            // return dateTimeFormatter.parser().parseMillis(value);
        } catch (ParseException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new MapperParsingException("failed to parse solr_date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number", e);
            }
        }
    }
    
	protected Date parseSolrDateMath(String val, long now) throws ParseException {
		String math = null;
        final org.apache.solr.util.DateMathParser p = new org.apache.solr.util.DateMathParser();

		p.setNow(new Date(now));

		if (val.startsWith(NOW)) {
			math = val.substring(NOW.length());
		} else {
			final int zz = val.indexOf(Z);
			if (0 < zz) {
				math = val.substring(zz + 1);
				ISO8601CanonicalDateFormat df = new ISO8601CanonicalDateFormat();
				p.setNow(df.parse(val.substring(0, zz + 1)));
			} else {
				throw new ParseException("Invalid Date String:'" + val + '\'',
						zz);
			}
		}

		if (null == math || math.equals("")) {
			return p.getNow();
		}

		return p.parseMath(math);

	}

	protected static class ISO8601CanonicalDateFormat extends SimpleDateFormat {

		private static final long serialVersionUID = 1L;
		protected static Locale CANONICAL_LOCALE = Locale.ROOT;
		protected static final TimeZone CANONICAL_TZ = TimeZone
				.getTimeZone("UTC");

		protected NumberFormat millisParser = NumberFormat
				.getIntegerInstance(CANONICAL_LOCALE);

		protected NumberFormat millisFormat = new DecimalFormat(".###",
				new DecimalFormatSymbols(CANONICAL_LOCALE));

		public ISO8601CanonicalDateFormat() {
			super("yyyy-MM-dd'T'HH:mm:ss", CANONICAL_LOCALE);
			this.setTimeZone(CANONICAL_TZ);
		}

		@Override
		public Date parse(String i, ParsePosition p) {
			/* delegate to SimpleDateFormat for easy stuff */
			Date d = super.parse(i, p);
			int milliIndex = p.getIndex();
			/* worry about the milliseconds ourselves */
			if (null != d && -1 == p.getErrorIndex()
					&& milliIndex + 1 < i.length()
					&& '.' == i.charAt(milliIndex)) {
				p.setIndex(++milliIndex); // NOTE: ++ to chomp '.'
				Number millis = millisParser.parse(i, p);
				if (-1 == p.getErrorIndex()) {
					int endIndex = p.getIndex();
					d = new Date(d.getTime()
							+ (long) (millis.doubleValue() * Math.pow(10,
									(3 - endIndex + milliIndex))));
				}
			}
			return d;
		}

		@Override
		public StringBuffer format(Date d, StringBuffer toAppendTo,
				FieldPosition pos) {
			/* delegate to SimpleDateFormat for easy stuff */
			super.format(d, toAppendTo, pos);
			/* worry aboutthe milliseconds ourselves */
			long millis = d.getTime() % 1000l;
			if (0L == millis) {
				return toAppendTo;
			}
			if (millis < 0L) {
				// original date was prior to epoch
				millis += 1000L;
			}
			int posBegin = toAppendTo.length();
			toAppendTo.append(millisFormat.format(millis / 1000d));
			if (DateFormat.MILLISECOND_FIELD == pos.getField()) {
				pos.setBeginIndex(posBegin);
				pos.setEndIndex(toAppendTo.length());
			}
			return toAppendTo;
		}

		@Override
		public DateFormat clone() {
			ISO8601CanonicalDateFormat c = (ISO8601CanonicalDateFormat) super
					.clone();
			c.millisParser = NumberFormat.getIntegerInstance(CANONICAL_LOCALE);
			c.millisFormat = new DecimalFormat(".###",
					new DecimalFormatSymbols(CANONICAL_LOCALE));
			return c;
		}
	}
}
