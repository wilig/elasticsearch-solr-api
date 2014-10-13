package org.codelibs.elasticsearch.solr.index.mapper.date;

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
import java.util.List;
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
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

public class SolrDateFieldMapper extends NumberFieldMapper<Long> {

    private static final String NOW = "NOW";

    private static final char Z = 'Z';

    public static final String CONTENT_TYPE = "solr_date";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda
                .forPattern("dateOptionalTime", Locale.ROOT);

        public static final FieldType FIELD_TYPE = new FieldType(
                NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;

        public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

        public static final boolean ROUND_CEIL = true;
    }

    public static class Builder extends
            NumberFieldMapper.Builder<Builder, SolrDateFieldMapper> {

        protected TimeUnit timeUnit = Defaults.TIME_UNIT;

        protected String nullValue = Defaults.NULL_VALUE;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        private Locale locale;

        public Builder(final String name) {
            super(
                    name,
                    new FieldType(Defaults.FIELD_TYPE),
                    org.elasticsearch.index.mapper.core.NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT);
            builder = this;
            // do *NOT* rely on the default locale
            locale = Locale.ROOT;
        }

        public Builder timeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder nullValue(final String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(
                final FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override
        public SolrDateFieldMapper build(final BuilderContext context) {
            boolean roundCeil = Defaults.ROUND_CEIL;
            if (context.indexSettings() != null) {
                final Settings settings = context.indexSettings();
                roundCeil = settings.getAsBoolean(
                        "index.mapping.date.round_ceil", settings.getAsBoolean(
                                "index.mapping.date.parse_upper_inclusive",
                                Defaults.ROUND_CEIL));
            }
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            if (!locale.equals(dateTimeFormatter.locale())) {
                dateTimeFormatter = new FormatDateTimeFormatter(
                        dateTimeFormatter.format(), dateTimeFormatter.parser(),
                        dateTimeFormatter.printer(), locale);
            }
            final SolrDateFieldMapper fieldMapper = new SolrDateFieldMapper(
                    buildNames(context), dateTimeFormatter,
                    fieldType.numericPrecisionStep(), boost, fieldType,
                    docValues, nullValue, timeUnit, roundCeil,
                    ignoreMalformed(context), coerce(context),
                    postingsProvider, docValuesProvider, similarity,
                    normsLoading, fieldDataSettings, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        public Builder locale(final Locale locale) {
            this.locale = locale;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(final String name,
                final Map<String, Object> node,
                final ParserContext parserContext)
                throws MapperParsingException {
            final SolrDateFieldMapper.Builder builder = new SolrDateFieldMapper.Builder(
                    name);
            parseNumberField(builder, name, node, parserContext);
            for (final Map.Entry<String, Object> entry : node.entrySet()) {
                final String propName = Strings
                        .toUnderscoreCase(entry.getKey());
                final Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propNode));
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString()
                            .toUpperCase(Locale.ROOT)));
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                }
            }
            return builder;
        }
    }

    protected FormatDateTimeFormatter dateTimeFormatter;

    // Triggers rounding up of the upper bound for range queries and filters if
    // set to true.
    // Rounding up a date here has the following meaning: If a date is not
    // defined with full precision, for example, no milliseconds given, the date
    // will be filled up to the next larger date with that precision.
    // Example: An upper bound given as "2000-01-01", will be converted to
    // "2000-01-01T23.59.59.999"
    private final boolean roundCeil;

    private final DateMathParser dateMathParser;

    private String nullValue;

    protected final TimeUnit timeUnit;

    protected SolrDateFieldMapper(final Names names,
            final FormatDateTimeFormatter dateTimeFormatter,
            final int precisionStep, final float boost,
            final FieldType fieldType, final Boolean docValues,
            final String nullValue, final TimeUnit timeUnit,
            final boolean roundCeil, final Explicit<Boolean> ignoreMalformed,
            final Explicit<Boolean> coerce,
            final PostingsFormatProvider postingsProvider,
            final DocValuesFormatProvider docValuesProvider,
            final SimilarityProvider similarity,

            final Loading normsLoading,
            @Nullable final Settings fieldDataSettings,
            final Settings indexSettings, final MultiFields multiFields,
            final CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues,
                ignoreMalformed, coerce, new NamedAnalyzer("_solr_date/"
                        + precisionStep, new NumericDateAnalyzer(precisionStep,
                        dateTimeFormatter.parser())),
                new NamedAnalyzer("_solr_date/max", new NumericDateAnalyzer(
                        Integer.MAX_VALUE, dateTimeFormatter.parser())),
                postingsProvider, docValuesProvider, similarity, normsLoading,
                fieldDataSettings, indexSettings, multiFields, copyTo);
        this.dateTimeFormatter = dateTimeFormatter;
        this.nullValue = nullValue;
        this.timeUnit = timeUnit;
        this.roundCeil = roundCeil;
        dateMathParser = new DateMathParser(dateTimeFormatter, timeUnit);
    }

    public FormatDateTimeFormatter dateTimeFormatter() {
        return dateTimeFormatter;
    }

    public DateMathParser dateMathParser() {
        return dateMathParser;
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
    public Long value(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToLong((BytesRef) value);
        }
        return parseStringValue(value.toString());
    }

    /** Dates should return as a string. */
    @Override
    public Object valueForSearch(final Object value) {
        if (value instanceof String) {
            // assume its the string that was indexed, just return it... (for example, with get)
            return value;
        }
        final Long val = value(value);
        if (val == null) {
            return null;
        }
        return dateTimeFormatter.printer().print(val);
    }

    @Override
    public BytesRef indexedValueForSearch(final Object value) {
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
        return bytesRef.get();
    }

    private long parseValue(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return dateTimeFormatter.parser().parseMillis(
                    ((BytesRef) value).utf8ToString());
        }
        return dateTimeFormatter.parser().parseMillis(value.toString());
    }

    private String convertToString(final Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    @Override
    public Query fuzzyQuery(final String value, final Fuzziness fuzziness,
            final int prefixLength, final int maxExpansions,
            final boolean transpositions) {
        final long iValue = dateMathParser.parse(value,
                System.currentTimeMillis());
        long iSim;
        try {
            iSim = fuzziness.asTimeValue().millis();
        } catch (final Exception e) {
            // not a time format
            iSim = fuzziness.asLong();
        }
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                iValue - iSim, iValue + iSim, true, true);
    }

    @Override
    public Query termQuery(final Object value,
            @Nullable final QueryParseContext context) {
        final long lValue = parseToMilliseconds(value, context);
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lValue, lValue, true, true);
    }

    public long parseToMilliseconds(final Object value,
            @Nullable final QueryParseContext context) {
        return parseToMilliseconds(value, context, false);
    }

    public long parseToMilliseconds(final Object value,
            @Nullable final QueryParseContext context,
            final boolean includeUpper) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        final long now = context == null ? System.currentTimeMillis() : context
                .nowInMillis();
        return includeUpper && roundCeil ? dateMathParser.parseRoundCeil(
                convertToString(value), now) : dateMathParser.parse(
                convertToString(value), now);
    }

    public long parseToMilliseconds(final String value,
            @Nullable final QueryParseContext context,
            final boolean includeUpper) {
        final long now = context == null ? System.currentTimeMillis() : context
                .nowInMillis();
        return includeUpper && roundCeil ? dateMathParser.parseRoundCeil(value,
                now) : dateMathParser.parse(value, now);
    }

    @Override
    public Filter termFilter(final Object value,
            @Nullable final QueryParseContext context) {
        final long lValue = parseToMilliseconds(value, context);
        return NumericRangeFilter.newLongRange(names.indexName(),
                precisionStep, lValue, lValue, true, true);
    }

    @Override
    public Query rangeQuery(final Object lowerTerm, final Object upperTerm,
            final boolean includeLower, final boolean includeUpper,
            @Nullable final QueryParseContext context) {
        return NumericRangeQuery.newLongRange(
                names.indexName(),
                precisionStep,
                lowerTerm == null ? null : parseToMilliseconds(lowerTerm,
                        context),
                upperTerm == null ? null : parseToMilliseconds(upperTerm,
                        context, includeUpper), includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(final Object lowerTerm, final Object upperTerm,
            final boolean includeLower, final boolean includeUpper,
            @Nullable final QueryParseContext context) {
        return rangeFilter(lowerTerm, upperTerm, includeLower, includeUpper,
                context, false);
    }

    public Filter rangeFilter(final Object lowerTerm, final Object upperTerm,
            final boolean includeLower, final boolean includeUpper,
            @Nullable final QueryParseContext context,
            final boolean explicitCaching) {
        boolean cache = explicitCaching;
        Long lowerVal = null;
        Long upperVal = null;
        if (lowerTerm != null) {
            if (lowerTerm instanceof Number) {
                lowerVal = ((Number) lowerTerm).longValue();
            } else {
                final String value = convertToString(lowerTerm);
                cache = explicitCaching
                        || !hasNowExpressionWithNoRounding(value);
                lowerVal = parseToMilliseconds(value, context, false);
            }
        }
        if (upperTerm != null) {
            if (upperTerm instanceof Number) {
                upperVal = ((Number) upperTerm).longValue();
            } else {
                final String value = convertToString(upperTerm);
                cache = explicitCaching
                        || !hasNowExpressionWithNoRounding(value);
                upperVal = parseToMilliseconds(value, context, includeUpper);
            }
        }

        final Filter filter = NumericRangeFilter.newLongRange(
                names.indexName(), precisionStep, lowerVal, upperVal,
                includeLower, includeUpper);
        if (!cache) {
            // We don't cache range filter if `now` date expression is used and also when a compound filter wraps
            // a range filter with a `now` date expressions.
            return NoCacheFilter.wrap(filter);
        } else {
            return filter;
        }
    }

    @Override
    public Filter rangeFilter(final QueryParseContext parseContext,
            final Object lowerTerm, final Object upperTerm,
            final boolean includeLower, final boolean includeUpper,
            @Nullable final QueryParseContext context) {
        return rangeFilter(parseContext, lowerTerm, upperTerm, includeLower,
                includeUpper, context, false);
    }

    public Filter rangeFilter(final QueryParseContext parseContext,
            final Object lowerTerm, final Object upperTerm,
            final boolean includeLower, final boolean includeUpper,
            @Nullable final QueryParseContext context,
            final boolean explicitCaching) {
        boolean cache = explicitCaching;
        Long lowerVal = null;
        Long upperVal = null;
        if (lowerTerm != null) {
            if (lowerTerm instanceof Number) {
                lowerVal = ((Number) lowerTerm).longValue();
            } else {
                final String value = convertToString(lowerTerm);
                cache = explicitCaching
                        || !hasNowExpressionWithNoRounding(value);
                lowerVal = parseToMilliseconds(value, context, false);
            }
        }
        if (upperTerm != null) {
            if (upperTerm instanceof Number) {
                upperVal = ((Number) upperTerm).longValue();
            } else {
                final String value = convertToString(upperTerm);
                cache = explicitCaching
                        || !hasNowExpressionWithNoRounding(value);
                upperVal = parseToMilliseconds(value, context, includeUpper);
            }
        }

        final Filter filter = NumericRangeFieldDataFilter.newLongRange(
                (IndexNumericFieldData) parseContext.getForField(this),
                lowerVal, upperVal, includeLower, includeUpper);
        if (!cache) {
            // We don't cache range filter if `now` date expression is used and also when a compound filter wraps
            // a range filter with a `now` date expressions.
            return NoCacheFilter.wrap(filter);
        } else {
            return filter;
        }
    }

    private boolean hasNowExpressionWithNoRounding(final String value) {
        final int index = value.indexOf("now");
        if (index != -1) {
            if (value.length() == 3) {
                return true;
            } else {
                int indexOfPotentialRounding = index + 3;
                if (indexOfPotentialRounding >= value.length()) {
                    return true;
                } else {
                    char potentialRoundingChar;
                    do {
                        potentialRoundingChar = value
                                .charAt(indexOfPotentialRounding++);
                        if (potentialRoundingChar == '/') {
                            return false; // We found the rounding char, so we shouldn't forcefully disable caching
                        } else if (potentialRoundingChar == ' ') {
                            return true; // Next token in the date math expression and no rounding found, so we should not cache.
                        }
                    } while (indexOfPotentialRounding < value.length());
                    return true; // Couldn't find rounding char, so we should not cache
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        final long value = parseStringValue(nullValue);
        return NumericRangeFilter.newLongRange(names.indexName(),
                precisionStep, value, value, true, true);
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(final ParseContext context,
            final List<Field> fields) throws IOException {
        String dateAsString = null;
        Long value = null;
        float boost = this.boost;
        if (context.externalValueSet()) {
            final Object externalValue = context.externalValue();
            if (externalValue instanceof Number) {
                value = ((Number) externalValue).longValue();
            } else {
                dateAsString = (String) externalValue;
                if (dateAsString == null) {
                    dateAsString = nullValue;
                }
            }
        } else {
            final XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                dateAsString = nullValue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.longValue(coerce.value());
            } else if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName)
                                || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                dateAsString = nullValue;
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                value = parser.longValue(coerce.value());
                            } else {
                                dateAsString = parser.text();
                            }
                        } else if ("boost".equals(currentFieldName)
                                || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticsearchIllegalArgumentException(
                                    "unknown property [" + currentFieldName
                                            + "]");
                        }
                    }
                }
            } else {
                dateAsString = parser.text();
            }
        }

        if (dateAsString != null) {
            assert value == null;
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), dateAsString,
                        boost);
            }
            value = parseStringValue(dateAsString);
        }

        if (value != null) {
            if (fieldType.indexed() || fieldType.stored()) {
                final CustomLongNumericField field = new CustomLongNumericField(
                        this, value, fieldType);
                field.setBoost(boost);
                fields.add(field);
            }
            if (hasDocValues()) {
                addDocValue(context, fields, value);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(final Mapper mergeWith, final MergeContext mergeContext)
            throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            nullValue = ((SolrDateFieldMapper) mergeWith).nullValue;
            dateTimeFormatter = ((SolrDateFieldMapper) mergeWith).dateTimeFormatter;
        }
    }

    @Override
    protected void doXContentBody(final XContentBuilder builder,
            final boolean includeDefaults, final Params params)
            throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults
                || precisionStep != org.elasticsearch.index.mapper.core.NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", precisionStep);
        }
        builder.field("format", dateTimeFormatter.format());
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

        if (includeDefaults || timeUnit != Defaults.TIME_UNIT) {
            builder.field("numeric_resolution",
                    timeUnit.name().toLowerCase(Locale.ROOT));
        }
        // only serialize locale if needed, ROOT is the default, so no need to serialize that case as well...
        if (dateTimeFormatter.locale() != null
                && dateTimeFormatter.locale() != Locale.ROOT) {
            builder.field("locale", dateTimeFormatter.locale());
        } else if (includeDefaults) {
            if (dateTimeFormatter.locale() == null) {
                builder.field("locale", Locale.ROOT);
            } else {
                builder.field("locale", dateTimeFormatter.locale());
            }
        }
    }

    private long parseStringValue(final String value) {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (final Exception ignore) {
            try {
                return parseSolrDateMath(value.toUpperCase(Locale.ROOT),
                        System.currentTimeMillis()).getTime();
            } catch (final Exception e) {
                try {
                    final long time = Long.parseLong(value);
                    return timeUnit.toMillis(time);
                } catch (final NumberFormatException e1) {
                    throw new MapperParsingException(
                            "failed to parse date field [" + value
                                    + "], tried both date format ["
                                    + dateTimeFormatter.format()
                                    + "], and timestamp number with locale ["
                                    + dateTimeFormatter.locale() + "]", e);
                }
            }
        }
    }

    protected Date parseSolrDateMath(final String val, final long now)
            throws ParseException {
        String math = null;
        final org.apache.solr.util.DateMathParser p = new org.apache.solr.util.DateMathParser();

        p.setNow(new Date(now));

        if (val.startsWith(NOW)) {
            math = val.substring(NOW.length());
        } else {
            final int zz = val.indexOf(Z);
            if (0 < zz) {
                math = val.substring(zz + 1);
                final ISO8601CanonicalDateFormat df = new ISO8601CanonicalDateFormat();
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
            setTimeZone(CANONICAL_TZ);
        }

        @Override
        public Date parse(final String i, final ParsePosition p) {
            /* delegate to SimpleDateFormat for easy stuff */
            Date d = super.parse(i, p);
            int milliIndex = p.getIndex();
            /* worry about the milliseconds ourselves */
            if (null != d && -1 == p.getErrorIndex()
                    && milliIndex + 1 < i.length()
                    && '.' == i.charAt(milliIndex)) {
                p.setIndex(++milliIndex); // NOTE: ++ to chomp '.'
                final Number millis = millisParser.parse(i, p);
                if (-1 == p.getErrorIndex()) {
                    final int endIndex = p.getIndex();
                    d = new Date(d.getTime()
                            + (long) (millis.doubleValue() * Math.pow(10, 3
                                    - endIndex + milliIndex)));
                }
            }
            return d;
        }

        @Override
        public StringBuffer format(final Date d, final StringBuffer toAppendTo,
                final FieldPosition pos) {
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
            final int posBegin = toAppendTo.length();
            toAppendTo.append(millisFormat.format(millis / 1000d));
            if (DateFormat.MILLISECOND_FIELD == pos.getField()) {
                pos.setBeginIndex(posBegin);
                pos.setEndIndex(toAppendTo.length());
            }
            return toAppendTo;
        }

        @Override
        public DateFormat clone() {
            final ISO8601CanonicalDateFormat c = (ISO8601CanonicalDateFormat) super
                    .clone();
            c.millisParser = NumberFormat.getIntegerInstance(CANONICAL_LOCALE);
            c.millisFormat = new DecimalFormat(".###",
                    new DecimalFormatSymbols(CANONICAL_LOCALE));
            return c;
        }
    }
}
