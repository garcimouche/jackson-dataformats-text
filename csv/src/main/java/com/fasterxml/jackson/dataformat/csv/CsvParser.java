package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.FormatFeature;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.base.ParserMinimalBase;
import com.fasterxml.jackson.core.json.DupDetector;
import com.fasterxml.jackson.core.json.JsonReadContext;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.dataformat.csv.impl.CsvDecoder;
import com.fasterxml.jackson.dataformat.csv.impl.CsvIOContext;
import com.fasterxml.jackson.dataformat.csv.impl.TextBuffer;

/**
 * {@link JsonParser} implementation used to expose CSV documents
 * in form that allows other Jackson functionality to deal
 * with it.
 *<p>
 * Implementation is based on a state-machine that pulls information
 * using {@link CsvDecoder}.
 */
public class CsvParser
    extends ParserMinimalBase
{
    /**
     * Enumeration that defines all togglable features for CSV parsers
     */
    public enum Feature
        implements FormatFeature // since 2.7
    {
        /**
         * Feature determines whether spaces around separator characters
         * (commas) are to be automatically trimmed before being reported
         * or not.
         * Note that this does NOT force trimming of possible white space from
         * within double-quoted values, but only those surrounding unquoted
         * values (white space outside of double-quotes is never included regardless
         * of trimming).
         *<p>
         * Default value is false, as per <a href="http://tools.ietf.org/html/rfc4180">RFC-4180</a>.
         */
        TRIM_SPACES(false),

        /**
         * Feature that determines how stream of records (usually CSV lines, but sometimes
         * multiple lines when linefeeds are included in quoted values) is exposed:
         * either as a sequence of Objects (false), or as an Array of Objects (true).
         * Using stream of Objects is convenient when using
         * <code>ObjectMapper.readValues(...)</code>
         * and array of Objects convenient when binding to <code>List</code>s or
         * arrays of values.
         *<p>
         * Default value is false, meaning that by default a CSV document is exposed as
         * a sequence of root-level Object entries.
         */
        WRAP_AS_ARRAY(false),

        /**
         * Feature that allows ignoring of unmappable "extra" columns; that is, values for
         * columns that appear after columns for which types are defined. When disabled,
         * an exception is thrown for such column values, but if enabled, they are
         * silently ignored.
         *<p>
         * Feature is disabled by default.
         *
         * @since 2.7
         */
        IGNORE_TRAILING_UNMAPPABLE(false),

        /**
         * Feature that allows skipping input lines that are completely empty, instead
         * of being decoded as lines of just a single column with empty String value (or,
         * depending on binding, `null`).
         *<p>
         * Feature is disabled by default.
         *
         * @since 2.9
         */
        SKIP_EMPTY_LINES(false),

        /**
         * Feature that allows there to be a trailing single extraneous data
         * column that is empty. When this feature is disabled, any extraneous
         * column, regardless of content will cause an exception to be thrown.
         * Disabling this feature is only useful when
         * IGNORE_TRAILING_UNMAPPABLE is also disabled.
         */
        ALLOW_TRAILING_COMMA(true),

        /**
         * Feature that allows failing (with a {@link CsvMappingException}) in cases
         * where number of column values encountered is less than number of columns
         * declared in active schema ("missing columns").
         *<p>
         * Note that this feature has precedence over {@link #INSERT_NULLS_FOR_MISSING_COLUMNS}
         *<p>
         * Feature is disabled by default.
         *
         * @since 2.9
         */
        FAIL_ON_MISSING_COLUMNS(false),
        
        /**
         * Feature that allows "inserting" virtual key / `null` value pairs in case
         * a row contains fewer columns than declared by configured schema.
         * This typically has the effect of forcing an explicit `null` assigment (or
         * corresponding "null value", if so configured) at databinding level.
         * If disabled, no extra work is done and values for "missing" columns are
         * not exposed as part of the token stream.
         *<p>
         * Note that this feature is only considered if
         * {@link #INSERT_NULLS_FOR_MISSING_COLUMNS}
         * is disabled.
         *<p>
         * Feature is disabled by default.
         *
         * @since 2.9
         */
        INSERT_NULLS_FOR_MISSING_COLUMNS(false),
        ;

        final boolean _defaultState;
        final int _mask;
        
        /**
         * Method that calculates bit set (flags) of all features that
         * are enabled by default.
         */
        public static int collectDefaults()
        {
            int flags = 0;
            for (Feature f : values()) {
                if (f.enabledByDefault()) {
                    flags |= f.getMask();
                }
            }
            return flags;
        }
        
        private Feature(boolean defaultState) {
            _defaultState = defaultState;
            _mask = (1 << ordinal());
        }

        @Override
        public boolean enabledByDefault() { return _defaultState; }
        @Override
        public boolean enabledIn(int flags) { return (flags & _mask) != 0; }
        @Override
        public int getMask() { return _mask; }
    }

    private final static CsvSchema EMPTY_SCHEMA;
    static {
        EMPTY_SCHEMA = CsvSchema.emptySchema();
    }

    /*
    /**********************************************************************
    /* Configuration
    /**********************************************************************
     */
    
    /**
     * Codec used for data binding when (if) requested.
     */
    protected ObjectCodec _objectCodec;

    protected int _formatFeatures;

    /**
     * Definition of columns being read. Initialized to "empty" instance, which
     * has default configuration settings.
     */
    protected CsvSchema _schema = EMPTY_SCHEMA;

    /**
     * Number of columns defined by schema.
     */
    protected int _columnCount = 0;

    /*
    /**********************************************************************
    /* State
    /**********************************************************************
     */

    /**
     * Information about parser context, context in which
     * the next token is to be parsed (root, array, object).
     */
    protected JsonReadContext _parsingContext;

    /**
     * Name of column that we exposed most recently, accessible after
     * {@link JsonToken#FIELD_NAME} as well as value tokens immediately
     * following field name.
     */
    protected String _currentName;

    /**
     * String value for the current column, if accessed.
     */
    protected String _currentValue;

    /**
     * Index of the column we are exposing
     */
    protected int _columnIndex;

    /**
     * Current logical state of the parser; one of <code>STATE_</code>
     * constants.
     */
    protected CsvParserState _state = new CsvStateDocStart();

    /**
     * We will hold on to decoded binary data, for duration of
     * current event, so that multiple calls to
     * {@link #getBinaryValue} will not need to decode data more
     * than once.
     */
    protected byte[] _binaryValue;

    /**
     * Pointer to the first character of the next array value to return.
     */
    protected int _arrayValueStart;

    /**
     * Contents of the cell, to be split into distinct array values.
     */
    protected String _arrayValue;

    protected String _arraySeparator;

    protected String _nullValue;
    
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */

    /**
     * Thing that actually reads the CSV content
     */
    protected final CsvDecoder _reader;

    /**
     * Buffer that contains contents of all values after processing
     * of doubled-quotes, escaped characters.
     */
    protected final TextBuffer _textBuffer;

    protected ByteArrayBuilder _byteArrayBuilder;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public CsvParser(CsvIOContext ctxt, int stdFeatures, int csvFeatures,
            ObjectCodec codec, Reader reader)
    {
        super(stdFeatures);    
        _objectCodec = codec;
        _textBuffer =  ctxt.csvTextBuffer();
        DupDetector dups = JsonParser.Feature.STRICT_DUPLICATE_DETECTION.enabledIn(stdFeatures)
                ? DupDetector.rootDetector(this) : null;
        _formatFeatures = csvFeatures;
        _parsingContext = JsonReadContext.createRootContext(dups);
        _reader = new CsvDecoder(this, ctxt, reader, _schema, _textBuffer,
                stdFeatures, csvFeatures);
    }

    /*
    /**********************************************************
    /* Versioned                                                                             
    /**********************************************************                 
     */

    @Override
    public Version version() {
        return PackageVersion.VERSION;
    }

    /*
    /**********************************************************                              
    /* Overridden methods
    /**********************************************************                              
     */

    @Override
    public ObjectCodec getCodec() {
        return _objectCodec;
    }

    @Override
    public void setCodec(ObjectCodec c) {
        _objectCodec = c;
    }

    @Override
    public boolean canUseSchema(FormatSchema schema) {
        return (schema instanceof CsvSchema);
    }

    @Override
    public void setSchema(FormatSchema schema)
    {
        if (schema instanceof CsvSchema) {
            _schema = (CsvSchema) schema;
            String str = _schema.getNullValueString();
            _nullValue = str;
        } else if (schema == null) {
            schema = EMPTY_SCHEMA;
        } else {
            super.setSchema(schema);
        }
        _columnCount = _schema.size();            
        _reader.setSchema(_schema);
    }

    @Override
    public int releaseBuffered(Writer out) throws IOException {
        return _reader.releaseBuffered(out);
    }

    @Override
    public boolean isClosed() { return _reader.isClosed(); }

    @Override
    public void close() throws IOException { _reader.close(); }

    /*                                                                                       
    /**********************************************************                              
    /* FormatFeature support                                                                             
    /**********************************************************                              
     */

    @Override
    public int getFormatFeatures() {
        return _formatFeatures;
    }

    @Override
    public JsonParser overrideFormatFeatures(int values, int mask) {
        int oldF = _formatFeatures;
        int newF = (_formatFeatures & ~mask) | (values & mask);

        if (oldF != newF) {
            _formatFeatures = newF;
            _reader.overrideFormatFeatures(newF);
        }
        return this;
    }

    /*
    /***************************************************
    /* Public API, configuration
    /***************************************************
     */

    /**
     * Method for enabling specified CSV feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser enable(Feature f)
    {
        _formatFeatures |= f.getMask();
        return this;
    }

    /**
     * Method for disabling specified  CSV feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser disable(Feature f)
    {
        _formatFeatures &= ~f.getMask();
        return this;
    }

    /**
     * Method for enabling or disabling specified CSV feature
     * (check {@link Feature} for list of features)
     */
    public JsonParser configure(Feature f, boolean state)
    {
        if (state) {
            enable(f);
        } else {
            disable(f);
        }
        return this;
    }

    /**
     * Method for checking whether specified CSV {@link Feature}
     * is enabled.
     */
    public boolean isEnabled(Feature f) {
        return (_formatFeatures & f.getMask()) != 0;
    }

    /**
     * Accessor for getting active schema definition: it may be
     * "empty" (no column definitions), but will never be null
     * since it defaults to an empty schema (and default configuration)
     */
    @Override
    public CsvSchema getSchema() {
        return _schema;
    }
    
    /*
    /**********************************************************
    /* Location info
    /**********************************************************
     */
    
    @Override
    public JsonStreamContext getParsingContext() {
        return _parsingContext;
    }

    @Override
    public JsonLocation getTokenLocation() {
        return _reader.getTokenLocation();
    }

    @Override
    public JsonLocation getCurrentLocation() {
        return _reader.getCurrentLocation();
    }

    @Override
    public Object getInputSource() {
        return _reader.getInputSource();
    }
    
    /*
    /**********************************************************
    /* Parsing, basic
    /**********************************************************
     */

    /**
     * We need to override this method to support coercion from basic
     * String value into array, in cases where schema does not
     * specify actual type.
     */
    @Override
    public boolean isExpectedStartArrayToken() {
        if (_currToken == null) {
            return false;
        }
        switch (_currToken.id()) {
        case JsonTokenId.ID_FIELD_NAME:
        case JsonTokenId.ID_START_OBJECT:
        case JsonTokenId.ID_END_OBJECT:
        case JsonTokenId.ID_END_ARRAY:
            return false;
        case JsonTokenId.ID_START_ARRAY:
            return true;
        }
        // Otherwise: may coerce into array, iff we have essentially "untyped" column
        if (_columnIndex < _columnCount) {
            CsvSchema.Column column = _schema.column(_columnIndex);
            if (column.getType() == CsvSchema.ColumnType.STRING) {
                _startArray(column);
                return true;
            }
        }
        // 30-Dec-2014, tatu: Seems like it should be possible to allow this
        //   in non-array-wrapped case too (for 2.5), so let's try that:
        else if (_currToken == JsonToken.VALUE_STRING) {
            _startArray(CsvSchema.Column.PLACEHOLDER);
            return true;
        }
        return false;
    }

    @Override
    public String getCurrentName() throws IOException {
        return _currentName;
    }

    @Override
    public void overrideCurrentName(String name) {
        _currentName = name;
    }

    @Override
    public JsonToken nextToken() throws IOException
    {
        _binaryValue = null;
        
        return _currToken = _state.handle(this);
    }

    /*
    /**********************************************************
    /* Parsing, optimized methods
    /**********************************************************
     */

    @Override
    public boolean nextFieldName(SerializableString str) throws IOException {
        // Optimize for expected case of getting FIELD_NAME:
        if (_state instanceof CsvStateNextEntry) {
            _binaryValue = null;
            JsonToken t = _state.handle(this);
            _currToken = t;
            if (t == JsonToken.FIELD_NAME) {
                return str.getValue().equals(_currentName);
            }
            return false;
        }
        // unlikely, but verify just in case
        return (nextToken() == JsonToken.FIELD_NAME) && str.getValue().equals(getCurrentName());
    }

    @Override
    public String nextFieldName() throws IOException
    {
        // Optimize for expected case of getting FIELD_NAME:
        if (_state instanceof CsvStateNextEntry) {
            _binaryValue = null;
            JsonToken t = _state.handle(this);
            _currToken = t;
            if (t == JsonToken.FIELD_NAME) {
                return _currentName;
            }
            return null;
        }
        // unlikely, but verify just in case
        return (nextToken() == JsonToken.FIELD_NAME) ? getCurrentName() : null;
    }

    @Override
    public String nextTextValue() throws IOException
    {
        _binaryValue = null;
        JsonToken t;
        if (_state instanceof CsvStateNamedValue) {
            _currToken = t = _state.handle(this);
            if (t == JsonToken.VALUE_STRING) {
                return _currentValue;
            }
        } else if (_state instanceof CsvStateUnnamedValue) {
            _currToken = t = _state.handle(this);
            if (t == JsonToken.VALUE_STRING) {
                return _currentValue;
            }
        } else {
            t = nextToken();
            if (t == JsonToken.VALUE_STRING) {
                return getText();
            }
        }
        return null;
    }

    /*
    /**********************************************************
    /* Parsing, helper methods, regular
    /**********************************************************
     */

    /**
     * Method called to process the expected header line
     */
    protected void _readHeaderLine() throws IOException {
        /*
            When the header line is present and the settings ask for it
            to be processed, two different options are possible:

            a) The schema has been populated.  In this case, build a new
               schema where the order matches the *actual* order in which
               the given CSV file offers its columns, iif _schema.reordersColumns()
               is set to true; there cases the consumer of the csv file
               knows about the columns but not necessarily the order in
               which they are defined.

            b) The schema has not been populated.  In this case, build a
               default schema based on the columns found in the header.
         */

        if (_schema.size() > 0 && !_schema.reordersColumns()) {
            if (_schema.strictHeaders()) {
                String name;
                for (CsvSchema.Column column : _schema._columns) {
                    name = _reader.nextString();
                    if (name == null) {
                        _reportError(String.format("Missing header %s", column.getName()));
                    } else if (!column.getName().equals(name)) {
                        _reportError(String.format("Expected header %s, actual header %s", column.getName(), name));
                    }
                }
                if ((name = _reader.nextString()) != null) {
                    _reportError(String.format("Extra header %s", name));
                }
            }
            else {
                //noinspection StatementWithEmptyBody
                while (_reader.nextString() != null) { /* does nothing */ }
            }
            return;
        }

        // either the schema is empty or reorder columns flag is set
        String name;
        CsvSchema.Builder builder = _schema.rebuild().clearColumns();

        while ((name = _reader.nextString()) != null) {
            // one more thing: always trim names, regardless of config settings
            name = name.trim();

            // See if "old" schema defined type; if so, use that type...
            CsvSchema.Column prev = _schema.column(name);
            if (prev != null) {
                builder.addColumn(name, prev.getType());
            } else {
                builder.addColumn(name);
            }
        }

        // Ok: did we get any  columns?
        CsvSchema newSchema = builder.build();
        int size = newSchema.size();
        if (size < 2) { // 1 just because we may get 'empty' header name
            String first = (size == 0) ? "" : newSchema.columnName(0).trim();
            if (first.length() == 0) {
                _reportCsvMappingError("Empty header line: can not bind data");
            }
        }
        // otherwise we will use what we got
        setSchema(builder.build());
    }

    /*
    /**********************************************************
    /* Parsing, helper methods, extra column(s)
    /**********************************************************
     */

    /**
     * Helper method called when an extraneous column value is found.
     * What happens then depends on configuration, but there are three
     * main choices: ignore value (and rest of line); expose extra value
     * as "any property" using configured name, or throw an exception.
     *
     * @since 2.7
     */
    protected JsonToken _handleExtraColumn(String value) throws IOException
    {
        // If "any properties" enabled, expose as such
        String anyProp = _schema.getAnyPropertyName();
        if (anyProp != null) {
            _currentName = anyProp;
            _state = new CsvStateNamedValue();
            return JsonToken.FIELD_NAME;
        }
        _currentName = null;
        // With [dataformat-csv#95] we'll simply ignore extra
        if (Feature.IGNORE_TRAILING_UNMAPPABLE.enabledIn(_formatFeatures)) {
            _state = new CsvStateSkipExtraColumns();
            return _state.handle(this);
        }

        // 14-Mar-2012, tatu: As per [dataformat-csv#1], let's allow one specific case
        // of extra: if we get just one all-whitespace entry, that can be just skipped
        _state = new CsvStateSkipExtraColumns();
        if (_columnIndex == _columnCount && Feature.ALLOW_TRAILING_COMMA.enabledIn(_formatFeatures)) {
            value = value.trim();
            if (value.isEmpty()) {
                // if so, need to verify we then get the end-of-record;
                // easiest to do by just calling ourselves again...
                String next = _reader.nextString();
                if (next == null) { // should end of record or input
                    return _handleObjectRowEnd();
                }
            }
        }
        // 21-May-2015, tatu: Need to enter recovery mode, to skip remainder of the line
        return _reportCsvMappingError("Too many entries: expected at most %d (value #%d (%d chars) \"%s\")",
                _columnCount, _columnIndex, value.length(), value);
    }

    /*
    /**********************************************************
    /* Parsing, helper methods, missing column(s)
    /**********************************************************
     */

    /**
     * Helper method called when end of row occurs before finding values for
     * all schema-specified columns.
     *
     * @since 2.9
     */
    protected JsonToken _handleMissingColumns() throws IOException
    {
        if (Feature.FAIL_ON_MISSING_COLUMNS.enabledIn(_formatFeatures)) {
            // First: to allow recovery, set states to expose next line, if any
            _handleObjectRowEnd();
            // and then report actual problem
            return _reportCsvMappingError("Not enough column values: expected %d, found %d",
                    _columnCount, _columnIndex);
        }
        if (Feature.INSERT_NULLS_FOR_MISSING_COLUMNS.enabledIn(_formatFeatures)) {
            _state = new CsvStateMissingValue();
            _currentName = _schema.columnName(_columnIndex);
            _currentValue = null;
            return JsonToken.FIELD_NAME;
        }
        return _handleObjectRowEnd();
    }

    /*
    /**********************************************************
    /* Parsing, helper methods: row end handling, recover
    /**********************************************************
     */

    /**
     * Helper method called to handle details of state update when end of logical
     * record occurs.
     *
     * @since 2.9
     */
    protected final JsonToken _handleObjectRowEnd() throws IOException
    {
        _parsingContext = _parsingContext.getParent();
        if (!_reader.startNewLine()) {
            _state = new CsvStateDocEnd();
        } else {
            _state = new CsvStateRecordStart();
        }
        return JsonToken.END_OBJECT;
    }

    JsonToken getCurrToken(){
        return _currToken;
    }
    
    JsonToken setCurrToken(JsonToken tok){
        _currToken = tok;
        return _currToken;
    }
    
    
    /*
    /**********************************************************
    /* String value handling
    /**********************************************************
     */
    
    
    // For now we do not store char[] representation...
    @Override
    public boolean hasTextCharacters() {
        if (_currToken == JsonToken.FIELD_NAME) {
            return false;
        }
        return _textBuffer.hasTextAsCharacters();
    }

    @Override
    public String getText() throws IOException {
        if (_currToken == JsonToken.FIELD_NAME) {
            return _currentName;
        }
        return _currentValue;
    }

    @Override
    public char[] getTextCharacters() throws IOException {
        if (_currToken == JsonToken.FIELD_NAME) {
            return _currentName.toCharArray();
        }
        return _textBuffer.contentsAsArray();
    }

    @Override
    public int getTextLength() throws IOException {
        if (_currToken == JsonToken.FIELD_NAME) {
            return _currentName.length();
        }
        return _textBuffer.size();
    }

    @Override
    public int getTextOffset() throws IOException {
        return 0;
    }

    @Override // since 2.8
    public int getText(Writer w) throws IOException {
        String value = (_currToken == JsonToken.FIELD_NAME) ?
                _currentName : _currentValue;
        if (value == null) {
            return 0;
        }
        w.write(value);
        return value.length();
    }
    
    /*
    /**********************************************************************
    /* Binary (base64)
    /**********************************************************************
     */

    @Override
    public Object getEmbeddedObject() throws IOException {
        // in theory may access binary data using this method so...
        return _binaryValue;
    }

    @SuppressWarnings("resource")
    @Override
    public byte[] getBinaryValue(Base64Variant variant) throws IOException
    {
        if (_binaryValue == null) {
            if (_currToken != JsonToken.VALUE_STRING) {
                _reportCsvMappingError("Current token (%s) not VALUE_STRING, can not access as binary", _currToken);
            }
            ByteArrayBuilder builder = _getByteArrayBuilder();
            _decodeBase64(_currentValue, builder, variant);
            _binaryValue = builder.toByteArray();
        }
        return _binaryValue;
    }

    /*
    /**********************************************************************
    /* Number accessors
    /**********************************************************************
     */

    @Override
    public NumberType getNumberType() throws IOException {
        return _reader.getNumberType();
    }
    
    @Override
    public Number getNumberValue() throws IOException {
        return _reader.getNumberValue();
    }

    @Override
    public int getIntValue() throws IOException {
        return _reader.getIntValue();
    }
    
    @Override
    public long getLongValue() throws IOException {
        return _reader.getLongValue();
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
        return _reader.getBigIntegerValue();
    }

    @Override
    public float getFloatValue() throws IOException {
        return _reader.getFloatValue();
    }

    @Override
    public double getDoubleValue() throws IOException {
        return _reader.getDoubleValue();
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        return _reader.getDecimalValue();
    }

    /*
    /**********************************************************************
    /* Helper methods from base class
    /**********************************************************************
     */

    @Override
    protected void _handleEOF() throws JsonParseException {
        // I don't think there's problem with EOFs usually; except maybe in quoted stuff?
        _reportInvalidEOF(": expected closing quote character", null);
    }

    /*
    /**********************************************************
    /* Internal methods, error reporting
    /**********************************************************
     */

    /**
     * Method called when there is a problem related to mapping data
     * (compared to a low-level generation); if so, should be surfaced
     * as 
     *
     * @since 2.9
     */
    public <T> T _reportCsvMappingError(String msg, Object... args) throws JsonProcessingException {
        if (args.length > 0) {
            msg = String.format(msg, args);
        }
        throw CsvMappingException.from(this, msg, _schema);
    }

    public void _reportParsingError(String msg)  throws JsonProcessingException {
        super._reportError(msg);
    }

    public void _reportUnexpectedCsvChar(int ch, String msg)  throws JsonProcessingException {
        super._reportUnexpectedChar(ch, msg);
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    public ByteArrayBuilder _getByteArrayBuilder()
    {
        if (_byteArrayBuilder == null) {
            _byteArrayBuilder = new ByteArrayBuilder();
        } else {
            _byteArrayBuilder.reset();
        }
        return _byteArrayBuilder;
    }

    protected void _startArray(CsvSchema.Column column)
    {
        _currToken = JsonToken.START_ARRAY;
        _parsingContext = _parsingContext.createChildArrayContext(_reader.getCurrentRow(),
                _reader.getCurrentColumn());
        _state = new CsvStateInArray();
        _arrayValueStart = 0;
        _arrayValue = _currentValue;
        String sep = column.getArrayElementSeparator();
        if (sep.isEmpty()) {
            sep = _schema.getArrayElementSeparator();
        }
        _arraySeparator = sep;
    }
}
