package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;

/**
 * State in which we should expose `null` value token as a value for
 * "missing" column;
 * see {@link Feature#INSERT_NULLS_FOR_MISSING_COLUMNS} for details.
 *
 * @since 2.9
 */
public class CsvStateMissingValue extends CsvParserState {

    
    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        csvParser._state = new CsvStateMissingName();
        return JsonToken.VALUE_NULL;
    }
}
