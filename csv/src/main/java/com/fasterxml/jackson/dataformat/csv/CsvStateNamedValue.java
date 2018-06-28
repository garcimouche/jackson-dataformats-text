package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State in which value matching field name will
 * be returned.
 */
public class CsvStateNamedValue extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        // 06-Oct-2015, tatu: During recovery, may get past all regular columns,
        //    but we also need to allow access past... sort of.
        if (csvParser._columnIndex < csvParser._columnCount) {
            CsvSchema.Column column = csvParser._schema.column(csvParser._columnIndex);
            ++csvParser._columnIndex;
            if (column.isArray()) {
                csvParser._startArray(column);
                return JsonToken.START_ARRAY;
            }
        }
        csvParser._state = new CsvStateNextEntry();
        if (csvParser._nullValue != null) {
            if (csvParser._nullValue.equals(csvParser._currentValue)) {
                return JsonToken.VALUE_NULL;
            }
        }
        return JsonToken.VALUE_STRING;
    }
}
