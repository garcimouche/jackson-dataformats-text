package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State in which next entry will be available, returning
 * either {@link JsonToken#FIELD_NAME} or value
 * (depending on whether entries are expressed as
 * Objects or just Arrays); or
 * matching close marker.
 */
public class CsvStateNextEntry extends CsvParserState {
    
    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        // NOTE: only called when we do have real Schema
        String next;

        try {
            next = csvParser._reader.nextString();
        } catch (IOException e) {
            // 12-Oct-2015, tatu: Need to resync here as well...
            csvParser._state = new CsvStateSkipExtraColumns();
            throw e;
        }

        if (next == null) { // end of record or input...
            // 16-Mar-2017, tatu: [dataformat-csv#137] Missing column(s)?
            if (csvParser._columnIndex < csvParser._columnCount) {
                return csvParser._handleMissingColumns();
            }
            return csvParser._handleObjectRowEnd();
        }
        csvParser._currentValue = next;
        if (csvParser._columnIndex >= csvParser._columnCount) {
            return csvParser._handleExtraColumn(next);
        }
        csvParser._state = new CsvStateNamedValue();
        csvParser._currentName = csvParser._schema.columnName(csvParser._columnIndex);
        return JsonToken.FIELD_NAME;
    }

}
