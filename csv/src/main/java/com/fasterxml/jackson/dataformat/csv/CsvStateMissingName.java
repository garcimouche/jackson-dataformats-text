package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;

/**
 * State in which we should expose name token for a "missing column"
 * (for which placeholder `null` value is to be added as well);
 * see {@link Feature#INSERT_NULLS_FOR_MISSING_COLUMNS} for details.
 *
 * @since 2.9
 */
public class CsvStateMissingName extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        if (++csvParser._columnIndex < csvParser._columnCount) {
            csvParser._state = new CsvStateMissingValue();
            csvParser._currentName = csvParser._schema.columnName(csvParser._columnIndex);
            // _currentValue already set to null earlier
            return JsonToken.FIELD_NAME;
        }
        return csvParser._handleObjectRowEnd();
    }
}
