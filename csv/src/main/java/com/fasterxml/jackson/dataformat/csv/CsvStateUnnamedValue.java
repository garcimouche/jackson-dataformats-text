package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State in which "unnamed" value (entry in an array)
 * will be returned, if one available; otherwise
 * end-array is returned.
 */
public class CsvStateUnnamedValue extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        String next = csvParser._reader.nextString();
        if (next == null) { // end of record or input...
            csvParser._parsingContext = csvParser._parsingContext.getParent();
            if (!csvParser._reader.startNewLine()) { // end of whole thing...
                csvParser._state = new CsvStateDocEnd();
            } else {
                // no, just end of record
                csvParser._state = new CsvStateRecordStart();
            }
            return JsonToken.END_ARRAY;
        }
        // state remains the same
        csvParser._currentValue = next;
        ++csvParser._columnIndex;
        if (csvParser._nullValue != null) {
            if (csvParser._nullValue.equals(next)) {
                return JsonToken.VALUE_NULL;
            }
        }
        return JsonToken.VALUE_STRING;
    }
    
}
