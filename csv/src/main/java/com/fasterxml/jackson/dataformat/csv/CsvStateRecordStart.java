package com.fasterxml.jackson.dataformat.csv;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State before logical start of a record, in which next
 * token to return will be {@link JsonToken#START_OBJECT}
 * (or if no Schema is provided, {@link JsonToken#START_ARRAY}).
 */
public class CsvStateRecordStart extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) {
        csvParser._columnIndex = 0;
        if (csvParser._columnCount == 0) { // no schema; exposed as an array
            csvParser._state = new CsvStateUnnamedValue();
            csvParser._parsingContext = csvParser._reader.childArrayContext(csvParser._parsingContext);
            return JsonToken.START_ARRAY;
        }
        // otherwise, exposed as an Object
        csvParser._parsingContext = csvParser._reader.childObjectContext(csvParser._parsingContext);
        csvParser._state = new CsvStateNextEntry();
        return JsonToken.START_OBJECT;
    }
    
}
