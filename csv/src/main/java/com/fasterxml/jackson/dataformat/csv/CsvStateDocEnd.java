package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State in which end marker is returned; either
 * null (if no array wrapping), or
 * {@link JsonToken#END_ARRAY} for wrapping.
 * This step will loop, returning series of nulls
 * if {@link #nextToken} is called multiple times.
 */
public class CsvStateDocEnd extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        csvParser._reader.close();
        if (csvParser._parsingContext.inRoot()) {
            return null;
        }
        // should always be in array, actually... but:
        boolean inArray = csvParser._parsingContext.inArray();
        csvParser._parsingContext = csvParser._parsingContext.getParent();
        return inArray ? JsonToken.END_ARRAY : JsonToken.END_OBJECT;
    }
}
