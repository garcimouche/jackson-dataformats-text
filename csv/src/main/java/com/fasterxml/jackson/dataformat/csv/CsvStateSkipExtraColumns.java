package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * State in which we have encountered more column values than there should be,
 * and need to basically skip extra values if callers tries to advance parser
 * state.
 *
 * @since 2.6
 */
public class CsvStateSkipExtraColumns extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        while (csvParser._reader.nextString() != null) { }

        // But once we hit the end of the logical line, get out
        // NOTE: seems like we should always be within Object, but let's be conservative
        // and check just in case
        csvParser._parsingContext = csvParser._parsingContext.getParent();
        csvParser._state = csvParser._reader.startNewLine() ? new CsvStateRecordStart() : new CsvStateDocEnd();
        return (csvParser.setCurrToken(csvParser._parsingContext.inArray() ? JsonToken.END_ARRAY : JsonToken.END_OBJECT));
    }
}
