package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;

/**
 * Initial state before anything is read from document.
 * 
 * @author franck
 *
 */
public class CsvStateDocStart extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        // also, if comments enabled, may need to skip leading ones
        csvParser._reader.skipLeadingComments();
        // First things first: are we expecting header line? If so, read, process
        if (csvParser._schema.usesHeader()) {
            csvParser._readHeaderLine();
            csvParser._reader.skipLeadingComments();
        }
        // and if we are to skip the first data line, skip it
        if (csvParser._schema.skipsFirstDataRow()) {
            csvParser._reader.skipLine();
            csvParser._reader.skipLeadingComments();
        }
        
        /* Only one real complication, actually; empy documents (zero bytes).
         * Those have no entries. Should be easy enough to detect like so:
         */
        final boolean wrapAsArray = Feature.WRAP_AS_ARRAY.enabledIn(csvParser._formatFeatures);
        if (!csvParser._reader.hasMoreInput()) {
            csvParser._state = new CsvStateDocEnd();
            // but even empty sequence must still be wrapped in logical array
            if (wrapAsArray) {
                csvParser._parsingContext = csvParser._reader.childArrayContext(csvParser._parsingContext);
                return JsonToken.START_ARRAY;
            }
            return null;
        }
        
        if (wrapAsArray) {
            csvParser._parsingContext = csvParser._reader.childArrayContext(csvParser._parsingContext);
            csvParser._state = new CsvStateRecordStart();
            return JsonToken.START_ARRAY;
        }
        // otherwise, same as regular new entry...
        return new CsvStateRecordStart().handle(csvParser);
    }; 
}
