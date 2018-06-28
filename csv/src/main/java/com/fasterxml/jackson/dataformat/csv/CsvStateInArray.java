package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;

/**
 * State in which a column value has been determined to be of
 * an array type, and will need to be split into multiple
 * values. This can currently only occur for named values.
 * 
 * @since 2.5
 */
public class CsvStateInArray extends CsvParserState {

    @Override
    public JsonToken handle(CsvParser csvParser) throws IOException {
        int offset = csvParser._arrayValueStart;
        if (offset < 0) { // just returned last value
            csvParser._parsingContext = csvParser._parsingContext.getParent();
            // no arrays in arrays (at least for now), so must be back to named value
            csvParser._state = new CsvStateNextEntry();
            return JsonToken.END_ARRAY;
        }
        int end = csvParser._arrayValue.indexOf(csvParser._arraySeparator, offset);

        if (end < 0) { // last value
            csvParser._arrayValueStart = end; // end marker, regardless

            // 11-Feb-2015, tatu: Tricky, As per [dataformat-csv#66]; empty Strings really
            //     should not emit any values. Not sure if trim
            if (offset == 0) { // no separator
                // for now, let's use trimming for checking
                if (csvParser._arrayValue.isEmpty() || csvParser._arrayValue.trim().isEmpty()) {
                    csvParser. _parsingContext = csvParser._parsingContext.getParent();
                    csvParser._state = new CsvStateNextEntry();
                    return JsonToken.END_ARRAY;
                }
                csvParser._currentValue = csvParser._arrayValue;
            } else {
                csvParser._currentValue = csvParser._arrayValue.substring(offset);
            }
        } else {
            csvParser._currentValue = csvParser._arrayValue.substring(offset, end);
            csvParser._arrayValueStart = end+csvParser._arraySeparator.length();
        }
        if (csvParser.isEnabled(Feature.TRIM_SPACES)) {
            csvParser._currentValue = csvParser._currentValue.trim();
        }
        if (csvParser._nullValue != null) {
            if (csvParser._nullValue.equals(csvParser._currentValue)) {
                return JsonToken.VALUE_NULL;
            }
        }
        return JsonToken.VALUE_STRING;
    }
}
