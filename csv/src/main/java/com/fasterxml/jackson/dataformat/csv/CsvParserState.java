/**
 * 
 */
package com.fasterxml.jackson.dataformat.csv;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Abstract top level for CSV Parsing state machine
 *  
 * @author franck
 *
 */
public abstract class CsvParserState {
    
    /**
     * handle the current token
     * @param csvParser the context will be updated with the next state
     * @return the {@link JsonToken} the element being read
     */
    public JsonToken handle(CsvParser csvParser) throws IOException{
        throw new IllegalStateException();
    }

}
