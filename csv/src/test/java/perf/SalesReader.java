package perf;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

/**
 * Manual test 
 */
public final class SalesReader
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 1) {
            System.err.println("Usage: java .... [input file]");
            System.exit(1);
        }
        new SalesReader().read(new File(args[0]));
    }

    private void read(File inputFile) throws IOException, InterruptedException
    {
        int x = 1;
        while (x<3) {
            Class<?> cls = ((x & 1) == 0) ? Map.class : SalesEntry.class;
//            Class<?> cls = ((x & 1) == 0) ? Map.class : StringPair.class;
            ++x;
            readAll(inputFile, cls);
            Thread.sleep(500L); 
        }
    }

    private <T> int readAll(File inputFile, Class<T> cls) throws IOException
    {
        System.out.print("Reading input as "+cls.getName()+" instances: ");
        
        int count = 0;
        CsvMapper mapper = new CsvMapper();
        
        CsvSchema schema = CsvSchema.builder()
            .setUseHeader(true)
            .build();
        
        MappingIterator<T> it = mapper.readerFor(cls)
                .with(schema)
                .readValues(inputFile);
        
//        CsvSchema schema = mapper.schemaFor(StringPair.class);
//        schema.skipsFirstDataRow();
        
//        MappingIterator<T> it = mapper.readerFor(cls)
//                                      .with(schema)
//                                      .with(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE)
//                                      .readValues(inputFile);

        while (it.hasNext()) {
            T row = it.nextValue();
            ++count;
            if ((count & 0x3FFF) == 0) {
                System.out.print('.');
            }
            System.out.println("Sale -> " + row.toString()); 
        }
        System.out.println();
        it.close();
        return count;
    }
    
    @JsonPropertyOrder({ "first", "second" })
    static class StringPair {
        public String first, second;
    }
    
}
