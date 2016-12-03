package minibase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by stiwari on 11/25/2016 AD.
 */
public class HeapFileEncoder {

    public static void convert(ArrayList<ArrayList<Integer>> tuples, File outFile, int npagebytes, int numFields) throws IOException {
        File tmpInput = File.createTempFile("tmpTable", ".txt");
        tmpInput.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmpInput));

        for (ArrayList<Integer> tuple : tuples) {
            for (Integer field : tuple) {
                bw.write(String.valueOf(field));
            }
        }
    }
}
