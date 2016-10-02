package mapreducejava;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Counts all of the hits for an ip. Outputs all ip's
 */
public class MatrixReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
   



        int[] row = new int[3];
        int[] col = new int[3];

        while (values.hasNext()) {
            String[] entries = values.next().toString().split(",");
            if (entries[0].matches("a")) {
                int index = Integer.parseInt(entries[2].trim());
                row[index] = Integer.parseInt(entries[3].trim());
                System.out.println("Row index-->" + index);
            }
            if (entries[0].matches("b")) {
                int index = Integer.parseInt(entries[1].trim());
                col[index] = Integer.parseInt(entries[3].trim());
                System.out.println("Col index-->" + index);
            }
        }

     
        int total = 0;
        for (int i = 0; i < 3; i++) {
            total += row[i] * col[i];
        }
        output.collect(key, new IntWritable(total));
        System.out.println("Keys:" + key.toString() + "-Values:" + total);
        
    }

}
