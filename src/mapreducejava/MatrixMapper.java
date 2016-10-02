package mapreducejava;

import Business.Matrix;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper that takes a line from an Apache access log and emits the IP with a
 * count of 1. This can be used to count the number of times that a host has hit
 * a website.
 */
public class MatrixMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    // Reusable IntWritable for the count
    private static final IntWritable one = new IntWritable(1);

    public void map(LongWritable fileOffset, Text lineContents,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        List<Matrix> myMatrix = new ArrayList<Matrix>();
        String[] csv = lineContents.toString().split(",");
        String matrix = csv[0].trim();
        String row;
        String column;
        String myKey;
        if (matrix.contains("a")) {
            for (int i = 0; i < 3; i++) {
                row = csv[1].trim();
                myKey = row + i;
                System.out.print(myKey + ":" + lineContents + "\n");
                output.collect(new Text(myKey), lineContents);
                Matrix mat = new Matrix();
                mat.setKey("FromA"+myKey);
                mat.setValue(lineContents.toString());
                myMatrix.add(mat);
            }
        }
        if (matrix.contains("b")) {
            for (int i = 0; i < 3; i++) {
                column = csv[2].trim();
                myKey = i+column;
                System.out.print(myKey + ":" + lineContents + "\n");
                output.collect(new Text(myKey), lineContents);
                Matrix mat = new Matrix();
                mat.setKey("FromB"+myKey);
                mat.setValue(lineContents.toString());
                myMatrix.add(mat);
            }
        }
        for (Matrix m : myMatrix) {
            System.out.println("Keys-->" + m.getKey() + "Values" + m.getValue());
        }

    }
}
