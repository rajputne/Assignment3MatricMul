package mapreducejava;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Runner {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("ip-count");

        conf.setMapperClass(MatrixMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setReducerClass(MatrixReducer.class);        
        final File f = new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/InputFiles/InputFile";
        String outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Results";
        FileInputFormat.setInputPaths(conf, new Path(inFiles));
        FileOutputFormat.setOutputPath(conf, new Path(outFiles));
        JobClient.runJob(conf);

    }

}
