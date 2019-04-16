import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by HRJ on 2019/4/16.
 */
public class Person_Count_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        Set<String> lineName = new HashSet<String>();
        String line = values.toString();
        String[] names = line.split(" ");
        lineName.addAll(Arrays.asList(names));
        for (String firstName:lineName) {
            for (String secondName:lineName) {
                if (firstName.equals(secondName)){
                    continue;
                }else {
                    context.write(new Text(firstName + "," + secondName), new IntWritable(1));
                }
            }
        }
    }
}
