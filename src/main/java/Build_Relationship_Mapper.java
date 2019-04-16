import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class Build_Relationship_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String line = values.toString();
        String[] kv = line.split(",");
        context.write(new Text(kv[0]), new Text(kv[1]));
    }
}
