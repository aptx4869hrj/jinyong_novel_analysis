import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class Person_Count_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value:values) {
            count = count + value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
