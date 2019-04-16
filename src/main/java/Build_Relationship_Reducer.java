import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by HRJ on 2019/4/16.
 */
public class Build_Relationship_Reducer extends Reducer<Text, Text, Text, NullWritable>{
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double count = 0;
        StringBuilder stringBuilder = new StringBuilder();
        List<String> list = new ArrayList<String>();
        for (Text value:values) {
            list.add(value.toString());
            String[] numValue = value.toString().split("\\s+");
            count += Integer.parseInt(numValue[1]);
        }

        for (String text:list) {
            String[] numValue = text.split("\\s+");
            double number = Integer.parseInt(numValue[1]);
            double scale = number/count;
            stringBuilder.append(numValue[0] + ":" + String.format("%.4f", scale) + ";");
        }

        stringBuilder.insert(0, key.toString() + "\t" + "0.1#");
        String res = stringBuilder.toString().substring(0, stringBuilder.length() - 1);

        context.write(new Text(res), NullWritable.get());
    }
}
