import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class LPA_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] name = value.toString().split("\\t+");
        String[] names = name[0].split("#");
        String namesList = names[1];
        String[] nameList = name[1].split("#");

        context.write(new Text(namesList), new Text("#" + nameList[1]));
        context.write(new Text(namesList), new Text("$" + nameList[0]));

        String[] name_split = nameList[1].split(";");

        for (String string:name_split) {
            String[] split = string.split(":");
            context.write(new Text(split[0]), new Text(name[0]));
        }
    }
}
