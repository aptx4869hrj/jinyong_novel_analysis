import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class PageRank_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] name = value.toString().split("\t");
        String[] names = name[1].split("#");
        context.write(new Text(name[0]), new Text("#" + names[1]));

        double pageRank = Double.parseDouble(names[0]);
        String[] nameList = names[1].split(";");
        for (String name_value:nameList) {
            String[] nameValue = name_value.split(":");
            double relation = Double.parseDouble(nameValue[1]);
            double cal = pageRank * relation;
            context.write(new Text(nameValue[0]), new Text(String.valueOf(cal)));
        }
    }

}
