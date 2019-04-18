import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by HRJ on 2019/4/16.
 */
public class LPA_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    /*
        line = 33$义生	0.0015630487375194626#程青竹:0.5000;袁承志:0.5000
        PR = 0.0015630487375194626
        name = 义生
        nameList = 程青竹:0.5000;袁承志:0.5000
        label = 33

     */
        String line = value.toString();
        int index_t = line.indexOf("\t");
        int index_j = line.indexOf("#");
        int index_dollar = line.indexOf("$");
        String PR = line.substring(index_t+1,index_j);
        String name = line.substring(index_dollar+1,index_t);
        String nameList = line.split("#")[1];
        String label = line.substring(0,index_dollar);
        StringTokenizer tokenizer = new StringTokenizer(nameList,";");
        while(tokenizer.hasMoreTokens()){
            String[] element = tokenizer.nextToken().split(":");
            context.write(new Text(element[0]),new Text(label+"#"+name));
            //程青竹 33#义生
            //袁承志 33#义生
        }
        context.write(new Text(name),new Text("#"+nameList)); // 义生#程青竹:0.5000;袁承志:0.5000
        context.write(new Text(name),new Text("$"+label)); // 义生$33
        context.write(new Text(name),new Text("@"+PR)); // 义生@0.0015630487375194626
    }
}
