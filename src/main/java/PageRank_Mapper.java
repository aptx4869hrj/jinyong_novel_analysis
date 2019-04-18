import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class PageRank_Mapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //任通武	0.1#胡斐:0.4286;张九:0.0714;汪铁鹗:0.1429;相国夫人:0.0714;福康安:0.2143;马春花:0.0714
        //name[0] = 仁通武, name[1] = 0.1#胡斐:0.4286;张九:0.0714;汪铁鹗:0.1429;相国夫人:0.0714;福康安:0.2143;马春花:0.0714
        //names[0] = 0.1, names[1] = 胡斐:0.4286;张九:0.0714;汪铁鹗:0.1429;相国夫人:0.0714;福康安:0.2143;马春花:0.0714

//        String[] name = value.toString().split("\t");
//        String[] names = name[1].split("#");
//        context.write(new Text(name[0]), new Text("#" + names[1]));//write(仁通武#胡斐:0.4286;张九:0.0714;汪铁鹗:0.1429;相国夫人:0.0714;福康安:0.2143;马春花:0.0714)
//
//        double pageRank = Double.parseDouble(names[0]);//pagerank = 0.1
//        String[] nameList = names[1].split(";");
//        for (String name_value:nameList) {
//            String[] nameValue = name_value.split(":");
//            double relation = Double.parseDouble(nameValue[1]);
//            double cal = pageRank * relation;
//            context.write(new Text(nameValue[0]), new Text(String.valueOf(cal)));
//    }
        String line = value.toString();
        int index_dollar = line.indexOf("$");
        if (index_dollar != -1){
            line = line.substring(index_dollar+1);
        }
        int index_t = line.indexOf("\t");
        int index_j = line.indexOf("#");
        double PR = Double.parseDouble(line.substring(index_t+1,index_j));
        String name = line.substring(0,index_t);
        String names = line.substring(index_j+1);
        for (String name_value:names.split(";")){
            String[] nv = name_value.split(":");
            double relation = Double.parseDouble(nv[1]);
            double cal = PR * relation;
            context.write(new Text(nv[0]),new Text(String.valueOf(cal)));
        }
        context.write(new Text(name),new Text("#"+line.substring(index_j+1)));
    }

}
