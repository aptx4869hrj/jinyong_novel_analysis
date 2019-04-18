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
//        String[] name = value.toString().split("\\t+");
//        String[] names = name[0].split("#");
//        String namesList = names[1];
//        String[] nameList = name[1].split("#");
//
//        context.write(new Text(namesList), new Text("#" + nameList[1]));
//        context.write(new Text(namesList), new Text("$" + nameList[0]));
//
//        String[] name_split = nameList[1].split(";");
//
//        for (String string:name_split) {
//            String[] split = string.split(":");
//            context.write(new Text(split[0]), new Text(name[0]));
//        }
        //        String[] split = value.toString().split("\t");//[0]6#丁同  [1]0.008290701427018436#李三:0.250;李文秀:0.500;老头子:0.125;霍元龙:0.125
//        String name = split[0].split("#")[1];//丁同
//        String namevalue="#"+split[1].split("#")[1];//#李三:0.250;李文秀:0.500;老头子:0.125;霍元龙:0.125
//        String[] split1 = split[1].split("#");//[0]0.008290701427018436   [1]李三:0.250;李文秀:0.500;老头子:0.125;霍元龙:0.125
//        String[] split2 = split1[1].split(";");//李三:0.250
//        for(String str:split2){
//            String[] split3 = str.split(":");
//            context.write(new Text(split3[0]),new Text(split[0]));
//        }
//        context.write(new Text(name),new Text(namevalue));
//        context.write(new Text(name),new Text("@"+split1[0]));
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
        }
        context.write(new Text(name),new Text("#"+nameList));
        context.write(new Text(name),new Text("$"+label));
        context.write(new Text(name),new Text("@"+PR));
    }
}
