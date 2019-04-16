import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by HRJ on 2019/4/16.
 */
public class PageRank_Reducer extends Reducer<Text, Text, Text, Text>{
    //输出与Build_Relationship_Reducer的输出一致
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        String nameList = "";
        double count = 0;
        for (Text values:value) {
            if(!values.toString().startsWith("#")){
                count +=Double.parseDouble(values.toString());
            }else {
                nameList = values.toString();
            }
        }

        context.write(key, new Text(String.valueOf(count) + nameList));
    }
}
