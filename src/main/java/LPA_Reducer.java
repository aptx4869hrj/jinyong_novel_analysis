import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;

/**
 * Created by HRJ on 2019/4/16.
 */
public class LPA_Reducer extends Reducer<Text, Text, Text, Text>{
    protected void reduce(Text key, Iterable<Text> value, Context context){

    }

}
