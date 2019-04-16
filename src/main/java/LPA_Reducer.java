import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by HRJ on 2019/4/16.
 */
public class LPA_Reducer extends Reducer<Text, Text, Text, Text>{
    Map<String, String> globalLabel = new HashMap<String, String>();

    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        Map<String, Double> name_weight = new HashMap<String, Double>();
        Map<String, String> name_label = new HashMap<String, String>();

        String peopleRelation = "";
        String peoplePageRank = "";
        List<String> people_label = new ArrayList<String>();
        for (Text values :value) {
            String content = values.toString();
            if (content.startsWith("#")){
                peopleRelation = content.replace("#","");
            }else if (content.startsWith("$")){
                peoplePageRank = content.replace("$", "");
            }else{
                people_label.add(content);
            }
        }
        
        String[] arrName_weight = peopleRelation.split(",");
        for (String string:arrName_weight) {
            String[] split = string.split("#");
            name_weight.put(split[0], Double.parseDouble(split[1]));
        }
        
        Map<String, Double> label_weight = new HashMap<String, Double>();
        for (String string:people_label) {
            String[] split = string.split("#");
            name_label.put(split[1], split[0]);
            String label = split[0];
            if (globalLabel.containsKey(split[1])){
                label = globalLabel.get(split[1]);
            }
            if (!label_weight.containsKey(label)){
                label_weight.put(label, name_weight.get(split[1]));
            }else {
                Double weight = label_weight.get(label);
                if (name_weight.containsKey(split[1])){
                    label_weight.put(label, weight);
                }
            }
        }

        double maxWeight = 0;
        String newLabel = "";
        for (Map.Entry<String, Double> entry:label_weight.entrySet()) {
            double weight = entry.getValue();
            if (maxWeight < weight){
                maxWeight = weight;
                newLabel = entry.getKey();
            }
        }

        globalLabel.put(key.toString(), newLabel);

        Text keyWrite = new Text(newLabel + "#" + key.toString());
        Text valueWrite = new Text(peoplePageRank + "#" + peopleRelation);
        context.write(keyWrite, valueWrite);
    }

}
