import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.io.IOException;

/**
 * Created by HRJ on 2019/4/16.
 */
public class LPA_Reducer extends Reducer<Text, Text, Text, Text>{
//    Map<String, String> globalLabel = new HashMap<String, String>();
//
//    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
//        Map<String, Double> name_weight = new HashMap<String, Double>();
//        Map<String, String> name_label = new HashMap<String, String>();
//
//        String peopleRelation = "";
//        String peoplePageRank = "";
//        List<String> people_label = new ArrayList<String>();
//        for (Text values :value) {
//            String content = values.toString();
//            if (content.startsWith("#")){
//                peopleRelation = content.replace("#","");
//            }else if (content.startsWith("$")){
//                peoplePageRank = content.replace("$", "");
//            }else{
//                people_label.add(content);
//            }
//        }
//
//        String[] arrName_weight = peopleRelation.split(",");
//        for (String string:arrName_weight) {
//            String[] split = string.split("#");
//            name_weight.put(split[0], Double.parseDouble(split[1]));
//        }
//
//        Map<String, Double> label_weight = new HashMap<String, Double>();
//        for (String string:people_label) {
//            String[] split = string.split("#");
//            name_label.put(split[1], split[0]);
//            String label = split[0];
//            if (globalLabel.containsKey(split[1])){
//                label = globalLabel.get(split[1]);
//            }
//            if (!label_weight.containsKey(label)){
//                label_weight.put(label, name_weight.get(split[1]));
//            }else {
//                Double weight = label_weight.get(label);
//                if (name_weight.containsKey(split[1])){
//                    label_weight.put(label, weight);
//                }
//            }
//        }
//
//        double maxWeight = 0;
//        String newLabel = "";
//        for (Map.Entry<String, Double> entry:label_weight.entrySet()) {
//            double weight = entry.getValue();
//            if (maxWeight < weight){
//                maxWeight = weight;
//                newLabel = entry.getKey();
//            }
//        }
//
//        globalLabel.put(key.toString(), newLabel);
//
//        Text keyWrite = new Text(newLabel + "#" + key.toString());
//        Text valueWrite = new Text(peoplePageRank + "#" + peopleRelation);
//        context.write(keyWrite, valueWrite);
//    }
    Map<String,String> name_label_map = new HashMap<String, String>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String label = "";
        String nameList = "";
        String pr = "";
        Map<String,String> relation_name_label = new HashMap<String, String>();
        for(Text text:values){
            String str = text.toString();
            if (str.length() > 0 && str.charAt(0) == '$'){
                label = str.replace("$","");
            }else if (str.length() > 0 &&str.charAt(0) == '@'){
                pr = str.replace("@","");
            }else if (str.length() > 0 &&str.charAt(0) == '#'){
                nameList = str.replace("#","");
            }else if (str.length() > 0){
                String[] element = str.split("#");
                relation_name_label.put(element[1],element[0]);
            }
        }

        Map<String,Float> label_pr_map = new HashMap<String, Float>();
        StringTokenizer nameList_Tokenizer = new StringTokenizer(nameList,";");
        while(nameList_Tokenizer.hasMoreTokens()){
            String[] name_pr = nameList_Tokenizer.nextToken().split(":");
            Float current_pr = Float.parseFloat(name_pr[1]);
            String current_label = relation_name_label.get(name_pr[0]);
            Float label_pr;
            if ((label_pr = label_pr_map.get(current_label)) != null){
                label_pr_map.put(current_label,label_pr+current_pr);
            }else{
                label_pr_map.put(current_label,current_pr);
            }
        }


        StringTokenizer tokenizer = new StringTokenizer(nameList,";");
        float maxPr = Float.MIN_VALUE;
        List<String> maxNameList = new ArrayList<String>();
        while (tokenizer.hasMoreTokens()){
            String[] element = tokenizer.nextToken().split(":");
            float tmpPr = label_pr_map.get(relation_name_label.get(element[0]));
            if (maxPr < tmpPr){
                maxNameList.clear();
                maxPr = tmpPr;
                maxNameList.add(element[0]);
            }else if (maxPr == tmpPr){
                maxNameList.add(element[0]);
            }
        }

        Random random = new Random();
        int index = random.nextInt(maxNameList.size());
        String target_name = maxNameList.get(index);
        String target_label = relation_name_label.get(target_name);
        if (name_label_map.get(target_name) != null){
            target_label = name_label_map.get(target_name);
        }else{
            name_label_map.put(key.toString(),target_label);
        }
        if (target_label == null){
            System.out.println();
        }
        context.write(new Text(target_label + "$" + key.toString()),new Text(pr + "#" + nameList));
    }

}
