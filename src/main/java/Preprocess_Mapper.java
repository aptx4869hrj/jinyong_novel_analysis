import java.io.*;
import java.util.*;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.management.FileSystem;

/**
 * 该类做为一个 mapTask 使用。类声名中所使用的四个泛型意义为别为：
 *
 * KEYIN:   默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
 *      但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
 * VALUEIN: 默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 * KEYOUT：  是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
 * VALUEOUT：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
 */
public class Preprocess_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map阶段的业务逻辑就写在自定义的map()方法中 maptask会对每一行输入数据调用一次我们自定义的map()方法
     */
    //    hello,1 hello,1
    Set<String> name = new HashSet<String>();
    @Override
    protected void setup(Context context) throws IOException {
        File file = new File("File/nameFile/jinyong_all_person.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        String line = br.readLine();

        while (line != null){
            DicLibrary.insert(DicLibrary.DEFAULT, line);
            name.add(line);
            line = br.readLine();
        }
    }
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        Result result = DicAnalysis.parse(line);
        List<Term> term = result.getTerms();
        StringBuilder stringBuilder = new StringBuilder();
        for (Term terms : term) {
            if (name.contains(terms.getName())) {
                stringBuilder.append(terms.getName() + " ");
            }
        }
        String res =stringBuilder.length() > 0? stringBuilder.toString().substring(0,stringBuilder.length()-1):"";
        context.write(new Text(res),new IntWritable(1));
    }

}