/**
 * Map/Reduce框架运转在<key, value> 键值对上，
 * 框架把作业的输入看为是一组<key, value> 键值对，同样也产出一组 <key, value> 键值对做为作业的输出，这两组键值对的类型可能不同。
 * 框架需要对key和value的类(classes)进行序列化操作，
 * 这些类需要实现 Writable接口。
 * 为了方便框架执行排序操作，key类必须实现 WritableComparable接口。
 * 一个Map/Reduce 作业的输入和输出类型如下所示：
 * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
 */

import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
//        为每一个InputSplit产生一个map任务，而每个InputSplit由该作业的IntputFormat产生
//        Mapper extends JobConfigurable, Closeable
        public static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreElements()){
                word.set(tokenizer.nextToken());
                output.collect(word, one);
//                输出 <word, 1>键值对，有重复
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()){
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
//        指定combiner，每次map运行之后会按照key进行排序，然后把输出传递给本地的combiner，进行本地聚合。
        conf.setReducerClass(Reduce.class);
//        Reducer将每个key出现的次数求和

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        //指定TextInputFormat，一次处理一行
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
