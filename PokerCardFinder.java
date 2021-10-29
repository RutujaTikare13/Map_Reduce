import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PokerCardFinder {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text suite = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            suite.set(arr[0]);
            // write to context key:suite(diamond, club, heart, spade) and value: face value of the card
            context.write(suite, new IntWritable(Integer.parseInt(arr[1])));
            }
        }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, MyArrayWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            // Read all the cards found into a list
            List<Integer> suiteCards = new ArrayList<>();
            for (IntWritable val : values) {
                suiteCards.add(val.get());
            }
            // Sort the list
            Collections.sort(suiteCards);
            ArrayList<IntWritable> missing = new ArrayList<IntWritable>();
            Integer expected = 1;
            for (Integer i : suiteCards) {
                // Wite all cards that were missing between the previous and currently found card. Eg if 4 was the last
                // card we saw and the current card is 10. Write 5, 6, 7, 8, 9.
                if (!expected.equals(i)) {
                    for(int j = expected; j < i; j++) {
                        missing.add(new IntWritable(expected));
                        expected++;
                    }
                }
                expected++;
            }
            // Add all after the last card in the deck for the suite. Eg: if 4 is the last card the write from 5 to 13
            if (expected != 14) {
                for (int i = expected; i < 14; i++) {
                    missing.add(new IntWritable(i));
                }
            }
            context.write(key, new MyArrayWritable(IntWritable.class,
                    missing.toArray(new IntWritable[missing.size()])));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: poker <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "poker");
        job.setJarByClass(PokerCardFinder.class);
        job.setMapperClass(PokerCardFinder.TokenizerMapper.class);
//        job.setCombinerClass(PokerCardFinder.IntSumReducer.class);
        job.setReducerClass(PokerCardFinder.IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyArrayWritable.class); //TODO: Decide on o/p class
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
