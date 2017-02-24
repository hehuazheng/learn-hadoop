package com.hzz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class WordCountChangeOutputFormat {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static final class ValueOnlyOutputFormat<K, V> extends FileOutputFormat<K, V> {

        public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            CompressionCodec codec = null;
            String extension = "";
            if(isCompressed) {
                Class file = getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(file, conf);
                extension = codec.getDefaultExtension();
            }

            Path file1 = this.getDefaultWorkFile(job, extension);
            FileSystem fs = file1.getFileSystem(conf);
            FSDataOutputStream fileOut;
            if(!isCompressed) {
                fileOut = fs.create(file1, false);
                return new LineRecordWriter(fileOut);
            } else {
                fileOut = fs.create(file1, false);
                return new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
            }
        }

        protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
            private static final String utf8 = "UTF-8";
            private static final byte[] newline;
            protected DataOutputStream out;

            public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
                this.out = out;
            }

            public LineRecordWriter(DataOutputStream out) {
                this(out, "\t");
            }

            private void writeObject(Object o) throws IOException {
                if(o instanceof Text) {
                    Text to = (Text)o;
                    this.out.write(to.getBytes(), 0, to.getLength());
                } else {
                    this.out.write(o.toString().getBytes("UTF-8"));
                }

            }

            public synchronized void write(K key, V value) throws IOException {
                boolean nullKey = key == null || key instanceof NullWritable;
                boolean nullValue = value == null || value instanceof NullWritable;
                if(!nullValue) {
                    if(!nullKey) {
                        this.writeObject(key);
                        this.writeObject("-");
                    }
                    this.writeObject(value);
                    this.out.write(newline);
                }
            }

            public synchronized void close(TaskAttemptContext context) throws IOException {
                this.out.close();
            }

            static {
                try {
                    newline = "\n".getBytes("UTF-8");
                } catch (UnsupportedEncodingException var1) {
                    throw new IllegalArgumentException("can\'t find UTF-8 encoding");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountChangeOutputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(ValueOnlyOutputFormat.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        ValueOnlyOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
