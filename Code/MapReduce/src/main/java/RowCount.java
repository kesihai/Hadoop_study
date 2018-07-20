import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RowCount {
    public static class RowCountMapper extends TableMapper<Text, LongWritable> {
        public static byte[] COL_NAME = "count".getBytes();
        Text t = new Text();
        LongWritable iWrite = new LongWritable(1);

        @Override
        protected void map(
                ImmutableBytesWritable key,
                Result value,
                Context context) throws IOException, InterruptedException {
            t.set(COL_NAME);
            context.write(t, iWrite);
        }
    }

    public static class RowCountReducer extends TableReducer<Text, LongWritable, NullWritable> {
        public static byte[] FAMILY = "f".getBytes();
        public static byte[] COL_COUNT= "count".getBytes();

        @Override
        protected void reduce(
                Text key,
                Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable intw: values){
                count += intw.get();
            }

            Put put = new Put(Bytes.toBytes("RowCount"));
            put.addColumn(FAMILY, COL_COUNT, Bytes.toBytes(count));
            context.write(NullWritable.get(), put);
        }
    }

    public static class RowCountCombin extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long i = 0;
            for (LongWritable val : values){
                i += val.get();
            }
            context.write(key, new LongWritable(i));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("please input two table name: sourceTable, resultTableName");
            return;
        }
        Job job = Job.getInstance(HBaseConfiguration.create(), "RowCount");
        job.setJarByClass(RowCount.class);
        job.setMapperClass(RowCountMapper.class);
        job.setReducerClass(RowCountReducer.class);
        job.setCombinerClass(RowCountCombin.class);
        job.setNumReduceTasks(1);

        Scan scan = new Scan();
        scan.setCaching(50000);
        scan.setCacheBlocks(false);

        final String sourceTable = args[0];
        final String targetTable = args[1];
        TableMapReduceUtil.initTableMapperJob(sourceTable, scan, RowCountMapper.class, Text.class,LongWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(targetTable, RowCountReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
