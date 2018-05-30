package spark.streaming.kafka.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by sss on 2017/9/8.
 */

/** * Created by apple on 2017/2/15. */

public class MyTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

    public MyTextOutputFormat() {
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String path, Progressable progress) throws IOException {

        String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");

        CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, job);

        Path file = FileOutputFormat.getTaskOutputPath(job, path + codec.getDefaultExtension());

        FileSystem fs = file.getFileSystem(job);

        String file_path = path + codec.getDefaultExtension();

        Path newFile = new Path(FileOutputFormat.getOutputPath(job), file_path);

        FSDataOutputStream fileOut;

        if (fs.exists(newFile)) {
            fileOut = fs.append(newFile,4096,progress);
        } else {
            fileOut = fs.create(newFile, progress);
        }
        return new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);

    }
}
