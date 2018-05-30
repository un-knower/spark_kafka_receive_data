package spark.streaming.kafka.outputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

/** * Created by apple on 2017/2/15.

 spark streaming 实现根据文件内容自定义文件名，并实现文件内容追加
 网址：http://www.infocool.net/kb/WWW/201702/298137.html

 *
 * */
public class MyMultipleTextOutputFormat<K, V> extends MultipleTextOutputFormat<K, V> {

    private TextOutputFormat<K, V> theTextOutputFormat = null;

    public RecordWriter getRecordWriter(final FileSystem fs, final JobConf job, final String name, final Progressable arg3) throws IOException {

        return new RecordWriter() {

            TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap();

            public void write(Object key, Object value) throws IOException {
                //根据消息内容，定义输出路径和输出内容（同时清洗数据）
                String finalPath = "/user/yimr/sss/tmp21/" + value.toString();

                RecordWriter rw = (RecordWriter) this.recordWriters.get(finalPath);
                try {
                    if (rw == null) {
                        rw = getBaseRecordWriter(fs, job, finalPath, arg3);
                        this.recordWriters.put(finalPath, rw);
                    }
                    rw.write(key.toString(), value);
                } catch (Exception e) {
                    //一个周期内，job不能完成，下一个job启动，会造成同时写一个文件的情况，变更文件名，添加后缀  尽量避免
                    //this.rewrite(finalPath + "-", key.toString()+value);
                }
            }

            public void rewrite(String path, String line) {
                String finalPath = path + new Random().nextInt(10);
                RecordWriter rw = (RecordWriter) this.recordWriters.get(finalPath);
                try {
                    if (rw == null) {
                        rw = getBaseRecordWriter(fs, job, finalPath, arg3);
                        this.recordWriters.put(finalPath, rw);
                    }
                    rw.write(line, null);
                } catch (Exception e) {
                    //重试
                    this.rewrite(finalPath, line);
                }
            }

            public void close(Reporter reporter) throws IOException {
                Iterator keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter rw = (RecordWriter) this.recordWriters.get(keys.next());
                    rw.close(reporter);
                }

                this.recordWriters.clear();
            }
        };
    }


    protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job, String path, Progressable arg3) throws IOException {
        if (this.theTextOutputFormat == null) {
            this.theTextOutputFormat = new MyTextOutputFormat();
        }

        return this.theTextOutputFormat.getRecordWriter(fs, job, path, arg3);
    }
}