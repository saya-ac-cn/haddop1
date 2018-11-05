package ac.cn.saya.youTube.etl;

import ac.cn.saya.youTube.util.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class VideoETLMapper extends Mapper<Object,Text,NullWritable,Text> {

    private static final Logger LOGGER = Logger.getLogger(VideoETLMapper.class);

    private Text result = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       LOGGER.debug("处理前："+value.toString());
       String line = ETLUtil.getETLString(value.toString());
       LOGGER.debug("处理后："+line);
       if(line != null)
       {
           result.set(line);
           context.write(NullWritable.get(),result);
       }

    }
}
