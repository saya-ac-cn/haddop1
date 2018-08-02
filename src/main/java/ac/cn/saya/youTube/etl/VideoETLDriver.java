package ac.cn.saya.youTube.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class VideoETLDriver implements Tool {

    private  Configuration conf = null;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] strings) throws Exception {
        conf = this.getConf();
        conf.set("mapreduce.framework.name","local");
        conf.set("inpath",strings[0]);
        conf.set("outpath",strings[1]);

        Job job = Job.getInstance(conf,"youtube-video-etl");
        //指定本次mr job jar包运行主类
        job.setJarByClass(VideoETLDriver.class);
        job.setMapperClass(VideoETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        this.initJobInputPath(job);
        this.initJobOutPutPath(job);

        return job.waitForCompletion(true)?0:1;
    }

    private void initJobOutPutPath(Job job)throws IOException
    {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("outpath");

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outPathString);
        if(fs.exists(outPath))
        {
            //如果之前存在输出目录则删除
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
    }

    private void initJobInputPath(Job job)throws IOException
    {
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("inpath");

        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(inPathString);
        if(fs.exists(inPath))
        {
            FileInputFormat.addInputPath(job,inPath);
        }
        else
        {
            throw new RuntimeException("该文件目录不存在："+inPathString);
        }
    }

    public static void main(String[] agrs) throws Exception {
        try
        {
            String [] arry = {"E:\\linshi\\hadoop\\YouTube\\video\\in\\0.txt","E:\\linshi\\hadoop\\YouTube\\video\\out"};
            int resultCode = ToolRunner.run( new VideoETLDriver(),arry);
            if(resultCode == 0)
            {
                System.out.println("Success");
            }
            else
            {
                System.err.println("Failt");
            }
            System.exit(resultCode);
        }catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
