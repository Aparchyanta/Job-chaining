package FirstProject.JobChaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;



public class JobChainingDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

	}
	public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        Configuration conf=new Configuration();
        conf.set("filter", args[2]);
        conf.set("Salary", args[3]);
        
        Job ljob = new Job(conf);
        ControlledJob ljob1 =new ControlledJob(conf);
        ljob1.setJobName("getLocation");
        ljob=ljob1.getJob();
        
        ljob.setJarByClass(JobChainingDriver.class);
        ljob.setJobName(this.getClass().getName());
        
        //Configuration mapAConf = new Configuration(false);
       // ChainMapper.addMapper(job, LocationMapper.class, LongWritable.class, Text.class,Text.class, Text.class, mapAConf);
        
       // Configuration mapBConf = new Configuration(false);
       // ChainMapper.addMapper(job, SalaryMapper.class, LongWritable.class, Text.class, Text.class, Text.class, mapBConf);
        

        FileInputFormat.setInputPaths(ljob, new Path(args[0]));
        FileOutputFormat.setOutputPath(ljob, new Path(args[1]));
        ljob.setMapperClass(LocationMapper.class);
       
        //job.setReducerClass(CountR.class);

        ljob.setMapOutputKeyClass(Text.class);
        ljob.setMapOutputValueClass(Text.class);

        ljob.setNumReduceTasks(0);
        
        if (ljob.waitForCompletion(true)) {
            return 0;
        }
        Job sjob = new Job(conf);
        ControlledJob sjob1 =new ControlledJob(conf);
        sjob1.setJobName("getSalary");
        sjob=sjob1.getJob();
       
        sjob.setJarByClass(JobChainingDriver.class);
        sjob.setJobName(this.getClass().getName());
        
        //Configuration mapAConf = new Configuration(false);
       // ChainMapper.addMapper(job, LocationMapper.class, LongWritable.class, Text.class,Text.class, Text.class, mapAConf);
        
       // Configuration mapBConf = new Configuration(false);
       // ChainMapper.addMapper(job, SalaryMapper.class, LongWritable.class, Text.class, Text.class, Text.class, mapBConf);
        

        FileInputFormat.setInputPaths(sjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(sjob, new Path(args[1]));
        sjob.setMapperClass(SalaryMapper.class);
       
        //job.setReducerClass(CountR.class);

        ljob.setMapOutputKeyClass(Text.class);
        ljob.setMapOutputValueClass(Text.class);

        ljob.setNumReduceTasks(0);
        
        if (sjob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

}
