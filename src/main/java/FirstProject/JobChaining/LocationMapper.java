package FirstProject.JobChaining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text>{
@Override
protected void map(LongWritable key, Text value,
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	Configuration conf = context.getConfiguration();
	String loc = conf.get("filter");
	String record = value.toString();
	if(record.contains(loc)) 
	{
      	//String line=value.toString();
		String[] arr=record.split(",");
		String id=arr[0];
		String location=arr[2];
		String name=arr[1];
		String salary=arr[3];
		
			context.write(new Text(id), new Text(name+","+location+","+salary));
		
	}
}
}
