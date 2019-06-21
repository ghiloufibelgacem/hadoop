package tn.enis.ghiloufi.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<LongWritable, Text, Text,  FloatWritable> {
	
	private Text age = new Text();
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns=line.split(",");
        age.set(columns[1]);
        if("age".equals(columns[1])){
        	return;
        }
        Float price =Float.parseFloat(columns[4]);
        FloatWritable income = new FloatWritable(price);
        context.write(age,income);
    }
}
