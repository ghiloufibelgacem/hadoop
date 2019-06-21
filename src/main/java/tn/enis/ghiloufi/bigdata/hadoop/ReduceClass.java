package tn.enis.ghiloufi.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, FloatWritable, Text, Text> {
	
	float min = 0;
	float max = 0;
	int total = 0;

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		
		int total = 0;
        float min = -1;
        float max = -1;
        for (FloatWritable value : values)
        {
        	total++;
        	if(value.get()<min ||min==-1) min = value.get();
        	if(value.get()>max || max==-1) max = value.get();
        }
        context.write(key, new Text("minPrice : "+min+" , maxPrice :"+max+" , nbrClient :"+total));
    }
		
	}