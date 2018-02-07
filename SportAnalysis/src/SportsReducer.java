import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;



public class SportsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    		int sum=0;
    		for(IntWritable value: values){     //Recieves sport and for each sport sums the number of times the corresponding athletes were mentioned
    			sum=sum+value.get();              //Sets the final sum as the result
    		}
    		result.set(sum);

context.write(key,result);                   //outputs the sport and its athletes mention count
        }


}
