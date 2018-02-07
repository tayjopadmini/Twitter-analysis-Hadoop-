import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HashReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {

            sum= sum + value.get();       //Recieves the hash tag word and for each word sums the 1's

        }

               result.set(sum);            //Sets the final sum as the result

        context.write(key,result);         //outputs the hash tag words and its count

    }

}
