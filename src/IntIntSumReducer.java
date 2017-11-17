import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IntIntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values)
            sum = sum + value.get();

        context.write(key, new IntWritable(sum));
    }
}
