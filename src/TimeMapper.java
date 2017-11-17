import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.util.Calendar;

public class TimeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  Integer rioTimeZone = -3;

  private Integer adjustTimeZone(Integer hour){
    hour = hour + rioTimeZone;
    if(hour < 0)
      hour += 24;
    return hour;
  }

  private Integer getTweetHour(String epoch){
    Date tweet_dt = new Date(Long.parseLong(epoch));
    Calendar cal = Calendar.getInstance();
    cal.setTime(tweet_dt);
    Integer hour = cal.get(Calendar.HOUR_OF_DAY);
    return adjustTimeZone(hour);
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] data = value.toString().split(";");

    if(data.length == 4){
      Integer len = data[2].length();
      if(len <= 140 && data[0].length() <= 13){ // some epochs are invalid
        Integer hour = getTweetHour(data[0]);
        context.write(new IntWritable(hour), new IntWritable(1));
      }
    }
  }
}
