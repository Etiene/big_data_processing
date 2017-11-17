import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.util.Calendar;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class HashtagMapper extends Mapper<Object, Text, Text, IntWritable> {

  private Integer getTweetHour(String epoch){
    Date tweet_dt = new Date(Long.parseLong(epoch));
    Calendar cal = Calendar.getInstance();
    cal.setTime(tweet_dt);
    Integer hour = cal.get(Calendar.HOUR_OF_DAY);

    return hour;
  }

  private Matcher getHashtags(String tweet){
    String pattern = "\\B(#\\w*[a-zA-Z]+\\w*)";
    return Pattern.compile(pattern).matcher(tweet);
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] data = value.toString().toLowerCase().split(";");

    if(data.length == 4){
      Integer len = data[2].length();
      if(len <= 140 && data[0].length() <= 13){ // some epochs are invalid???

        Integer hour = getTweetHour(data[0]);
        if(hour == 2){
          Matcher matcher = getHashtags(data[2]);
          while (matcher.find()) {
               context.write(new Text(matcher.group(0)), new IntWritable(1));
          }
        }
      }
    }
  }
}
