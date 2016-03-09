package Dictionary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;

public class DictionaryReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO iterate through values, parse, transform, and write to context
        String compositeValue = "";
        String localValue;
        HashMap<String, String> langTrans = new HashMap<String, String>();

        for (Text text : values) {
            String key = null;
            localValue = text.toString();
            if (localValue.contains("french:"))
                key = "french";
            else if (localValue.contains("german:"))
                key = "german";
            else if (localValue.contains("italian:"))
                key = "italian";
            else if (localValue.contains("portuguese:"))
                key = "portuguese";
            else if (localValue.contains("spanish:"))
                key = "spanish";
            else
                key = null;
            if (key != null) {
                if(!langTrans.containsKey(key))
                    langTrans.put(key, localValue);
                else{
                    String temp = localValue.substring((localValue.indexOf(':')+1), localValue.length());
                    localValue = langTrans.get(key) + "," + temp;
                    langTrans.put(key, localValue);
                }
            }
        }

        if(!langTrans.containsKey("french"))
            langTrans.put("french","french:N/A");
        if(!langTrans.containsKey("german"))
            langTrans.put("german", "german:N/A");
        if(!langTrans.containsKey("italian"))
            langTrans.put("italian","italian:N/A");
        if(!langTrans.containsKey("portuguese"))
            langTrans.put("portuguese", "portuguese:N/A");
        if(!langTrans.containsKey("spanish"))
            langTrans.put("spanish","spanish:N/A");

        compositeValue = langTrans.get("french")+ "|" + langTrans.get("german")+ "|"
                            + langTrans.get("italian") + "|" + langTrans.get("portuguese") + "|"
                                + langTrans.get("spanish");

        context.write(word, new Text(compositeValue));
    }
}
