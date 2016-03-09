package Dictionary;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class CacheMapper extends Mapper<LongWritable, Text, Text, Text> {
    String fileName = null, language = null;
    public Map<String, String> translations = new HashMap<String, String>();
    Path[] cachedFilePaths = null;


    public void setup(Context context) throws IOException, InterruptedException {
        // TODO: determine the name of the additional language based on the file name
        cachedFilePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        fileName = cachedFilePaths[0].getName();
        language = fileName.substring(0, fileName.lastIndexOf('.'));

        // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1


    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
        String partOfSpeech = null, word = null, translations = null;
        String strValue = value.toString();
        String localKey = null, localValue = null;

        partOfSpeech = strValue.substring(strValue.indexOf('['), strValue.indexOf(']')) + "]";
        word = strValue.substring(0, strValue.indexOf(':'));

        translations = searchFile(partOfSpeech, word);

        // TODO: where there is a match from above, add language:translation to the list of translations in the existing record (if no match, add language:N/A
        localKey = word + ": " + partOfSpeech + " ";
        if (translations != null)
            localValue = (strValue.split("\\t"))[1] + "|" + translations;
        else
            localValue = (strValue.split("\\t"))[1];
        context.write(new Text(localKey), new Text(localValue));
    }

    private String searchFile(String partOfSpeech, String word) {
        String line = null, translations = language + ":N/A";

        try {
            if (fileName != null) {
                FileReader fileReader = new FileReader(fileName);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                while ((line = bufferedReader.readLine()) != null) {
                    if ((line.contains(word)) && (line.contains(partOfSpeech))) {
                        String[] tokens = line.split("\\t");
                        String tmpWord = tokens[0];
                        String tmpValue = tokens[1];
                        if (tmpWord.equalsIgnoreCase(word)) {
                            if (tmpValue != null) {
                                if (tmpValue.lastIndexOf(']') == tmpValue.length() - 1) {
                                    if (tmpValue.lastIndexOf('[') < tmpValue.lastIndexOf(']')) {
                                        String tempTranslations = tmpValue.substring(0, tmpValue.lastIndexOf('['));
                                        tempTranslations = tempTranslations.replace(';', ',');
                                        if (translations.contains("N/A")) {
                                            translations = language + ":" + tempTranslations;
                                        } else {
                                            translations = translations + "," + tempTranslations;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                bufferedReader.close();
                return translations;
            }

        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        return null;
    }
}
