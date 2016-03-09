package Dictionary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class DictionaryMapper  extends Mapper<LongWritable, Text, Text, Text> {
      // TODO define class variables for translation, language, and file name
      String filename, language;

      public void setup(Context context) {
      // TODO determine the language of the current file by looking at it's name
            FileSplit inputFileSplit = (FileSplit) context.getInputSplit();
            filename = inputFileSplit.getPath().getName();
            language = filename.substring(0, filename.lastIndexOf('.'));
                    //context.getConfiguration().get(inputFileSplit.getPath().getName());
      }

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String localkey = null;
            String localvalue = null;
            String partOfSpeech = "";
            String translations = "";
            int i=0;
            String[] validPartOfSpeech = {"[adjective]", "[adverb]", "[conjunction]", "[noun]", "[preposition]", "[pronoun]", "[verb]"};


      // TODO instantiate a tokenizer based on a regex for the file
            String[] inTokens = value.toString().split("\\t");

      // iterate through the tokens of the line, parse, transform and write to context
            for(String str: inTokens){
                  if(i==0)
                        localkey = str;
                  else if(i==1)
                        localvalue = str;
                  else if(i>1){
                        System.out.println("Bad record.. discarding");
                        localkey = null;
                        localvalue = null;
                  }
                  i++;
            }

            if(localvalue!=null){
                  if(localvalue.lastIndexOf(']')== (localvalue.length()-1)){
                        if((localvalue.lastIndexOf('[')) < localvalue.length()-1){
                              // persist the local value as it's a valid record
                              partOfSpeech = localvalue.substring(localvalue.lastIndexOf('['), localvalue.length());
                              for(String val: validPartOfSpeech){
                                    if(partOfSpeech.equalsIgnoreCase(val)){
                                          translations = localvalue.substring(0, localvalue.lastIndexOf('['));
                                          translations = translations.replace(';',',');
                                          localkey = localkey + ": " +partOfSpeech + " ";
                                          localvalue = language + ":" + translations;
                                          context.write(new Text(localkey), new Text(localvalue));
                                    }
                              }
                        }
                  }
            }
      }
}
