//package bbejeck.mapred.coocurrance;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 11/24/12
 * Time: 12:55 AM
 */
public class NewText implements Writable,WritableComparable<NewText> {

    private Text word;

    public NewText(Text word) {
        this.word = word;
    }

    public NewText(String word  ) {
        this(new Text(word));
    }

    public NewText() {
        this.word = new Text();
    }

    @Override
    public int compareTo(NewText other) {                         // A compareTo B
        int returnVal = this.word.compareTo(other.getWord());      // return -1: A < B


        if(returnVal != 0){                                        // return 0: A = B
            return -returnVal;                                      // return 1: A > B
        }
        return 0;
    }

    public static NewText read(DataInput in) throws IOException {
        NewText newText = new NewText();
        newText.readFields(in);
        return newText;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);

    }

    @Override
    public String toString() {
        return "( "+word+ " ), ";
    }

    public void set(String word){
        this.word.set(word);
    }
    public Text getWord() {
        return word;
    }

}