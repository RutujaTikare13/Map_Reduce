import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

class MyArrayWritable extends ArrayWritable {
    public MyArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
    super(valueClass, values);
    }

    public MyArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        for(IntWritable i : get()){
            i.write(arg0);
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(get());
    }
}