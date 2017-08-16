import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.*;


public class TextPair implements WritableComparable<TextPair>{
    private Text first;
    private Text second;

    public TextPair(){
        /**
         * Writale 实现都必须要一个默认的构造函数
          */
        set(new Text(), new Text());
    }

    public TextPair(String first, String second){
        set(new Text(first), new Text(second));
    }


    public void set(Text first, Text second){
        this.first = first;
        this.second = second;
    }

    /***
     *
     * Writable实例易变、常重用，write()和readFields()应当避免分配对象
     * DataOutput 和 DataInput中有丰富的方法，应该优先调用
     */
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextPair){
            TextPair tmp = (TextPair)obj;
            return first.equals(tmp.first) && second.equals(tmp.second);
        }
        return false;
    }

    /**
     *
     * HashPartitioner 使用hashcode进行reduce分区
     */
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    /**
     *
     * 提供compareTo()来保证该类实现 WritableComparable()
     */
    public int compareTo(TextPair o) {
        int cmp = first.compareTo(o.first);
        if (cmp != 0){
            return cmp;
        }
        return second.compareTo(o.second);
    }

    /**
     * 实现一个更高效的RawComparator,避免了反序列化为某个特定类对象的去调用其compareTo（）函数
     */
    public static class Comparator extends WritableComparator{
        public static final Text.Comparator COMPARATOR = new Text.Comparator();

        public Comparator(){
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                /**
                 * 计算firstL1和firstL2,每个字节流中第一个Text字段的长度。
                 * 每个都由可变长度的整型(由WritableUtils的decodeVIntSize()返回)和它的编码值(由readVInt()返问)组成。
                 */
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0){
                    return cmp;
                }
                return COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }

        }
        }
        static {
            /**
             * 注册原始的comparator使得每次遇到TextPair类都会使用该原始comparator来作为默认comparator
             */
        WritableComparator.define(TextPair.class, new Comparator());
    }

    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR =new Text.Comparator();
        public FirstComparator() {
            super(TextPair.class);
        }
        @Override
        public int compare(byte[] b1,int s1,int l1,
                           byte[] b2,int s2,int l2) {
            try{
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            }catch(IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if(a instanceof TextPair && b instanceof TextPair) {
                return((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
}
