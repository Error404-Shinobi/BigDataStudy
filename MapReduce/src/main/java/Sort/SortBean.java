package Sort;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author xuansama
 * @Date 2021/2/16 - 4:37 下午
 */
public class SortBean implements WritableComparable <SortBean> {
    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String   toString() {
        return  word + "\t" + num;
    }

    //实现比较器，指定排序的规则
    /*
        第一列（word）按照字典顺序进行排列
        第一列相同的时候，第二列（num）按照升序进行排列
     */
    @Override
    public int compareTo(@NotNull SortBean sortBean) {
        //先对word排列
        int result = this.word.compareTo(sortBean.word);
        //如果word相同，则按照num进行排序
        if (result == 0) {
            return this.num - sortBean.num;
        }
        return result;
    }

    //实现序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);
    }

    //实现反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.word=in.readUTF();
        this.num=in.readInt();
    }
}
