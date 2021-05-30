package index;

import java.util.List;

/**
 * key-value pair in node
 */
public class KvInNode {
    /**
     * true:this is range select,false: it is not range select
     * */
    public boolean rangeFlag;
    /**
     * for range select
     * */
    public List<Integer> selectRsByRangeList;
    /** key*/
    public String key;
    /** only for leaf node, point to the address of record*/
    public Integer recordAddr;

    public KvInNode(String key) {
        this.key = key;

    }

    /**
     * compare key
     */
    public int wholeMatch(KvInNode compareKv) {
        int flag = key.compareTo(compareKv.key);
        if (flag < 0) {
            //for range select
            if(compareKv.key.indexOf(key) ==0){
                return 0;
            }
            return -1;
        }else if(flag == 0) {
            return 0;
        }
        else {
            return 1;
        }
    }

    public boolean leafMathch(KvInNode compareKv){
        return this.key.indexOf(compareKv.key) == 0;
    }

}
