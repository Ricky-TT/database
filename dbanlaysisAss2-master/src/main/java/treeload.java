import index.BpTree;
import index.BpTreeIndex;

public class treeload {
    public static void main(String[] args) {
        if(args.length!=1){
            throw  new RuntimeException("the number of args is no valid");
        }
        int pageSize = Integer.parseInt(args[0]);
        BpTree bpTree = new BpTree();
        long startTime = System.currentTimeMillis();
        BpTreeIndex.constructBpIndex(pageSize, bpTree);
        long endTime = System.currentTimeMillis();
        System.out.println("load tree time is :"+(endTime-startTime)+"ms");
    }
}
