package index;

/**
 * B + tree
 * */
public class BpTree {
    /**
     * B+ tree
     */
    public BpNode root;
    /**
     * head in leaf node list of B+ tree
     */
    public BpNode leafHeadNode;

    public BpTree() {
        //defaul init root node is root node
        root = new BpNode(true, true);
        leafHeadNode = root;
    }

    public KvInNode selectKeyInTree(KvInNode searchKey) {
        return this.root.select(searchKey);
    }

    public void insertKey(KvInNode key) {
        this.root.insertKv(this,key);
    }
    /**
     * insert leaf list
     * */
    public void insertLeafList(BpNode node){
        if(this.leafHeadNode == null){
            this.leafHeadNode = node;
        }
        BpNode lastNode = leafHeadNode;
        //find last node
        while(lastNode.next!=null){
            lastNode = lastNode.next;
        }
        //insert node to list
        lastNode.next = node;
        node.prev = lastNode;
        node.next = null;

    }



}
