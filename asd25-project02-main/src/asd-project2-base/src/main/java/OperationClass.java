public class OperationClass {

    private String op_id;
    private long sender_id;
    private long receiver_id;
    private long value;

    public OperationClass(String op_id, long sender_id, long receiver_id, long value){
        this.op_id = op_id;
        this.sender_id = sender_id;
        this.receiver_id = receiver_id;
        this.value = value;
    }

    public String getOp_id(){
        return op_id;
    }

    public long getSender_id() {
        return sender_id;
    }

    public long getReceiver_id() {
        return receiver_id;
    }

    public long getValue() {
        return value;
    }

}
