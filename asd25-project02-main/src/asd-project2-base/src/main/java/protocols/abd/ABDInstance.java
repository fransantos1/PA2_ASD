package protocols.abd;

import io.netty.buffer.ByteBuf;
import protocols.abd.messages.JoinReplyMsg;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ABDInstance {

    private final long instanceId;
    private long key;
    private long val;
    private long tagLeft;
    private long tagRight;
    private long opSeq;

    // true -> Ready for request
    // false -> Not ready for request
    private boolean state;

    public ABDInstance(long id, long key){
        this.instanceId = id;
        this.key = key;
    }

    public ABDInstance(long id, long key, long value){
        this.instanceId = id;
        this.key = key;
        this.val = value;
        state = true;
    }

    public ABDInstance(long id, long key, long value, long tagRight, long tagLeft, long opSeq){
        this.instanceId = id;
        this.key = key;
        this.val = value;
        this.tagRight = tagRight;
        this.tagLeft = tagLeft;
        this.opSeq = opSeq;
        state = true;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public boolean isState() {
        return state;
    }

    public void setState(){
        state = !state;
    }

    public long getKey() {
        return key;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getVal() {
        return val;
    }

    public void setTag(long left, long right){
        tagLeft = left;
        tagRight = right;
    }

    public void setOpSeq(long opSeq) {
        this.opSeq = opSeq;
    }

    public void addOneOpSeq(){
        opSeq++;
    }

    public void setVal(long val) {
        this.val = val;
    }

    public long getTagLeft(){
        return tagLeft;
    }

    public long getTagRight(){
        return tagRight;
    }

    public static ISerializer<ABDInstance> serializer = new ISerializer<>() {
        @Override
        public void serialize(ABDInstance instance, ByteBuf out) throws IOException {

            out.writeLong(instance.instanceId);
            out.writeLong(instance.key);
            out.writeLong(instance.val);

            out.writeLong(instance.tagLeft);
            out.writeLong(instance.tagRight);

            out.writeLong(instance.opSeq);
        }

        @Override
        public ABDInstance deserialize(ByteBuf in) throws IOException {

            long instanceId = in.readLong();
            long key = in.readLong();
            long val = in.readLong();

            long left = in.readLong();
            long right = in.readLong();

            long opSeq = in.readLong();

            return new ABDInstance(instanceId, key, val, left, right, opSeq);
        }
    };
}
