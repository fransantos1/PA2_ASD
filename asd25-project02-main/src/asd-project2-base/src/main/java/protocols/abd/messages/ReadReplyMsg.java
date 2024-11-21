package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReadReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 206;

    private final long opSeq;
    private final long key1;
    private final long key2;
    private final long tag1;
    private final long tag2;
    private final long val1;
    private final long val2;


    public ReadReplyMsg(long opSeq, long key1, long key2, long tag1, long tag2, long val1, long val2) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key1 = key1;
        this.key2 = key2;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.val1 = val1;
        this.val2 = val2;
    }

    public long getOpSeq(){
        return opSeq;
    }

    public long getKey1(){
        return key1;
    }

    public long getKey2(){
        return key2;
    }

    public long getTag1(){
        return tag1;
    }

    public long getTag2(){
        return tag2;
    }

    public long getVal1(){
        return val1;
    }

    public long getVal2(){
        return val2;
    }

    public static ISerializer<ReadReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadReplyMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key1);
            out.writeLong(sampleMessage.key2);
            out.writeLong(sampleMessage.tag1);
            out.writeLong(sampleMessage.tag2);
            out.writeLong(sampleMessage.val1);
            out.writeLong(sampleMessage.val2);
        }

        @Override
        public ReadReplyMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key1 = in.readLong();
            long key2 = in.readLong();
            long tag1 = in.readLong();
            long tag2 = in.readLong();
            long val1 = in.readLong();
            long val2 = in.readLong();
            return new ReadReplyMsg(opSeq, key1, key2, tag1, tag2, val1, val2);
        }
    };

}
