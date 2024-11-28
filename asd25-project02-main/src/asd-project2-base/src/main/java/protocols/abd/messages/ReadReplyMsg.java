package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReadReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 206;

    private final long opSeq;
    private final long key;
    private final long tag;
    private final long val;


    public ReadReplyMsg(long opSeq, long key, long tag, long val) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.tag = tag;
        this.val = val;
    }

    public long getOpSeq(){
        return opSeq;
    }

    public long getKey(){
        return key;
    }

    public long getTag(){
        return tag;
    }

    public long getVal(){
        return val;
    }

    public static ISerializer<ReadReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadReplyMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key);
            out.writeLong(sampleMessage.tag);
            out.writeLong(sampleMessage.val);
        }

        @Override
        public ReadReplyMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key = in.readLong();
            long tag = in.readLong();
            long val = in.readLong();
            return new ReadReplyMsg(opSeq, key, tag, val);
        }
    };

}
