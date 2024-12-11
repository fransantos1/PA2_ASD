package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReadReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 106;

    private final long opSeq;
    private final long key;
    private final long tagl;
    private final long tagr;
    private final long val;


    public ReadReplyMsg(long opSeq, long key, long tagl, long tagr, long val) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.tagl = tagl;
        this.tagr = tagr;
        this.val = val;
    }

    public long getOpSeq(){
        return opSeq;
    }

    public long getKey(){
        return key;
    }

    public long getTagl(){
        return tagl;
    }

    public long getTagr(){
        return tagr;
    }

    public long getVal(){
        return val;
    }

    public static ISerializer<ReadReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadReplyMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key);
            out.writeLong(sampleMessage.tagl);
            out.writeLong(sampleMessage.tagr);
            out.writeLong(sampleMessage.val);
        }

        @Override
        public ReadReplyMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key = in.readLong();
            long tagl = in.readLong();
            long tagr = in.readLong();
            long val = in.readLong();
            return new ReadReplyMsg(opSeq, key, tagl, tagr, val);
        }
    };

}
