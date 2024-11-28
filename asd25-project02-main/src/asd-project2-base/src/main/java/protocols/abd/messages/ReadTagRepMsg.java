package protocols.abd.messages;

import java.io.IOException;
import java.sql.Timestamp;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagRepMsg extends ProtoMessage {

    public static final short MSG_ID = 202;

    private final long opSeq;
    private final long tag;
    private final long key;

    public ReadTagRepMsg(long opSeq, long tag, long key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tag = tag;
        this.key = key;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getTag() {
        return tag;
    }

    public long getKey() {
        return key;
    }

    public static ISerializer<ReadTagRepMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagRepMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.tag);
            out.writeLong(sampleMessage.key);
        }

        @Override
        public ReadTagRepMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long tag = in.readLong();
            long key = in.readLong();
            return new ReadTagRepMsg(opSeq, tag, key);
        }
    };

}
