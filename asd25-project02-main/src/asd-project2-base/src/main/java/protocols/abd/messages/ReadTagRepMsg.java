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
    private final long tag1;
    private final long tag2;
    private final long key1;
    private final long key2;

    public ReadTagRepMsg(long opSeq, long tag1, long tag2, long key1, long key2) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.key1 = key1;
        this.key2 = key2;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getTag1() {
        return tag1;
    }

    public long getTag2() {
        return tag2;
    }

    public long getKey1() {
        return key1;
    }

    public long getKey2() {
        return key2;
    }

    public static ISerializer<ReadTagRepMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagRepMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.tag1);
            out.writeLong(sampleMessage.tag2);
            out.writeLong(sampleMessage.key1);
            out.writeLong(sampleMessage.key2);
        }

        @Override
        public ReadTagRepMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long tag1 = in.readLong();
            long tag2 = in.readLong();
            long key1 = in.readLong();
            long key2 = in.readLong();
            return new ReadTagRepMsg(opSeq, tag1, tag2, key1, key2);
        }
    };

}
