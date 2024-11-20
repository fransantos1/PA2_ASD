package protocols.abd.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagMsg extends ProtoMessage{

    public static final short MSG_ID = 201;

    private final long opSeq;
    private final long key1;
    private final long key2;

    public ReadTagMsg(long opSeq, long key1, long key2) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key1 = key1;
        this.key2 = key2;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getKey1() {
        return key1;
    }

    public long getKey2() {
        return key2;
    }

    public static ISerializer<ReadTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key1);
            out.writeLong(sampleMessage.key2);
        }

        @Override
        public ReadTagMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key1 = in.readLong();
            long key2 = in.readLong();
            return new ReadTagMsg(opSeq, key1, key2);
        }
    };



}
