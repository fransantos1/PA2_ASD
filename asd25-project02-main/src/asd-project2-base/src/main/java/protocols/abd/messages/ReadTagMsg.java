package protocols.abd.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagMsg extends ProtoMessage{

    public static final short MSG_ID = 201;

    private final int opSeq;
    private final long key;

    public ReadTagMsg(int opSeq, long key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public long getKey() {
        return key;
    }

    public static ISerializer<ReadTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key.);
        }

        @Override
        public ReadTagMsg deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            long key = in.readLong();
            return new ReadTagMsg(opSeq, key);
        }
    };



}
