package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AckMsg extends ProtoMessage {

    public static final short MSG_ID = 204;

    private final long opSeq;
    private final long key1;
    private final long key2;

    public AckMsg(long opSeq, long key1, long key2) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key1 = key1;
        this.key2 = key2;
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

    public static ISerializer<AckMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(AckMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key1);
            out.writeLong(sampleMessage.key2);
        }

        @Override
        public AckMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key1 = in.readLong();
            long key2 = in.readLong();
            return new AckMsg(opSeq, key1, key2);
        }
    };

}
