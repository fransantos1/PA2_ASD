package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AckMsg extends ProtoMessage {

    public static final short MSG_ID = 101;

    private final long opSeq;
    private final long key;

    public AckMsg(long opSeq, long key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public long getOpSeq(){
        return opSeq;
    }

    public long getKey(){
        return key;
    }

    public static ISerializer<AckMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(AckMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key);
        }

        @Override
        public AckMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key = in.readLong();
            return new AckMsg(opSeq, key);
        }
    };

}
