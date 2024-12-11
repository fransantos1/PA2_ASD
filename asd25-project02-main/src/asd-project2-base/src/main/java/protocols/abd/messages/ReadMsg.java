package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReadMsg extends ProtoMessage {

    public static final short MSG_ID = 105;

    private final long opSeq;
    private final long key;


    public ReadMsg(long opSeq, long key) {
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

    public static ISerializer<ReadMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.key);
        }

        @Override
        public ReadMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long key = in.readLong();
            return new ReadMsg(opSeq, key);
        }
    };
}
