package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.List;

public class WriteTagMsg extends ProtoMessage {

    public static final short MSG_ID = 203;

    private final long opSeq;
    private final long key;
    private final long newTag;
    private final long newValue;

    public WriteTagMsg(long opSeq, long key, long newTag, long newValue){
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.newTag = newTag;
        this.newValue = newValue;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getKey() {
        return key;
    }

    public long getNewTag() {
        return newTag;
    }

    public long getNewValue() {
        return newValue;
    }

    public static ISerializer<WriteTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);

            out.writeLong(sampleMessage.key);

            out.writeLong(sampleMessage.newTag);

            out.writeLong(sampleMessage.newValue);
        }

        @Override
        public WriteTagMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();

            long key = in.readLong();

            long newTag = in.readLong();

            long newValue = in.readLong();

            return new WriteTagMsg(opSeq, key, newTag, newValue);
        }
    };
}
