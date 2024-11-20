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
    private final long key1;
    private final long key2;
    private final long newTag1;
    private final long newTag2;
    private final long newValue1;
    private final long newValue2;

    public WriteTagMsg(long opSeq, long key1, long key2, long newTag1, long newTag2, long newValue1, long newValue2){
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key1 = key1;
        this.key2 = key2;
        this.newTag1 = newTag1;
        this.newTag2 = newTag2;
        this.newValue1 = newValue1;
        this.newValue2 = newValue2;
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

    public long getNewTag1() {
        return newTag1;
    }

    public long getNewTag2() {
        return newTag2;
    }

    public long getNewValue1() {
        return newValue1;
    }

    public long getNewValue2() {
        return newValue2;
    }

    public static ISerializer<WriteTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);

            out.writeLong(sampleMessage.key1);
            out.writeLong(sampleMessage.key2);

            out.writeLong(sampleMessage.newTag1);
            out.writeLong(sampleMessage.newTag2);

            out.writeLong(sampleMessage.newValue1);
            out.writeLong(sampleMessage.newValue2);
        }

        @Override
        public WriteTagMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();

            long key1 = in.readLong();
            long key2 = in.readLong();

            long newTag1 = in.readLong();
            long newTag2 = in.readLong();

            long newValue1 = in.readLong();
            long newValue2 = in.readLong();

            return new WriteTagMsg(opSeq, key1, key2, newTag1, newTag2, newValue1, newValue2);
        }
    };
}
