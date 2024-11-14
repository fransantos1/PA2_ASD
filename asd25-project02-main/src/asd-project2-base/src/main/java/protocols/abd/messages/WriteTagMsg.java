package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.List;

public class WriteTagMsg extends ProtoMessage {

    public static final short MSG_ID = 203;

    private final int opSeq;
    private final long key;
    private final int newTag;
    private final long pending;

    public WriteTagMsg(int opSeq, long key, int newTag, long pending){
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.newTag = newTag;
        this.pending = pending;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public long getPending() {
        return pending;
    }

    public long getKey() {
        return key;
    }

    public int getNewTag() {
        return newTag;
    }

    public static ISerializer<WriteTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.opSeq);

            out.writeLong(sampleMessage.key);

            out.writeInt(sampleMessage.newTag);

            out.writeLong(sampleMessage.pending);
        }

        @Override
        public WriteTagMsg deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();

            long key = in.readLong();

            int newTag = in.readInt();

            long pending = in.readLong();

            return new WriteTagMsg(opSeq, key, newTag, pending);
        }
    };
}
