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
    private final String key;
    private final int newTag;
    private final List<Operation> pending;

    public WriteTagMsg(int opSeq, String key, int newTag, List<Operation> pending){
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.newTag = newTag;
        this.pending = pending;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public List<Operation> getPending() {
        return pending;
    }

    public String getKey() {
        return key;
    }

    public int getNewTag() {
        return newTag;
    }

    public static ISerializer<WriteTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.opSeq);

            out.writeInt(sampleMessage.key.length());
            out.writeBytes(sampleMessage.key.getBytes());

            out.writeInt(sampleMessage.newTag);

            out.writeInt(sampleMessage.key.length());
            out.writeBytes(sampleMessage.key.getBytes());
        }

        @Override
        public WriteTagMsg deserialize(ByteBuf in) throws IOException {

            return new WriteTagMsg(opSeq, aux);
        }
    };
}
