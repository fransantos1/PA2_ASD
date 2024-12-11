package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.ABDInstance;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JoinReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 103;

    private final Map<Long, ABDInstance> val;

    public JoinReplyMsg(Map<Long, ABDInstance> val) {
        super(MSG_ID);
        this.val = val;
    }

    public Map<Long, ABDInstance> getVal() {
        return val;
    }


    public static ISerializer<JoinReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReplyMsg sampleMessage, ByteBuf out) throws IOException {

            out.writeInt(sampleMessage.val.size());
            for (Map.Entry<Long, ABDInstance> entry : sampleMessage.val.entrySet()) {
                out.writeLong(entry.getKey());
                ABDInstance.serializer.serialize(entry.getValue(), out);
            }

            out.writeInt(sampleMessage.val.size());

        }

        @Override
        public JoinReplyMsg deserialize(ByteBuf in) throws IOException {

            Map<Long, ABDInstance> val = new HashMap<>();
            int size2 = in.readInt();
            for (int i = 0; i < size2; i++) {
                val.put(in.readLong(), ABDInstance.serializer.deserialize(in));
            }

            return new JoinReplyMsg(val);
        }
    };




}
