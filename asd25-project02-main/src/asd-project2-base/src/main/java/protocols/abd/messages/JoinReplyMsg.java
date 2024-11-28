package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JoinReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 207;

    private final Map<Long, Long> val;
    private final Map<Long, Long> tag;

    public JoinReplyMsg(Map<Long, Long> val, Map<Long, Long> tag) {
        super(MSG_ID);
        this.val = val;
        this.tag = tag;
    }

    public Map<Long, Long> getVal() {
        return val;
    }

    public Map<Long, Long> getTag() {
        return tag;
    }

    public static ISerializer<JoinReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReplyMsg sampleMessage, ByteBuf out) throws IOException {

            out.writeInt(sampleMessage.tag.size());
            for (Map.Entry<Long, Long> entry : sampleMessage.tag.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }

            out.writeInt(sampleMessage.val.size());
            for (Map.Entry<Long, Long> entry : sampleMessage.val.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }
        }

        @Override
        public JoinReplyMsg deserialize(ByteBuf in) throws IOException {
            Map<Long, Long> tag = new HashMap<>();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                tag.put(in.readLong(), in.readLong());
            }

            Map<Long, Long> val = new HashMap<>();
            int size2 = in.readInt();
            for (int i = 0; i < size2; i++) {
                val.put(in.readLong(), in.readLong());
            }

            return new JoinReplyMsg(val, tag);
        }
    };




}
