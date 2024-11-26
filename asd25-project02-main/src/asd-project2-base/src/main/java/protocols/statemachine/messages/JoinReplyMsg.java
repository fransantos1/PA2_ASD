package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.messages.ReadTagMsg;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class JoinReplyMsg extends ProtoMessage {

    public static final short MSG_ID = 401;
    private final List<Host> currentMembership;
    private final List<Integer> stateSnapshot;

    public JoinReplyMsg(List<Host> currentMembership, List<Integer> stateSnapshot) {
        super(MSG_ID);
        this.currentMembership = currentMembership;
        this.stateSnapshot = stateSnapshot;
    }

    public List<Host> getCurrentMembership() {
        return currentMembership;
    }

    public List<Integer> getStateSnapshot() {
        return stateSnapshot;
    }

    public static ISerializer<JoinReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReplyMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.currentMembership.size());
            for (Host h : sampleMessage.currentMembership) {
                Host.serializer.serialize(h, out);
            }
            out.writeInt(sampleMessage.stateSnapshot.size());
            for (Integer i : sampleMessage.stateSnapshot) {
                out.writeInt(i);
            }
        }

        @Override
        public JoinReplyMsg deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            List<Host> membership = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                membership.add(Host.serializer.deserialize(in));
            }
            size = in.readInt();
            List<Integer> stateSnapshot = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                stateSnapshot.add(in.readInt());
            }
            return new JoinReplyMsg(membership, stateSnapshot);
        }
    };
}
