package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;


public class LeaveMsg extends ProtoMessage {

    public static final short MSG_ID = 104;

    private final Host leaving;

    public LeaveMsg(Host leaving) {
        super(MSG_ID);
        this.leaving = leaving;
    }

    public Host getLeaving() {
        return leaving;
    }

    public static ISerializer<LeaveMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(LeaveMsg leaveMsg, ByteBuf out) throws IOException {
            Host.serializer.serialize(leaveMsg.leaving, out);
        }

        @Override
        public LeaveMsg deserialize(ByteBuf in) throws IOException {
            return new LeaveMsg(Host.serializer.deserialize(in));
        }
    };
}
