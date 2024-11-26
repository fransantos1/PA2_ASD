package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinMessage  extends ProtoMessage {

    public static final short MSG_ID = 402;
    private final Host replica;

    public JoinMessage( Host replica) {
        super(MSG_ID);
        this.replica = replica;
    }
    public Host getReplica() {return this.replica;}

    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage sampleMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(sampleMessage.replica, out);
        }
        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            Host replica = Host.serializer.deserialize(in);
            return new JoinMessage(replica);
        }
    };
}
