package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class responseMessage extends ProtoMessage {

    public final static short MSG_ID = 104;
    private final boolean isOK;


    public responseMessage(boolean isOK) {
        super(MSG_ID);
        this.isOK = isOK;
    }

    public boolean isOK(){
        return isOK;
    }

    public static ISerializer<responseMessage> serializer = new ISerializer<responseMessage>() {
        @Override
        public void serialize(responseMessage msg, ByteBuf out) {
            out.writeBoolean(msg.isOK);
        }

        @Override
        public responseMessage deserialize(ByteBuf in) {
            boolean isOK = in.readBoolean();
            return new responseMessage(isOK);
        }
    };

}
