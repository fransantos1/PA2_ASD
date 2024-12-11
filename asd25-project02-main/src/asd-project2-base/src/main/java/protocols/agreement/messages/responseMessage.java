package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class responseMessage extends ProtoMessage {

    public final static int PREPARE_RESPONSE = 0;
    public final static int ACCEPT_RESPONSE = 1;


    public final static short MSG_ID = 204;
    private final boolean isOK;
    private final int type;


    public responseMessage(boolean isOK, int type) {
        super(MSG_ID);
        this.isOK = isOK;
        this.type = type;
    }

    public boolean isOK(){
        return isOK;
    }

    public int getType(){return type;}


    public static ISerializer<responseMessage> serializer = new ISerializer<responseMessage>() {
        @Override
        public void serialize(responseMessage msg, ByteBuf out) {
            out.writeBoolean(msg.isOK);
            out.writeInt(msg.type);
        }

        @Override
        public responseMessage deserialize(ByteBuf in) {
            boolean isOK = in.readBoolean();
            int type = in.readInt();
            return new responseMessage(isOK, type);
        }
    };

}
