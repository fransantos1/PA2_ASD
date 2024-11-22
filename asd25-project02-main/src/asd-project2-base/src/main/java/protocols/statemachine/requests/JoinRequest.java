package protocols.statemachine.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class JoinRequest extends ProtoMessage {

        public static final short REQUEST_ID = 301;

        private final Host requester;
        private final UUID opId;

        public JoinRequest(UUID opId, Host requester) {
            super(REQUEST_ID);
            this.requester = requester;
            this.opId = opId;
        }

        public Host getRequester() {
            return requester;
        }
        public UUID getOpId() {
        return opId;
    }


        @Override
        public String toString() {
            return "JoinRequest{" +
                    "requester=" + requester +
                    '}';
        }
}
