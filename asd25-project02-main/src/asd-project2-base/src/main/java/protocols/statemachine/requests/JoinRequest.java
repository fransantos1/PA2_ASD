package protocols.statemachine.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinRequest extends ProtoMessage {

        public static final short REQUEST_ID = 301;

        private final Host requester;

        public JoinRequest(Host requester) {
            super(REQUEST_ID);
            this.requester = requester;
        }

        public Host getRequester() {
            return requester;
        }

        @Override
        public String toString() {
            return "JoinRequest{" +
                    "requester=" + requester +
                    '}';
        }
}
