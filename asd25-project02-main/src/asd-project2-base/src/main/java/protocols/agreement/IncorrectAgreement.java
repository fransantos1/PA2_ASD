package protocols.agreement;

import protocols.agreement.messages.broadcastMessage;
import protocols.agreement.messages.responseMessage;
import protocols.agreement.messages.prepareMessage;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.app.messages.ResponseMessage;
import protocols.statemachine.Utils.MembershipOp;
import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;

import javax.swing.plaf.nimbus.State;
import java.io.IOException;
import java.util.*;

/*
    on the operation class:
 optype after 1 is statemachine/paxos exclusive
 optype = 2 is add replica
 optype = 3  is remove replica
 optype = 4 is changeLeader
 */
public class IncorrectAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(IncorrectAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 210;
    public final static String PROTOCOL_NAME = "MultiPaxos";


    private Host currentLeader;
    private int currentBallot = 0;


    private Host myself;
    private int joinedInstance;

    private HashMap<UUID, Integer> incomingProposals ;
    private ArrayList<UUID> decidedProposals;


    private List<Host> membership;
    private int currentInstance;

    public IncorrectAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;

        currentLeader = null;

        /*--------------------- Register Timer Handlers ----------------------------- */

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);

        /*--------------------- Register Message Handlers ----------------------------- */



        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        membership = new LinkedList<>();
        decidedProposals = new ArrayList<>();
        incomingProposals = new HashMap<>();

        logger.info("Channel {} created, I am {}", cId, myself);

        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, broadcastMessage.MSG_ID, broadcastMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */

        try {
            registerMessageHandler(cId, broadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);

            registerMessageSerializer(cId, responseMessage.MSG_ID, responseMessage.serializer);
            registerMessageHandler(cId, responseMessage.MSG_ID, this::uponResponse, this::uponMsgFail);


        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponBroadcastMessage(broadcastMessage msg, Host host, short sourceProto, int channelId) {

        if(joinedInstance >= 0 ){

            switch (msg.getType()){

                case broadcastMessage.ACCEPT:

                    if(msg.getInstance() !=  currentInstance+1
                            && msg.getBallot() != currentBallot){
                        responseMessage err = new responseMessage(false, responseMessage.ACCEPT_RESPONSE);
                        openConnection(host);
                        sendMessage(err, host);
                        return;
                    }

                    broadcastMessage accept_ok = new broadcastMessage(msg.getInstance(), msg.getOpId(), msg.getOp(),
                            broadcastMessage.ACCEPT_OK, currentBallot, msg.getOpType());
                    broadCast(accept_ok);
                    break;

                case broadcastMessage.ACCEPT_OK:
                    if(decidedProposals.contains(msg.getOpId()))
                        return;
                    int nOfAcceptOk = incomingProposals.getOrDefault(msg.getOpId(), 0);
                    int new_nOfAcceptOk = nOfAcceptOk + 1;
                    incomingProposals.put(msg.getOpId(), new_nOfAcceptOk);
                    if(membership.size() != 2 && new_nOfAcceptOk < membership.size()/2 + 1) {
                        return;
                    }
                    incomingProposals.remove(msg.getOpId());
                    decidedProposals.add(msg.getOpId());
                    triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp(), msg.getOpType()));
                    break;

                case broadcastMessage.PREPARE:
                    if(msg.getBallot() < currentBallot){
                        responseMessage err = new responseMessage(false, responseMessage.PREPARE_RESPONSE);
                        openConnection(host);
                        sendMessage(err, host);
                        return;
                    }
                    currentBallot = msg.getBallot();
                    currentLeader = host;
                    responseMessage prepare_ok = new responseMessage(true, responseMessage.PREPARE_RESPONSE);
                    sendMessage(prepare_ok, host);
                    triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp(), msg.getOpType()));
                    break;
            }
        } else {

        }
    }

    private void uponResponse(responseMessage msg, Host host, short sourceProto, int channelId) {
        switch (msg.getType()){

            // the accept_response is only for errors because, the accept_OK is a broadcast
            case responseMessage.ACCEPT_RESPONSE:
                if(!msg.isOK()){
                    //there is something wrong with the Propose I made

                }
                break;
            case responseMessage.PREPARE_RESPONSE:
                if(!msg.isOK()){
                    //they dont accept me has the new leader
                }
                // when I recieve the majority, become the leader

                break;
        }
    }



    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        //logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {

        if(request.getInstance() == -1) {
            sendPrepare();
        }
        if(myself.equals(currentLeader)){
            return;
        }
        //logger.info("Recieved propose for instace: {}",request.getInstance());
        broadcastMessage msg = new broadcastMessage(request.getInstance(), request.getOpId(), request.getOperation(), broadcastMessage.ACCEPT, currentBallot, request.getOpType());
        //logger.debug("Sending propose: " + request.toString());
        broadCast(msg);
    }

    private void broadCast( broadcastMessage msg) {
        membership.forEach(h -> sendMessage(msg, h));
    }

    private void sendPrepare() {
        int ballot = currentBallot + membership.indexOf(myself);
        prepareMessage msg = new prepareMessage(myself,ballot, currentInstance++);
        logger.info("Sending prepare message to {} for instance {} with ballot {}", myself , msg.getInstance(), msg.getBallot());
        broadcastMessage new_msg = new broadcastMessage(currentInstance++, null, null, broadcastMessage.PREPARE, ballot, broadcastMessage.MEMBERSHIP_OP);
        broadCast(new_msg);

        //membership.forEach(h -> sendMessage(msg, h));
    }


    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);

        membership.add(request.getReplica());
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
