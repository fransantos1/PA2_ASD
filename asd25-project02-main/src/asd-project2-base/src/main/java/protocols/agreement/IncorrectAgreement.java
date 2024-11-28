package protocols.agreement;

import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.prepareMessage;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;

import java.io.IOException;
import java.util.*;

/**
on the operation class:
 optype after 1 is statemachine/paxos exclusive
 optype = 2 is add replica
 optype = 3  is remove replica
 optype = 4 is changeLeader



 */




public class IncorrectAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(IncorrectAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "MultiPaxos";


    private Host currentLeader;
    private int currentBallot;


    private Host myself;
    private int joinedInstance;

    private HashMap<BroadcastMessage, Integer> incomingProposals ;
    private ArrayList<BroadcastMessage> decidedProposals;


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

        decidedProposals = new ArrayList<>();
        incomingProposals = new HashMap<>();

        logger.info("Channel {} created, I am {}", cId, myself);

        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */

        try {
            registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);


            registerMessageSerializer(cId, prepareMessage.MSG_ID, prepareMessage.serializer);
            registerMessageHandler(cId, prepareMessage.MSG_ID, this::uponPrepare, this::uponMsgFail);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    // Ballot should be something directly associated with the node on the membership, if this node is node n on the membership, his next propose the Ballot number should be something like current Ballot + n so everyone has a diferent one
    //leader election
    // prepare( instance , Ballot number)
    // prepare_ok(instance, ballot number)
    // and this is becomes the leader?


    // propose values

    // the leader sends accept(instance, ballot, opID)
    // each replica verifies, if the instance, and ballot number are correct and send an accept_ok( instance, ballot, opid)

    // when a replica recieves the same accept_ok from a majority of replicas, they send a decided to the app




    //  filter if is a propose or a propose_Ok
    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            switch (msg.getType()){
                case BroadcastMessage.ACCEPT:
                    if(msg.getInstance() < currentInstance && currentInstance < membership.size()){
                        //send error message to Sender
                    }

                    // (instance, Ballot, operation)
                     //
                     //
                     //

                    break;
                case BroadcastMessage.ACCEPT_OK:

                    // (instance, Ballot, operation)
                    if(decidedProposals.contains(msg))
                        return;

                    int n_ofTurns = incomingProposals.getOrDefault(msg, 0);
                    n_ofTurns++;
                    incomingProposals.put(msg, n_ofTurns);

                    if(n_ofTurns < membership.size()/2 + 1)
                        return;

                    decidedProposals.add(msg);
                    triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));

                    break;
                case BroadcastMessage.PREPARE:
                    // (instance, Ballot)

                    break;
            }


        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());



        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);


    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received " + request);
        if(request.getInstance() == -1) {
            sendPrepare();
        }
        if(myself.equals(currentLeader)){
            return;
        }
        BroadcastMessage msg = new BroadcastMessage(request.getInstance(), request.getOpId(), request.getOperation(), BroadcastMessage.ACCEPT);
        logger.debug("Sending to: " + currentLeader.getAddress());
        membership.forEach(h -> sendMessage(msg, h));




    }
    /*---------------------- Membership Managment -------------------------- */
    //add replica, ask for propuse


    //remove replica, ask to propuse but it needs to be processed right away

    //if the replica being removed is the leader
    //send a prepare


    /*---------------------- Propose value -------------------------- */

    // propose on a instace

    //send propose_ok
    //in here I need to verify if the ballot is correct
    //and if itsnt notify the node trying to propuse

    //notify state machine of decided

    /*---------------------- Change leader -------------------------- */

    //


    private void sendPrepare() {
        int ballot = currentBallot + membership.indexOf(myself);
        prepareMessage msg = new prepareMessage(myself,ballot, currentInstance++);
        logger.info("Sending prepare message to {} for instance {} with ballot {}", myself , msg.getInstance(), msg.getBallot());
        membership.forEach(h -> sendMessage(msg, h));
    }


    private void uponPrepare(prepareMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received prepare message from {} for instance {} with ballot {}", host, msg.getInstance(), msg.getBallot());
        if(msg.getBallot() > currentBallot){
            //change the leader
            currentBallot = msg.getBallot();
            currentLeader = host;


            //send prepareOk
            return;
        } else {

        }

        //send message to the mf trying to be leader and tell him to go take a hike
    }




    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);

        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.

        membership.add(request.getReplica());
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);

        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.

        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
