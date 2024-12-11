package protocols.statemachine;

import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.app.HashApp;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.app.utils.Operation;
import protocols.statemachine.Utils.MembershipOp;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.JoinReplyMsg;
import protocols.statemachine.messages.forwardRequestMessage;
import protocols.statemachine.timer.BufferedRequestTimer;
import protocols.statemachine.timer.HeartBeatTimer;
import protocols.statemachine.timer.JoiningTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.IncorrectAgreement;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */




public class StateMachine extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 411;
    
    private final Host self;     //My own address/port
    private Host leader = null;

    private final int channelId; //Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance;

    // to mantain the membership
    //max timeout 10s
    private  final int timeOutTime = 10000;//10s
    private final int timeOutTries = 5;

    private HashMap<Host, Long> lastCommunication;
    private HashMap<Host, Integer> timeOutHosts;

    private HashMap<Long, Long> stateMap;

    private Queue<OrderRequest> requestsWatingTurn;
    private boolean isRoundActive = false;

    private HashMap<UUID, OrderRequest> awaitingRequests;
    private HashMap<Integer, DecidedNotification> decidedRequests; // Integer is the instance

    private Dictionary<Short,OrderRequest> bufferedReq;

    List<Host> initialMembership;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException, InvalidParameterException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;
        bufferedReq = new Hashtable<>();
        stateMap = new HashMap<>();


        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));


        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);


        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);
        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*--------------------- Register Message Handlers ----------------------------- */

        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoinMessage, this::uponMsgFail);


        registerMessageSerializer(channelId, forwardRequestMessage.MSG_ID, forwardRequestMessage.serializer);
        registerMessageHandler(channelId, forwardRequestMessage.MSG_ID, this::uponRequestForward, this::uponMsgFail);

        registerMessageSerializer(channelId,JoinReplyMsg.MSG_ID, JoinReplyMsg.serializer);
        registerMessageHandler(channelId, JoinReplyMsg.MSG_ID, this::uponJoinReplyMessage, this::uponMsgFail);

        //registerMessageHandler(channelId, JoinReplyMsg.MSG_ID, JoinReplyMsg.serializer);
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(BufferedRequestTimer.TIMER_ID, this::processNextBufferedRequest);

    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));
        String host = props.getProperty("initial_membership");

        String[] hosts = host.split(",");
        initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }


        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping (initiating)
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            requestChangeLeader();

            triggerNotification(new JoinedNotification(membership, 0));
            setupPeriodicTimer(new BufferedRequestTimer(),1000, 1000);
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            requestToJoin();
        }

    }

    private void processBufferedRequests() {
             logger.info("Processing buffered requests now that state is ACTIVE");
             Enumeration<OrderRequest> requests = bufferedReq.elements();
             while(requests.hasMoreElements()) {
                 OrderRequest req = requests.nextElement();
                 sendRequest(req, IncorrectAgreement.PROTOCOL_ID);
             }
    }



    private void processNextBufferedRequest(BufferedRequestTimer timer, long timerID) {
        if(bufferedReq.elements().hasMoreElements()) {
            OrderRequest req = bufferedReq.elements().nextElement();
            sendRequest(req, StateMachine.PROTOCOL_ID);
        }
    }


    /*--------------------------------- Order Requests ---------------------------------------- */
    private void uponRequestForward(forwardRequestMessage request, Host from, short sourceProto, int channelId) {
        sendRequest(request.getReq(), StateMachine.PROTOCOL_ID);
    }

    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            bufferedReq.put(request.getId(), request);
        } else if (state == State.ACTIVE) {
            awaitingRequests.put(request.getOpId(), request);
            if(!leader.equals(self)){
                openConnection(leader);
                logger.debug("Sending request To Leader : " + leader.getAddress());
                sendMessage( new forwardRequestMessage(request), leader);
                return;
            }
            logger.debug("Sending request To MultiPaxos");
            requestsWatingTurn.add(request);
            sendNextPropose();
        }
    }

    private void sendNextPropose(){
        if(isRoundActive)
            return;
        isRoundActive = true;
        OrderRequest req = requestsWatingTurn.poll();
        if(req == null)
            return;
        logger.debug("Sending next propose: " + req.getOpType());
        sendRequest(new ProposeRequest(nextInstance++, req.getOpId(), req.getOperation()),
                IncorrectAgreement.PROTOCOL_ID);
    }

    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        if(notification.getInstance() > nextInstance+1){
            logger.debug("Im behind, asking for a state transfer");
            // ask for state transfer or smth
            return;
        }

        nextInstance = notification.getInstance();
        if(notification.getOpType() == 0) {
            triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));
        }

        if (notification.getOpType() == DecidedNotification.MEMBERSHIP_OP) {
            MembershipOp op;
            try {
                op = MembershipOp.fromByteArray(notification.getOperation());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            switch (op.getType()) {
                case MembershipOp.ADD:
                    addReplica(notification);
                    break;
                case MembershipOp.REMOVE:
                    removeReplica(notification);
                    break;
                case MembershipOp.CHANGE_LEADER:
                    leader = op.getHost();
                    break;
            }
        }
        decidedRequests.put(notification.getInstance(), notification);

        if(leader.equals(self)){
            isRoundActive = false;
            sendNextPropose();
        }
    }

    /*------- Change Leader ------- */

    private void requestChangeLeader(){
        // when I send a request to become a leader I need to be aware that i'm gonna recieve requests before I know i'm the leader
        sendRequest(new ProposeRequest(-1, null , null), IncorrectAgreement.PROTOCOL_ID);
    }


    /*---------------------------------Membership ---------------------------------------- */

    int requestToJoinIndex = 0;
    private void requestToJoin() {
        if(requestToJoinIndex >= membership.size()) {requestToJoinIndex = 0;}
        Host host = initialMembership.get(requestToJoinIndex);
        openConnection(host);
        sendMessage(new JoinMessage(self), host);
        requestToJoinIndex++;
    }

    MembershipOp awaitingCurrent_state = null;
    private void addReplica(DecidedNotification notification) {
        logger.info("Adding new replica based on decided notification: {}", notification);
        MembershipOp op;
        try {
            op = MembershipOp.fromByteArray(notification.getOperation());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (awaitingRequests.containsKey(notification.getOpId())) {
            awaitingRequests.remove(notification.getOpId());
            sendRequest(new CurrentStateRequest(nextInstance), HashApp.PROTO_ID);
            awaitingCurrent_state = op;
        }

        AddReplicaRequest req = new AddReplicaRequest(notification.getInstance(), op.getHost());
        sendRequest(req, IncorrectAgreement.PROTOCOL_ID);

    }
    private void uponCurrentStateReply(CurrentStateReply reply,short sourceProto) {
        byte[] currentState = reply.getState();
        JoinReplyMsg response = new JoinReplyMsg(membership, currentState, nextInstance);
        openConnection(awaitingCurrent_state.getHost());
        sendMessage(response, awaitingCurrent_state.getHost());
    }





    private void removeReplica(DecidedNotification notification) {
        logger.info("Removing replica based on decided notification: {}", notification);

    }

    private void uponJoinMessage(JoinMessage request, Host from, short sourceProto, int channelId) {
        logger.info("Received joinMessage from {}",from);
        //Generate a operationID
        UUID opUUID = UUID.randomUUID();
        MembershipOp op = new MembershipOp(MembershipOp.ADD, request.getReplica());
        try {
            sendRequest(new OrderRequest(opUUID, op.toByteArray(), MembershipOp.ID), IncorrectAgreement.PROTOCOL_ID);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void uponJoinReplyMessage(JoinReplyMsg reply, Host from,short sourceProto, int channelId) {
        // full state
        // instance
        // Leader
        // membership
        logger.info("Received JoinReply from {} with membership: {}", from, reply.getCurrentMembership());
        this.state = State.ACTIVE;
        this.nextInstance = reply.getInstance();

        membership.forEach(this::openConnection);
        // send snapshot to HashApp
        sendRequest(new InstallStateRequest(reply.getStateSnapshot()), HashApp.PROTO_ID);
        triggerNotification(new JoinedNotification(membership, reply.getInstance()));
        setupPeriodicTimer(new BufferedRequestTimer(),1000, 1000);
    }

    private void heartBeat(JoiningTimer timer, long timerId) {

        for(Map.Entry<Host, Long> entry : lastCommunication.entrySet()) {
            Host key = entry.getKey();
            Long value = entry.getValue();
            Long currentTime = System.currentTimeMillis();

            if(currentTime-value > timeOutTime){
                //! remove from membership
                timeOutHosts.put(key, 0);
            }else{
                timeOutHosts.remove(key);
            }
        }
        for(Map.Entry<Host, Integer> entry : timeOutHosts.entrySet()) {
            Host key = entry.getKey();
            Integer tries = entry.getValue();
            if(tries > timeOutTries){
                timeOutHosts.remove(key);
                lastCommunication.remove(key);

                //! remove from membership
                continue;
            }

            entry.setValue(tries+1);
            //send message
        }


    }



    /*---------------------------------Receiving Messages ---------------------------------------- */


    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if(membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }


}
