package protocols.statemachine;

import protocols.agreement.notifications.JoinedNotification;
import protocols.statemachine.messages.JoinReplyMsg;
import protocols.statemachine.requests.JoinRequest;
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
import java.time.Duration;
import java.time.Instant;
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
    public static final short PROTOCOL_ID = 200;
    
    private final Host self;     //My own address/port
    private Host leader = null;

    private final int channelId; //Id of the created channel
        
    private  final int timeOutTime = 10000;//10s
    private final int timeOutTries = 5;
    
    private State state;
    private List<Host> membership;
    private int nextInstance;


    // to mantain the membership
    //max timeout 10s
    private HashMap<Host, Long> lastCommunication;
    private HashMap<Host, Integer> timeOutHosts;



    private HashMap<Long, Long> stateMap;
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

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*--------------------- Register Message Handlers ----------------------------- */
       //registerMessageHandler(channelId, JoinReplyMsg.MSG_ID, JoinReplyMsg.serializer);


        /*--------------------- Register Timer Handlers ----------------------------- */

        registerTimerHandler(JoiningTimer.TIMER_ID, this::requestToJoin);
        registerTimerHandler(JoiningTimer.TIMER_ID, this::requestToJoin);

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
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));
            processBufferedRequests();
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            //You have to do something to join the system and know which instance you joined

            // Start timer trigger to requestToJoin
            setupPeriodicTimer(new JoiningTimer(), 1000, 5000);
            // (and copy the state of that instance)
        }

    }



    private void processBufferedRequests() {
        logger.info("Processing buffered requests now that state is ACTIVE");
        Enumeration<OrderRequest> requests = bufferedReq.elements();
        while(requests.hasMoreElements()) {
            OrderRequest req = requests.nextElement();

            //! CHANGE THIS TO SEND TO LEADER
            sendRequest(new ProposeRequest(nextInstance++, req.getOpId(), req.getOperation()),
                    IncorrectAgreement.PROTOCOL_ID);
        }
    }



    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            bufferedReq.put(request.getId(), request);
        } else if (state == State.ACTIVE) {

            //Also do something starter, we don't want an infinite number of instances active
        	//Maybe you should modify what is it that you are proposing so that you remember that this
        	//operation was issued by the application (and not an internal operation, check the uponDecidedNotification)

            sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                        IncorrectAgreement.PROTOCOL_ID);
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        //Maybe we should make sure operations are executed in order?
        // what


        //You should be careful and check if this operation if an application operation (and send it up)

        //or if this is an operations that was executed by the state machine itself (in which case you should execute)
        //DLT.add(new OperationClass(notification.getOpId(), notification.getOperation()));
        triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));

    }

    /*---------------------------------Sending Messages ---------------------------------------- */
    /* ---- Timer Events ---- */

    int requestToJoinIndex = 0;
    private void requestToJoin(JoiningTimer timer, long timerId) {
        if(requestToJoinIndex >= membership.size()) {requestToJoinIndex = 0;}
        Host host = initialMembership.get(requestToJoinIndex);

        openConnection(host);
        //sendMessage(new JoinRequest(self), host);
        requestToJoinIndex++;
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
    private void uponRequestToJoin(JoinRequest request, Host from, short sourceProto, int channelId) {
        logger.info("Received JoinReply from {}",from);

        //Generate a operationID
        //Propose to the multipaxos leader
        //when I recieve the reply from multipaxos, send a message to the requesting process
        // sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()), IncorrectAgreement.PROTOCOL_ID);



        List<Host> currentMembership = new LinkedList<>(membership);
        List<Integer> stateSnapshot = new LinkedList<>();

        JoinReplyMsg msg = new JoinReplyMsg(currentMembership, stateSnapshot);
        sendMessage(msg, request.getRequester());

        logger.info("Sent JoinReply to {}",request.getRequester());

    }


    private void uponJoinReply(JoinReplyMsg reply, Host from,short sourceProto, int channelId) {
        // full state
        // instance
        // Leader
        // membership
        logger.info("Received JoinReply from {} with membership: {}", from, reply.getCurrentMembership());
        this.state = State.ACTIVE;
        this.membership = new LinkedList<>(reply.getCurrentMembership());
        this.nextInstance = reply.getStateSnapshot().get(0);

        membership.forEach(this::openConnection);
        triggerNotification(new JoinedNotification(membership, 0));
        processBufferedRequests();
    }



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
