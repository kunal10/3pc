package dc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

/**
 * TODO :
 * 1) Add a constructor for recovery from log after failure.
 * 2) Add a method to reset the state for a new transaction.
 * 3) Add threads for different algorithms executed by process in 3pc.
 */
public class Process {
  
  /**
   * @param pId
   * @param nc
   * @param startTime
   */
  public Process(int pId, long startTime, Config config) {
    super();
    this.pId = pId;
    // cId should be set to first process whenever the process gets involved in
    // a new transaction.
    this.cId = 1;
    if (pId == 1) {
      this.type = Message.NodeType.COORDINATOR;
    } else {
      this.type = Message.NodeType.PARTICIPANT;
    }
     // Initialize all Blocking queues
    controllerQueue = new LinkedBlockingQueue<>();
    commonQueue = new LinkedBlockingQueue<>();
    heartbeatQueue = new LinkedBlockingQueue<>();
    
    // Initialize Net Controller. Pass all the queues required.
    nc = new NetController(config, controllerQueue, commonQueue, heartbeatQueue);
    
    int numParticipants = nc.getConfig().numProcesses;
    
    // The value at index 0 will be irrelevant since 0 corresponds to the controller process.
    boolean[] upset = new boolean[numParticipants];
    
    for (int i = 0; i < upset.length; i++) {
      upset[i] = true;
    }
    this.state = new State(State.StateType.UNCERTAIN, upset);
    
    this.numProcesses = numParticipants;
    this.transaction = null;
    this.startTime = startTime;
  }
  /** 
   * This thread is used for sending periodic heart beats to every process in
   * the network. Every heart beat contains the state of the process at the time 
   * heart beat was sent. This is used by every process for following:
   * 1) To detect the death of other processes.
   * 2) Used by non-participant processes to get the 3pc decision after they 
   *    revive.
   * 3) Used by all processes to identify the set of last processes in case of a 
   *    total failure.
   */
  class HeartBeat extends Thread {
    public void run() {
      while(true) {
        synchronized(type) {
          Message.NodeType srcType = Message.NodeType.NON_PARTICIPANT;
          if (type == Message.NodeType.COORDINATOR ||
                  type == Message.NodeType.PARTICIPANT) {
            srcType = Message.NodeType.PARTICIPANT;
          }
          long curTime = getCurTime();
          for (int dest = 1; dest <= numProcesses; dest++) {
            // We are setting the destination type as PARTICIPANT here but id 
            // does not matter since the destination process knows whether 
            // it is a participant or non-participant.
            Message m = new Message(pId, dest, srcType,
                    Message.NodeType.PARTICIPANT, curTime);
            nc.sendMsg(dest, m);
          }
        }
      }
    }
  }
  
  public synchronized State getState() {
    return state;
  }
  
  public Transaction getTransaction() {
    return transaction;
  }
  
  public synchronized boolean isParticipant() {
    if (type == Message.NodeType.COORDINATOR ||
            type == Message.NodeType.PARTICIPANT) {
      return true;
    }
    return false;
  }
  
  /** Will return 0 if called before startTime. */
  public long getCurTime() {
    long curTime = System.currentTimeMillis() - startTime;
    return (curTime > 0) ? curTime : 0;
  }
  
  private int pId;
  /**
   *  Before updating this lock should be obtained on type as both should be 
   */
  private int cId;
  /**
   * NOTE : Both Participant and Coordinator thread running for the coordinator
   * process use the same outgoing socket for sending the message. So, 
   * Message.NodeType field present in messages can be used to distinguish
   * between messages sent from Coordinator/Participant thread.
   * For any other purpose (eg. srcType in Heartbeat message from this process)
   * type COORDINATOR -> PARTICIPANT.
   */
  private Message.NodeType type;
  /**
   * Number of processes involved in the 3pc protocol. This value is 1 less than
   * from nc.getConfig().numProcesses since controller is not involved in 3pc.
   */
  private int numProcesses;
  private NetController nc;
  /**
   * This thread should be started only after startTime in case this process is
   * started for 1st time so that it won't send heart beats to processes which 
   * are not yet up. Also it should start heartbeat only after reviveTime incase 
   * this process is being revived after failure. 
   */
  private HeartBeat hb;
  /**
   * State of the process at any particular point of time. This can be modified
   * by different threads spawned by this process in the following scenarios:
   * 1) By HeartBeat thread 
   *    - When it updates upset on death of some participant.
   * 2) By Participant/New Participant thread
   *    - When it updates StateType on reaching decision.
   * 3) By Non_Participant thread
   *    - When it finds out the decision from some participant.
   */
  private State state;
  /**
   * Current transaction being processed by this process. Null if no transaction
   * if being processed. Updated whenever a new transaction is started or
   * completed.
   */
  private Transaction transaction;
  /** 
   * Time in milli sec when this process is started. Should be same for all the
   * processes so that their clock is synchronized.
   * NOTE : If process dies and recovers after that, startTime will remain same.
   */
  private long startTime;
  
  /**
   * Queue for storing all the incoming messages which are coming from the controller. 
   * This is passed to NetController. Only non-heartbeat messages.
   * */
  private BlockingQueue<Message> controllerQueue;
  /**
   * Queue for storing all incoming messages from all processes other than the controller.
   * Only non heartbeat messages. 
   */
  private BlockingQueue<Message> commonQueue;
  /**
   * Queue for storing all heartbeat messages from all the other processes. 
   */
  private BlockingQueue<Message> heartbeatQueue;
}
