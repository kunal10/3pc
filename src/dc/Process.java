package dc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import dc.Action.ActionType;
import dc.Message.NotificationType;
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
    
    this.config = config;
    
    // Initialize Net Controller. Pass all the queues required.
    nc = new NetController(config, controllerQueue, commonQueue, heartbeatQueue);
    
    this.numProcesses = nc.getConfig().numProcesses;
    this.transaction = null;
    this.vote = true;
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
          // Loop starts from 1 since we don't need to send the heartbeat to 
          // the controller.
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
  
  /**
   * Executes the 
   */
  class Coordinator extends Thread {
    public void run() {
      // Wait for startTransaction message from Controller.
      Message m = null;
      try {
        m = controllerQueue.take();
        if (m.getAction().getType() != ActionType.START) {
          config.logger.log(Level.SEVERE, "First message should always be START");
          return;
        }
        // Notify controller about receipt of start message.
        notifyController(m, NotificationType.RECEIVE);
        
        // Wait for controller's response.
        m = controllerQueue.take();
        executeInstruction(m);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  public synchronized State getState() {
    return state;
  }
  
  public Transaction getTransaction() {
    return transaction;
  }
  
  public void notifyController(Message m, NotificationType nt) {
    m.setNotificationType(NotificationType.RECEIVE);
    nc.sendMsg(0, m);
  }
  
  /**
   * This method should be called by the controller before starting a new
   * transaction by sending a start message.
   * @param t
   * @param vote
   */
  public void initNextTransaction(Transaction t, boolean vote) {
    this.transaction = t;
    this.vote = vote;
    // The value at index 0 will be irrelevant since 0 corresponds to the
    // controller process.
    boolean[] upset = new boolean[this.numProcesses];
    for (int i = 0; i < upset.length; i++) {
      upset[i] = true;
    }
    this.state = new State(State.StateType.UNCERTAIN, upset);
  }
  
  public void executeInstruction(Message m) {
    Instruction instr = m.getInstruction();
    if (m.getInstruction() == null) {
      config.logger.log(Level.SEVERE, 
              "Execute Instruction received a message with no instruction" +
                      m.toString());
    }
    switch(instr.getInstructionType()) {
      case CONTINUE:
        break;
      case KILL:
        config.logger.log(Level.INFO, "Received Kill Instruction");
        kill();
        break;
      case HALT:
        config.logger.log(Level.INFO, "Received Halt Instruction");
        halt();
        break;
      case RESUME:
        config.logger.log(Level.INFO, "Resuming processing");
        break;
      case REVIVE:
      default:
        // We should never reach this point.
        config.logger.log(Level.SEVERE, "Received unexpected instruction: " +
                instr.toString());
        break;
    }
  }
  
  public synchronized boolean isParticipant() {
    if (type == Message.NodeType.COORDINATOR ||
            type == Message.NodeType.PARTICIPANT) {
      return true;
    }
    return false;
  }
  
  /** 
   */
  public void kill() {
    
  }
  
  /**
   * 
   */
  public void halt() {
    
  }
  
  /** Will return 0 if called before startTime. */
  public long getCurTime() {
    long curTime = System.currentTimeMillis() - startTime;
    return (curTime > 0) ? curTime : 0;
  }
  
  /** This process's unique id. */
  private int pId;
  /**
   *  Process id of last coordinator recognized by this process. Won't be 
   *  populated after recovery.
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
   * Total number of processes (including the coordinator). 
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
   * Queue for storing all the incoming messages which are coming from the controller. 
   * This is passed to NetController. Only non-heartbeat messages.
   * */
  private BlockingQueue<Message> controllerQueue;
  /**
   * Queue for storing all non heart beat messages from all processes other than 
   * the controller.
   */
  private BlockingQueue<Message> commonQueue;
  /**
   * Queue for storing all heartbeat messages from all the other processes. 
   */
  private BlockingQueue<Message> heartbeatQueue;
  /**
   * Config object used for setting up NetController. 
   */
  private Config config;
  /**
   * Vote corresponding to the current transaction. This should be set by 
   * before starting a new transaction.
   */
  private boolean vote;
  /** 
   * Time in milli sec when this process is started. Should be same for all the
   * processes so that their clock is synchronized.
   * NOTE : If process dies and recovers after that, startTime will remain same.
   */
  private long startTime;
}
