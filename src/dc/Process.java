package dc;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import javax.xml.transform.sax.SAXTransformerFactory;

import dc.Action.ActionType;
import dc.Message.NodeType;
import dc.Message.NotificationType;
import dc.State.StateType;
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
    // TODO : Populate this from DT Log in case of recovery.
    this.playlist = new Playlist();

    // Initialize all Blocking queues
    // TODO : Figure out if these should be initialized before all transactions.
    controllerQueue = new LinkedBlockingQueue<>();
    commonQueue = new LinkedBlockingQueue<>();
    heartbeatQueue = new LinkedBlockingQueue<>();

    coordinatorQueue = new LinkedBlockingQueue<>();
    coordinatorControllerQueue = new LinkedBlockingQueue<>();

    this.config = config;

    // Initialize Net Controller. Pass all the queues required.
    config.logger.info("Initiliazing nc for process " + pId);
    nc = new NetController(config, controllerQueue, commonQueue, heartbeatQueue,
            coordinatorQueue, coordinatorControllerQueue);
    config.logger.info("Finished Initiliazing nc for process " + pId);

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
   * revive.
   * 3) Used by all processes to identify the set of last processes in case of a
   * total failure.
   */
  class HeartBeat extends Thread {
    
    /**
     * Maintain the exisiting set of timers for all alive processes. 
     */
    HashMap<Integer, TimerTask> exisitingTimers;
    /**
     * Timer to use for scheduling tasks.
     */
    Timer timer;
    public HeartBeat() {
      exisitingTimers = new HashMap<>();
      timer = new Timer();
    }
    
    /** 
    * 
    * @author av28895
    * Timer task to schedule send of heart beat regularly.
    */
	  class SendHeartBeatTask extends TimerTask{
	    public void run()
	    {
	      sendHeartBeat();
	    }
	  }
	  
	  class ProcessKillOnTimeoutTask extends TimerTask{
	    int processToRemoveFromUpSet;
	    public ProcessKillOnTimeoutTask(int i) {
        processToRemoveFromUpSet = i;
      }
	    public void run() {
        
      }
	  }
    public void sendHeartBeat() {
      synchronized (type) {
        Message.NodeType srcType = Message.NodeType.NON_PARTICIPANT;
        if (isParticipant()) {
          srcType = Message.NodeType.PARTICIPANT;
        }
        long curTime = getCurTime();
        // Loop starts from 1 since we don't need to send the heart beat to
        // the controller.
        for (int dest = 1; dest <= numProcesses; dest++) {
          Message m = null;
          if (state.getUpset()[dest]) {
            m = new Message(pId, dest, srcType, Message.NodeType.PARTICIPANT,
                    curTime);
          } else {
            m = new Message(pId, dest, srcType,
                    Message.NodeType.NON_PARTICIPANT, curTime);
          }
          nc.sendMsg(dest, m);
        }
      }
    }
    
    private void addTimerToExistingTimer(TimerTask tt, int i){
      int freq = 1200;
      TimerTask tti = new ProcessKillOnTimeoutTask(i);
      exisitingTimers.put(i, tti);
      timer.schedule(tti, freq);
    }
    

    /**
     * Process heart beats of other processes.
     * TODO :
     * 1) Add timers to detect death.
     * 2) Update upsets in case of death.
     * 3) In case of total failure figure out if you are in set of last
     * processes.
     */
    public void processHeartBeat() {
      while (!heartbeatQueue.isEmpty()) {
        Message m = heartbeatQueue.remove();
        // int src = m.getSrc();
        // Disable the timer if a timer is running for that process
        if(exisitingTimers.containsKey(m.getSrc())){
          exisitingTimers.get(m.getSrc()).cancel();
        }
        // Add a new timer for the process which has sent a heartbeat
        
        
        // If non participant receives a decision from some other process
        // then it records it, notifies the controller.
        if (!isParticipant()) {
          dc.State s = m.getState();
          if (s.isTerminalState()) {
            recordDecision(s.getType());
            updatePlaylist();
            notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                    ActionType.DECISION, s.getType().name());
            // Return since we don't need to process any more messages.
            return;
          }
        }
      }
    }

    public void run() {
      long prevSendTime = 0;
      // Frequency at which heart beats are sent. Should not be too small other
      // wise heart beat queue will start filling faster than the rate at which
      // it is consumed.
      long freq = 1000;
      // Set a new timer which executed the SendHeartbeatTask for the given frequency.
      // On kill this timer should be killed using tt.cancel(); timer.cancel();
      // TODO: Register timer threads for killing later.
      
      TimerTask tt = new SendHeartBeatTask();
      timer.schedule(tt, 0, freq);
      
      // Initialize all the timers to track heartbeat of other processes.
      // Timeout on freq * 2
      for (int i = 1; i < numProcesses; i++ ) {
        if(i != pId){
          addTimerToExistingTimer(tt, i);
        }
      }
      
      while (true) {
        // If process has not reached a decision it keeps processing heart beats
        // of other processes.
        if (!state.isTerminalState()) {
          processHeartBeat();
        }
      }
    }
  }

  /**
   * Executes the
   */
  class Coordinator extends Thread {

    /**
     * TODO : Check if participant are alive before sending them a message
     * otherwise we will get exceptions in nc.sendMsg.
     * 
     * @param at
     * @param steps
     */
    private void sendPartial(ActionType at, int steps) {
      Message msg = null;
      Action action = new Action(at, "");
      if (steps == -1) {
        for (int i = 1; i < numProcesses; i++) {
          msg = new Message(pId, i, NodeType.COORDINATOR, NodeType.PARTICIPANT,
                  action, getCurTime());
          nc.sendMsg(i, msg);
        }
      } else {
        for (int i = 1; i <= steps; i++) {
          msg = new Message(pId, i, NodeType.COORDINATOR, NodeType.PARTICIPANT,
                  action, getCurTime());
          nc.sendMsg(i, msg);
        }
      }
    }

    /**
     * @return Returns true if all processes voted yes, false if any of the
     *         process died before voting or it voted no.
     * @throws InterruptedException
     */
    private boolean waitForVotes() throws InterruptedException {
      int receivedVotes = 0, numParticipants = numProcesses - 1;
      while (receivedVotes < numParticipants) {
        if (!allParticipantsAlive()) {
          return false;
        }
        if (!coordinatorQueue.isEmpty()) {
          // Consume vote and increment the counter.
          Message msg = coordinatorQueue.remove();
          // Return if a No vote is received.
          if (msg.getAction().getValue().compareToIgnoreCase("No") == 0) {
            config.logger.log(Level.INFO,
                    "Received No vote from process: " + msg.getSrc());
            return false;
          }
          config.logger.log(Level.INFO,
                  "Received Yes vote from process: " + msg.getSrc());
          receivedVotes++;
        }
      }
      return true;
    }

    /**
     * Waits till it receives Acks from all alive processes.
     */
    private void waitForAck() {
      int receivedAcks = 0, numParticipants = numProcesses - 1;
      while (receivedAcks < numParticipants) {
        if (!coordinatorQueue.isEmpty()) {
          // Consume ack and increment the counter.
          Message msg = coordinatorQueue.remove();
          config.logger.log(Level.INFO,
                  "Received Ack from process: " + msg.getSrc());
          receivedAcks++;
        }
        numParticipants = numAliveParticipiants();
      }
    }

    public void run() {
      Message msg = null;
      try {
        // Notify controller about starting of 3PC.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.START, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // Send partial or complete VOTE_REQ.
        int steps = msg.getInstr().getPartialSteps();
        sendPartial(ActionType.VOTE_REQ, steps);
        // TODO : Write 3PC in DT log.
        // Notify controller about send of VOTE_REQ.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.VOTE_REQ, Integer.toString(steps));
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // Wait for votes from all processes.
        boolean vote = waitForVotes();
        // Notify controller about receipt of VOTE_RES.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.VOTE_RES, msg.getAction().getValue());
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);
        steps = msg.getInstr().getPartialSteps();

        if (!vote) {
          // TODO : Write Abort in DT Log.
          recordDecision(StateType.ABORTED);
          // Send Abort to all participants who voted yes and are alive.
          sendDecision(StateType.ABORTED, steps);
          return;
        }

        // Send precommit to all participants who voted yes and are alive.
        sendPartial(ActionType.PRE_COMMIT, steps);
        // Notify controller about send of Precommit.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.PRE_COMMIT, msg.getAction().getValue());
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // Wait for ack from all the participants.
        waitForAck();
        // Notify controller about receipt of ACK.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.ACK, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // Write Commit in DT Log.
        recordDecision(StateType.COMMITED);
        sendDecision(StateType.COMMITED, steps);
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

  /**
   * This method should be called by controller before sending a start message
   * for a new transaction.
   * 
   * @param t
   * @param vote
   */
  public void initTransaction(Transaction t, boolean vote) {
    this.transaction = t;
    this.vote = vote;
    // The value at index 0 will be irrelevant since 0 corresponds to the
    // controller process.
    boolean[] upset = new boolean[this.numProcesses];
    for (int i = 0; i < upset.length; i++) {
      upset[i] = true;
    }
    this.state = new State(State.StateType.UNCERTAIN, upset);
    // cId should be set to first process whenever the process gets involved in
    // a new transaction.
    this.cId = 1;
    if (pId == 1) {
      this.type = Message.NodeType.COORDINATOR;
    } else {
      this.type = Message.NodeType.PARTICIPANT;
    }
  }

  /**
   * Write the decision to DT Log and update the state.
   */
  private void recordDecision(StateType st) {
    // Log decision to DT log.
    state.setType(st);
  }

  /**
   * TODO : Check for processes which voted yes and are still alive.
   * Figure out how to will handle the case where a process is dead but death
   * has not been detected. In such a case nc will throw exceptions.
   */
  private void sendDecision(StateType st, int steps) {
    Message msg = null;
    Action action = new Action(ActionType.DECISION, st.name());
    if (steps == -1) {
      for (int dest = 1; dest < numProcesses; dest++) {
        // Coordinator should not send a decision to its participant thread so
        // decision is not written to DT Log twice.
        if (dest == pId) {
          continue;
        }
        msg = new Message(pId, dest, NodeType.COORDINATOR, NodeType.PARTICIPANT,
                action, getCurTime());
        nc.sendMsg(dest, msg);
      }
    } else {
      for (int dest = 1; dest <= steps; dest++) {
        // Coordinator should not send a decision to its participant thread so
        // decision is not written to DT Log twice.
        if (dest == pId) {
          // Increment step by 1 as we are ignoring 1
          steps++;
          continue;
        }
        msg = new Message(pId, dest, NodeType.COORDINATOR, NodeType.PARTICIPANT,
                action, getCurTime());
        nc.sendMsg(dest, msg);
      }
    }
  }

  /**
   * Update the playlist for current transaction. Should be called only once per
   * transaction after the decision has been made.
   */
  private void updatePlaylist() {
    config.logger.log(Level.INFO,
            "Updating playlist for following transaction: "
                    + transaction.toString());
    switch (transaction.getType()) {
      case ADD:
        playlist.addSong(transaction.getNewSong());
        break;
      case EDIT:
        Song oldSong = transaction.getOldSong();
        Song newSong = transaction.getNewSong();
        playlist.editSong(oldSong.getName(), newSong.getName(),
                newSong.getUrl());
        break;
      case DELETE:
        playlist.deleteSong(transaction.getOldSong().getName());
        break;
      default:
        config.logger.log(Level.SEVERE,
                "Received Invalid Transaction: " + transaction.toString());
    }
  }

  /**
   * @param srcType
   * @param nt
   * @param at
   * @param value
   */
  private void notifyController(NodeType srcType, NotificationType nt,
          ActionType at, String value) {
    Action action = new Action(at, value);
    Message msg = new Message(pId, 0, srcType, NodeType.COORDINATOR, action,
            getCurTime());
    msg.setNotificationType(nt);
    nc.sendMsg(0, msg);
  }

  private void executeInstruction(Message m) {
    Instruction instr = m.getInstr();
    if (m.getInstr() == null) {
      config.logger.log(Level.SEVERE,
              "Execute Instruction received a message with no instruction"
                      + m.toString());
    }
    switch (instr.getInstructionType()) {
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
        config.logger.log(Level.SEVERE,
                "Received unexpected instruction: " + instr.toString());
        break;
    }
  }

  private synchronized boolean isParticipant() {
    if (type == Message.NodeType.COORDINATOR
            || type == Message.NodeType.PARTICIPANT) {
      return true;
    }
    return false;
  }

  private int numAliveParticipiants() {
    int alive = 0;
    int numParticipants = numProcesses - 1;
    boolean[] curUpset = state.getUpset();
    for (int i = 1; i <= numParticipants; i++) {
      if (curUpset[i]) {
        alive++;
      }
    }
    return alive;
  }
  
  private boolean allParticipantsAlive() {
    return numAliveParticipiants() == (numProcesses - 1);
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
  private long getCurTime() {
    long curTime = System.currentTimeMillis() - startTime;
    return (curTime > 0) ? curTime : 0;
  }

  /** This process's unique id. */
  private int pId;
  /**
   * Process id of last coordinator recognized by this process. Won't be
   * populated after recovery.
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
   * - When it updates upset on death of some participant.
   * 2) By Participant/New Participant thread
   * - When it updates StateType on reaching decision.
   * 3) By Non_Participant thread
   * - When it finds out the decision from some participant.
   */
  private State state;
  /**
   * Current transaction being processed by this process. Null if no transaction
   * if being processed. Updated whenever a new transaction is started or
   * completed.
   */
  private Transaction transaction;
  /**
   * Distributed song playlist. Gets updated in transactions for which
   * transaction decision is COMMITED.
   */
  private Playlist playlist;
  /**
   * Queue for storing all the incoming messages which are coming from the
   * controller.
   * This is passed to NetController. Only non-heartbeat messages.
   */
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
   * Queue for storing all messages sent to coordinator by all processes except
   * controller
   */
  private BlockingQueue<Message> coordinatorQueue;
  /**
   * Queue for storing all messages sent to coordinator by controller
   */
  private BlockingQueue<Message> coordinatorControllerQueue;
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
