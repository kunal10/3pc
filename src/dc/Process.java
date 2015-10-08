package dc;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

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
      coordinator = new Coordinator();
      coordinator.start();
    }
    this.type = Message.NodeType.PARTICIPANT;
    participant = new Participant();
    participant.start();

    if (heartBeat != null) {
      heartBeat.stop();
    }
    heartBeat = new HeartBeat();
    heartBeat.start();
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
     * Maintain the exisisting set of timers for all alive processes.
     */
    HashMap<Integer, TimerTask> exisitingTimers;
    /**
     * Timer to use for scheduling tasks.
     */
    Timer timer;
    // TimerTask to send heart beats
    TimerTask tt;

    public HeartBeat() {
      exisitingTimers = new HashMap<>();
      timer = new Timer();
    }

    /**
     * Timer task to schedule send of heart beat regularly.
     */
    class SendHeartBeatTask extends TimerTask {
      public void run() {
        sendHeartBeat();
      }
    }

    // TODO: update the alive set
    class ProcessKillOnTimeoutTask extends TimerTask {
      int processToRemoveFromUpSet;

      public ProcessKillOnTimeoutTask(int i) {
        processToRemoveFromUpSet = i;
      }

      public void run() {
        config.logger.info("Detected death of " + processToRemoveFromUpSet);
        exisitingTimers.remove(processToRemoveFromUpSet);
        state.removeProcessFromUpset(processToRemoveFromUpSet);
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
        for (int dest = 1; dest < numProcesses; dest++) {
          Message m = null;
          if (state.getUpset()[dest]) {
            m = new Message(pId, dest, srcType, Message.NodeType.PARTICIPANT,
                    state, curTime);
          } else {
            m = new Message(pId, dest, srcType,
                    Message.NodeType.NON_PARTICIPANT, state, curTime);
          }
          nc.sendMsg(dest, m);
        }
      }
    }

    private void addTimerToExistingTimer(int i) {
      int delay = 1100;
      TimerTask tti = new ProcessKillOnTimeoutTask(i);
      exisitingTimers.put(i, tti);
      timer.schedule(tti, delay);
    }

    /**
     * Process heart beats of other processes.
     * TODO :
     * 1) Add timers to detect death. - Done Should test
     * 2) Update upsets in case of death. - Done Should test
     * 3) In case of total failure figure out if you are in set of last
     * processes.
     */
    public void processHeartBeat() {
      while (true) {
        Message m = null;
        try {
          m = heartbeatQueue.take();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        // config.logger.info("Consumed Heartbeat of "+m.getSrc());
        // Disable the timer if a timer is running for that process.
        if (exisitingTimers.containsKey(m.getSrc())) {
          exisitingTimers.get(m.getSrc()).cancel();
        }
        // Add a new timer for the process which has sent a heartbeat
        addTimerToExistingTimer(m.getSrc());

        // If non participant receives a decision from some other process
        // then it records it, notifies the controller.
        if (!isParticipant()) {
          dc.State s = m.getState();
          if (s.isTerminalState()) {
            recordDecision(s.getType());
            // Return since we don't need to process any more messages.
            return;
          }
        }
      }
    }

    public void shutdownTimers() {
      tt.cancel();
      timer.cancel();
      config.logger.info("Shut down timers");
    }

    public void run() {
      // Frequency at which heart beats are sent. Should not be too small other
      // wise heart beat queue will start filling faster than the rate at which
      // it is consumed.
      long freq = 1000;
      // Set a new timer which executed the SendHeartbeatTask for the given
      // frequency.
      // On kill this timer should be killed using tt.cancel(); timer.cancel();
      // TODO: Register timer threads for killing later.

      tt = new SendHeartBeatTask();
      timer.schedule(tt, 0, freq);

      // Initialize all the timers to track heartbeat of other processes.
      for (int i = 1; i < numProcesses; i++) {
        if (i != pId) {
          addTimerToExistingTimer(i);
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
   * Coordinator thread spawned by process 1 at the beginning of each
   * transaction.
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
      while (receivedVotes <= numParticipants) {
        // TODO : **BUG** We should maintain a hashmap of votes and check if
        // someone who has not voted has died. Currently even if process is
        // dying after casting its vote we are returning false from here.
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
      int steps = -1;
      try {
        // Notify controller about starting of 3PC.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.START, "");
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        executeInstruction(msg);

        // TODO : Write 3PC in DT log.
        // Notify controller about send of VOTE_REQ.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.VOTE_REQ, Integer.toString(steps));
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        // Send partial or complete VOTE_REQ.
        steps = msg.getInstr().getPartialSteps();
        sendPartial(ActionType.VOTE_REQ, steps);
        executeInstruction(msg);

        // Wait for votes from all processes.
        boolean overalVote = waitForVotes();
        // Notify controller about receipt of VOTE_RES.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.VOTE_RES, msg.getAction().getValue());
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        executeInstruction(msg);

        // If some participant died before voting or voted No, abort.
        if (!overalVote) {
          // TODO : Write Abort in DT Log.
          recordDecision(StateType.ABORTED);
          // Notify controller that coordinator is going to send Abort.
          notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                  ActionType.DECISION, "ABORTED");
          // Wait for controller's response.
          msg = coordinatorControllerQueue.take();
          steps = msg.getInstr().getPartialSteps();
          // Send Abort to all participants who voted yes and are alive.
          sendDecision(StateType.ABORTED, steps);
          executeInstruction(msg);
          return;
        }

        // Everyone voted Yes.
        // Notify controller that coordinator is going to send Precommit.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.PRE_COMMIT, "");
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        // Send precommit to all participants who voted yes and are alive.
        sendPartial(ActionType.PRE_COMMIT, steps);
        executeInstruction(msg);

        // Wait for ack from all the participants.
        waitForAck();
        // Notify controller about receipt of ACK.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.ACK, "");
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        executeInstruction(msg);

        // Write Commit in DT Log.
        recordDecision(StateType.COMMITED);
        // Notify controller that coordinator is going to send Commit.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.DECISION, "COMMIT");
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        steps = msg.getInstr().getPartialSteps();
        sendDecision(StateType.COMMITED, steps);
        executeInstruction(msg);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  class NewCoordinator extends Thread {
    public void run() {
      Message msg = null;
      int steps = -1;
      try {
        // Notify controller about send of STATE_REQ.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.STATE_REQ, "");
        // Wait for controller's response.
        msg = coordinatorControllerQueue.take();
        steps = msg.getInstr().getPartialSteps();
        sendPartialMsg(ActionType.STATE_REQ, steps);
        executeInstruction(msg);

        // Wait for state report message from all participants.
        // Ignore the ones who have died since send of STATE_REQ.
        String stateReport = waitForParticipantMsg(ActionType.STATE_RES);
        StateType reportedSt = StateType.valueOf(stateReport);
        StateType coordSt = state.getType();
        if (dc.State.isTerminalStateType(reportedSt)
                || dc.State.isTerminalStateType(coordSt)) {
          // If coordinator had not reached a decision before then it records it
          if (coordSt == StateType.UNCERTAIN) {
            // Write Decision in DT Log.
            recordDecision(reportedSt);
          } else {
            // Updated reported state with coordinator's state.
            reportedSt = coordSt;
          }
          // Notify controller and send the decision.
          notifyAndSendDecision(reportedSt);
        } else if (reportedSt == StateType.UNCERTAIN
                && coordSt == StateType.UNCERTAIN) {
          // All participants and the coordinator are uncertain so abort.
          reportedSt = StateType.ABORTED;
          // TODO : Write Decision in DT Log.
          recordDecision(reportedSt);
          // Notify controller and send the decision.
          notifyAndSendDecision(reportedSt);
        } else {
          // Some processes reported committable state.
          
          // Notify controller about send of PRE_COMMIT.
          notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                  ActionType.STATE_REQ, "");
          // Wait for controller's response.
          msg = coordinatorControllerQueue.take();
          steps = msg.getInstr().getPartialSteps();
          sendPartialMsg(ActionType.PRE_COMMIT, steps);
          executeInstruction(msg);
          
          // Wait for Acks from all operational processes.
          waitForParticipantMsg(ActionType.ACK);
          // TODO : Write Decision in DT Log.
          recordDecision(StateType.COMMITED);
          // Notify controller and send the decision.
          notifyAndSendDecision(reportedSt);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return;
    }
  }

  /**
   * Participant thread spawned by all the processes at the beginning of each
   * transaction. This thread should be stopped by heart beat before updating
   * the cId and upset (except in the case when Coordinator death is detected
   * before receipt of VOTE_REQ).
   */
  class Participant extends Thread {
    /**
     * This inherently assumes that coordinator is fixed when this function is
     * being executed. This is fine since when a current coordinator dies,
     * heart beat thread should first stop the process's participant thread
     * before modifying cId. Otherwise this method will send the message to the
     * new Coordinator.
     * 
     * @param at
     * @param value
     */
    private void sendToCoordinator(ActionType at, String value) {
      Action action = new Action(at, value);
      Message msg = new Message(pId, cId, NodeType.PARTICIPANT,
              NodeType.COORDINATOR, action, getCurTime());
      nc.sendMsg(cId, msg);
    }

    /**
     * Wait for specified message from Coordinator. If it instead gets
     * a STATE_REQ here, then it waits for heart beat to detect death of
     * previous coordinator and spawn a new participant thread which will
     * consume the STATE_REQ.
     * 
     * When waiting for VOTE_REQ, if coordinator dies, then heart beat should
     * not do anything other than updating the upset. As the waitForMessage
     * does this in this case.
     * 
     * NOTE : It does not consume STATE_REQ message if coordinator has died.
     * 
     * @return
     */
    private boolean waitForMessage(ActionType expAction) {
      while (true) {
        if (expAction == ActionType.VOTE_REQ) {
          if (!Process.this.isAlive(cId)) {
            config.logger.log(Level.INFO, "Detected death of Coordinator: "
                    + cId + " while waiting for VOTE_REQ");
            return false;
          }
        }
        // We can't use take here since we could receive a STATE_REQ from a new
        // coordinator which should be consumed by a new participant thread.
        if (commonQueue.isEmpty()) {
          continue;
        }
        Message msg = commonQueue.peek();
        ActionType recvAction = msg.getAction().getType();
        if (recvAction == ActionType.STATE_REQ) {
          config.logger.log(Level.INFO, "Waiting for VOTE_REQ. Revceived: "
                  + msg.toString() + "\n Current Coordinator must have died.");
          // Wait for heart beat to detect death of current coordinator and
          // spawn a new participant thread.
          while (true) {
          }
        }
        // Received Abort from coordinator while waiting for Precommit.
        if (expAction == ActionType.PRE_COMMIT
                && recvAction == ActionType.DECISION) {
          msg = commonQueue.remove();
          config.logger.log(Level.INFO, "Waiting for " + expAction.name()
                  + " Revceived: " + msg.toString());
          return false;
        }
        // Received unexpected action.
        if (recvAction != expAction) {
          config.logger.log(Level.SEVERE, "Waiting for " + expAction.name()
                  + " Revceived: " + msg.toString());
          return false;
        }
        // Received expected action.
        msg = commonQueue.remove();
        config.logger.log(Level.INFO,
                "Received " + expAction.name() + "from Coordinator");
        return true;
      }
    }

    public void run() {
      Message msg = null;
      try {
        boolean receivedVoteReq = waitForMessage(ActionType.VOTE_REQ);
        if (!receivedVoteReq) {
          if (pId == cId) {
            // Coordinator thread takes the decision and updates the playlist.
            // Although this should never happen if Coordinator is alive in
            // which case we will never reach here.
            return;
          }
          // TODO Write abort in DT Log.
          recordDecision(StateType.ABORTED);
          return;
        }

        // Notify the controller about receipt of VOTE_REQ.
        config.logger.info("Notifying receipt of Vote request");
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.VOTE_REQ, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // If participant's vote is No.
        if (!vote) {
          sendToCoordinator(ActionType.VOTE_RES, "No");
          if (pId == cId) {
            // Coordinator thread takes the decision and updates the playlist.
            return;
          }
          // TODO Write decision to DT Log.
          recordDecision(StateType.ABORTED);
          return;
        }

        // If participant's vote is Yes.
        // TODO Write Yes vote to DT Log.
        sendToCoordinator(ActionType.VOTE_RES, "Yes");

        // Notify the controller about the send of VOTE_RES.
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.VOTE_RES, "Yes");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        boolean receivedPrecommit = waitForMessage(ActionType.PRE_COMMIT);
        // Notify the controller about receipt of Precommit/Abort.
        if (receivedPrecommit) {
          notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                  ActionType.PRE_COMMIT, "");
        } else {
          notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                  ActionType.DECISION, "ABORT");
        }
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        if (!receivedPrecommit) {
          if (pId == cId) {
            // Coordinator thread takes the decision and updates the playlist.
            return;
          }
          // TODO Write decision to DT Log.
          recordDecision(StateType.ABORTED);
          return;
        }

        // Send Ack to coordinator.
        sendToCoordinator(ActionType.ACK, "");
        // Notify the controller about sending of ACK.
        notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                ActionType.ACK, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        boolean receivedCommit = waitForMessage(ActionType.DECISION);
        if (!receivedCommit) {
          // This should never happen as either participant will receive commit
          // or detect death of coordinator and spawn new participant thread and
          // flow will never reach here.
          config.logger.log(Level.SEVERE, "waitForCommit received false.");
        }

        // Notify the controller about receipt of COMMIT.
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.DECISION, "Commit");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);
        if (pId == cId) {
          // Coordinator thread takes the decision and updates the playlist.
          return;
        }
        // TODO Write Commit to DT Log.
        recordDecision(StateType.COMMITED);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Send partial/full message depending on number of specified steps.
   * Does not send message to itself and non operational processes.
   * 
   * @param at
   * @param steps
   */
  private void sendPartialMsg(ActionType at, int steps) {
    Message msg = null;
    Action action = new Action(at, "");
    boolean[] curUpset = getUpset();
    if (steps == -1) {
      steps = curUpset.length;
    }
    for (int i = 1; i <= steps && i < curUpset.length; i++) {
      if (i == pId || !curUpset[i]) {
        // Ignore itself and non-operational processes and increment steps to
        // compensate for this.
        steps++;
      }
      msg = new Message(pId, i, NodeType.COORDINATOR, NodeType.PARTICIPANT,
              action, getCurTime());
      nc.sendMsg(i, msg);
    }
  }

  /**
   * Waits till it receives messages from all participants except itself.
   */
  private String waitForParticipantMsg(ActionType expAction) {
    HashMap<Integer, String> received = new HashMap<Integer, String>();
    boolean receivedAllOperational = false;
    while (!receivedAllOperational) {
      if (!coordinatorQueue.isEmpty()) {
        // Consume message and add to the hashmap.
        Message msg = coordinatorQueue.remove();
        ActionType receivedAction = msg.getAction().getType();
        if (receivedAction != expAction) {
          config.logger.log(Level.SEVERE,
                  "Expecting : " + expAction.name() + "Received : "
                          + receivedAction + " from process: " + msg.getSrc());
        }
        // Received expected message from the participant.
        received.put(msg.getSrc(), msg.getAction().getValue());
        config.logger.log(Level.INFO, "Received " + expAction.name()
                + " from process: " + msg.getSrc());
      }
      boolean[] curUpset = getUpset();
      receivedAllOperational = true;
      for (int i = 1; i < curUpset.length; i++) {
        // Won't receive a message from itself.
        if (i == pId) {
          continue;
        }
        // If process is still operational but not received the message then
        // continue waiting.
        if (curUpset[i] && !received.containsKey(i)) {
          receivedAllOperational = false;
        }
      }
    }
    // Iterate over the hash map and return value appropriate for specified
    // action.
    String result = "";
    for (Integer key : received.keySet()) {
      if (expAction == ActionType.STATE_RES) {
        result = StateType.UNCERTAIN.name();
        StateType st = StateType.valueOf(received.get(key));
        if (State.isTerminalStateType(st)) {
          return st.name();
        }
      } else if (expAction == ActionType.VOTE_RES) {
        result = "Yes";
        if (received.get(key).compareToIgnoreCase("No") == 0) {
          return "No";
        }
      } else if (expAction == ActionType.ACK) {
        return result;
      }
    }
    return result;
  }

  private boolean[] getUpset() {
    synchronized (state) {
      return state.getUpset();
    }
  }

  public Transaction getTransaction() {
    return transaction;
  }

  /**
   * Record the decision and update the state if decision is commit.
   */
  private void recordDecision(StateType st) {
    synchronized (state) {
      state.setType(st);
      if (st == StateType.COMMITED) {
        updatePlaylist();
      }
      config.logger.log(Level.INFO,
              "Reached Decision : " + st.name() + " for current transaction.");
      // Notify the controller about the decision.
      notifyController(type, NotificationType.RECEIVE, ActionType.DECISION,
              st.name());
    }
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

  private void notifyAndSendDecision(StateType st) {
    // Notify controller that coordinator is going to send decision.
    notifyController(NodeType.COORDINATOR, NotificationType.SEND,
            ActionType.DECISION, st.name());
    // Wait for controller's response.
    Message msg = null;
    try {
      msg = coordinatorControllerQueue.take();
    } catch (InterruptedException e) {
      config.logger.log(Level.SEVERE,
              "Received Interrupt waiting for controller on SEND DECISION");
      e.printStackTrace();
      return;
    }
    int steps = msg.getInstr().getPartialSteps();
    sendDecision(st, steps);
    executeInstruction(msg);
    return;
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
    Message msg = new Message(pId, 0, srcType, NodeType.CONTROLLER, action,
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
        config.logger.log(Level.INFO, "Received Continue Instruction");
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

  /**
   * Return number of operational process as per current value of the upset.
   * NOTE : Upset should be maintained correctly in case of total failure for
   * this method to return correct value
   * 
   * @return
   */
  private int numOperational() {
    int operational = 0;
    boolean[] curUpset = getUpset();
    for (int i = 1; i < curUpset.length; i++) {
      if (curUpset[i]) {
        operational++;
      }
    }
    return operational;
  }

  private boolean isParticipant() {
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

  // TODO Add a aliveSet for this. Upset is not same as alive set.
  private boolean isAlive(int procId) {
    return getUpset()[procId];
  }

  // TODO Figure out if there is a better way to kill than stop.
  private void killThread(Thread t) {
    if (t != null) {
      t.stop();
    }
  }

  private void kill() {
    heartBeat.shutdownTimers();
    killThread(heartBeat);
    killThread(coordinator);
    killThread(newCoordinator);
    killThread(newParticipant);
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
   * Different threads which might be spawned by a process during a transaction.
   * All threads except the heartBeat are stopped on reaching a decision.
   * heartBeat thread of a previous transaction(if it exists) is stopped on
   * receipt of new initTransaction.
   */
  private Thread coordinator;
  private Thread participant;
  private Thread newCoordinator;
  private Thread newParticipant;
  private HeartBeat heartBeat;
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
