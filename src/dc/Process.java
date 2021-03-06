package dc;

import java.io.FileNotFoundException;
import java.io.IOException;
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

    // Initialize DT Log
    dtLog = new DTLog(config);

    // Initialize all Blocking queues
    // TODO : Figure out if these should be initialized before all transactions.
    controllerQueue = new LinkedBlockingQueue<>();
    commonQueue = new LinkedBlockingQueue<>();
    heartbeatQueue = new LinkedBlockingQueue<>();

    coordinatorQueue = new LinkedBlockingQueue<>();
    coordinatorControllerQueue = new LinkedBlockingQueue<>();

    received = new HashMap<Integer, String>();

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

  public void clearQueues() {
    commonQueue.clear();
    controllerQueue.clear();
    coordinatorQueue.clear();
    coordinatorControllerQueue.clear();
    heartbeatQueue.clear();
  }

  /**
   * This method should be called by controller before sending a start message
   * for a new transaction.
   * 
   * @param t
   * @param vote
   */
  public void initTransaction(Transaction t, boolean vote) {
    config.logger.info("Starting Transaction: " + t.toString());
    this.transaction = t;
    this.vote = vote;
    // Initialize the recovered state.
    recoveredState = new RecoveredState(config.numProcesses);
    // The value at index 0 will be irrelevant since 0 corresponds to the
    // controller process.
    boolean[] upset = new boolean[this.numProcesses];
    boolean[] alive = new boolean[this.numProcesses];
    for (int i = 0; i < upset.length; i++) {
      upset[i] = true;
      alive[i] = true;
    }
    this.state = new State(State.StateType.UNCERTAIN, upset, alive);
    // cId should be set to first process whenever the process gets involved in
    // a new transaction.
    this.cId = 1;
    if (pId == 1) {
      this.type = Message.NodeType.COORDINATOR;
      coordinator = new Coordinator();
      coordinator.start();
    } else {
      this.type = Message.NodeType.PARTICIPANT;
      participant = new Participant();
      participant.start();
    }
    heartBeat = new HeartBeat();
    heartBeat.start();
  }

  public void reviveProcessState(Transaction t, boolean vote) {
    config.logger.info("Reviveing process " + pId);
    clearQueues();
    killThreads();
    this.cId = -1;
    this.transaction = t;
    this.vote = vote;
    this.type = Message.NodeType.NON_PARTICIPANT;
    try {
      recoveredState = dtLog.parseDTLog();
      state = new State(recoveredState.state);
      playlist = new Playlist(recoveredState.playlist);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    heartBeat = new HeartBeat();
    config.logger.info("Restarting heartbeat after recovery");
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

    boolean[] upsetIntersection;
    boolean[] receivedNpMsg;

    public HeartBeat() {
      exisitingTimers = new HashMap<>();
      timer = new Timer();
      upsetIntersection = new boolean[numProcesses];
      receivedNpMsg = new boolean[numProcesses];
      boolean[] curUpset = getUpset();
      for (int i = 1; i < numProcesses; i++) {
        upsetIntersection[i] = curUpset[i];
        receivedNpMsg[i] = false;
      }
    }

    /**
     * Timer task to schedule send of heart beat regularly.
     */
    class SendHeartBeatTask extends TimerTask {
      public void run() {
        try {
          sendHeartBeat();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    // TODO: update the alive set
    class ProcessKillOnTimeoutTask extends TimerTask {
      int killedProcess;

      public ProcessKillOnTimeoutTask(int i) {
        killedProcess = i;
      }

      public void run() {
        try {
          config.logger.info("Detected death of " + killedProcess);
          state.removeProcessFromAlive(killedProcess);
          exisitingTimers.remove(killedProcess);
          handleDeath();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      private void waitForThreads() {
        if (participant != null) {
          try {
            participant.join();
            config.logger.log(Level.INFO, "Participant Thread: " + pId
                    + "returned on death of Coordinator");
            participant = null;
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        if (newParticipant != null) {
          try {
            newParticipant.join();
            config.logger.log(Level.INFO, "New Participant Thread: " + pId
                    + "returned on death of new Coordinator");
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }

      int findNextCoordinator() {
        boolean[] curUpset = getUpset();
        int nextCoord = pId;
        for (int i = 1; i < numProcesses; i++) {
          if (curUpset[i]) {
            nextCoord = i;
            break;
          }
        }
        return nextCoord;
      }

      private void handleDeath() {
        if (isParticipant()) {
          config.logger.log(Level.INFO, "Current cId " + cId);
          config.logger.log(Level.INFO,
                  "Removing " + killedProcess + "from upset");
          synchronized (state) {
            state.removeProcessFromUpset(killedProcess);
          }
          config.logger.log(Level.INFO,
                  "Upset[killedProcess]" + state.getUpset()[killedProcess]);
          if (killedProcess == cId) {
            config.logger.log(Level.INFO, "Detected death of Coord");
            waitForThreads();
            cId = findNextCoordinator();
            config.logger.info("Setting New Cid : " + cId);
            spawnNewThread();
          }
        }
      }
    }

    public void sendHeartBeat() {
      try {
        Message.NodeType srcType = Message.NodeType.NON_PARTICIPANT;
        if (isParticipant()) {
          srcType = Message.NodeType.PARTICIPANT;
        }
        long curTime = getCurTime();
        // Loop starts from 1 since we don't need to send the heart beat to
        // the controller.
        for (int dest = 1; dest < numProcesses; dest++) {
          if (dest == pId) {
            continue;
          }
          Message m = null;
          // if (Process.this.isAlive(dest)) {
          NodeType destType;
          if (getUpset()[dest]) {
            destType = NodeType.PARTICIPANT;
          } else {
            destType = NodeType.NON_PARTICIPANT;
          }
          m = new Message(pId, dest, srcType, destType, state, curTime);
          nc.sendMsg(dest, m);
          // }
        }
      } catch (Exception e) {
        config.logger.info("Exception in Sending heartbeat from " + pId + " "
                + e.getMessage());
        e.printStackTrace();
      }
    }

    private void addTimerToExistingTimer(int i) {
      try {
        int delay = 1200;
        TimerTask tti = new ProcessKillOnTimeoutTask(i);
        exisitingTimers.put(i, tti);
        timer.schedule(tti, delay);
      } catch (Exception e) {
      }
    }

    private void updateReceivedNpMsg(Message m) {
      if (type == NodeType.NON_PARTICIPANT) {
        if (!receivedNpMsg[m.getSrc()]) {
          config.logger.info("Received 1st Np msg from : " + m.getSrc());
        }
        receivedNpMsg[m.getSrc()] = true;
      }
    }

    private void updateUpsetIntersection(Message m) {
      boolean[] upset = m.getState().getUpset();
      for (int i = 1; i < upset.length; i++) {
        upsetIntersection[i] = upsetIntersection[i] && upset[i];
      }
    }

    private boolean isLastProcess() {
      for (int i = 1; i < numProcesses; i++) {
        if (i == pId) {
          continue;
        }
        if (upsetIntersection[i] && !receivedNpMsg[i]) {
          // config.logger
          // .info(i + ": UpsetIntersection = " + upsetIntersection[i]);
          // config.logger.info(i + ": receivedNpMsg = " + receivedNpMsg[i]);
          return false;
        }
      }
      return upsetIntersection[pId];
    }

    private boolean lastProcessesRecovered() {
      for (int i = 1; i < upsetIntersection.length; i++) {
        if (upsetIntersection[i] && !Process.this.isAlive(i)) {
          return false;
        }
      }
      return true;
    }

    private int findNewCid() {
      for (int i = 1; i < upsetIntersection.length; i++) {
        if (upsetIntersection[i]) {
          return i;
        }
      }
      return -1;
    }

    private void spawnNewThread() {
      if (cId == pId) {
        config.logger.log(Level.INFO,
                "Spawning New Coordinator Thread for: " + pId);
        type = NodeType.COORDINATOR;
        newCoordinator = new NewCoordinator();
        newCoordinator.start();
      } else {
        config.logger.log(Level.INFO,
                "Spawning New Participant Thread for: " + pId);
        type = NodeType.PARTICIPANT;
        newParticipant = new NewParticipant();
        newParticipant.start();
      }
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
      int count = 0;
      while (true) {
        if (count == 0) {
          for (int i = 1; i < numProcesses; i++) {
            config.logger.info(
                    "Process " + i + ": alive = " + Process.this.isAlive(i)
                            + " upsetIntersection = " + upsetIntersection[i]);
          }
          count++;
        }
        if (!heartbeatQueue.isEmpty()) {
          Message m = heartbeatQueue.remove();
          // Disable the timer if a timer is running for that process.
          if (exisitingTimers.containsKey(m.getSrc())) {
            // config.logger.info("Received HB msg : " + m.toString());
            exisitingTimers.get(m.getSrc()).cancel();
          }
          // Add a new timer for the process which has sent a heartbeat
          // config.logger.info(pId + " Adding timer for "+m.getSrc());
          addTimerToExistingTimer(m.getSrc());
          synchronized (state) {
            state.addProcessToAlive(m.getSrc());
          }
          updateReceivedNpMsg(m);
          updateUpsetIntersection(m);
          // If non participant receives a decision from some other process
          // then it records it, notifies the controller.
          if (!isParticipant()) {
            dc.State s = m.getState();
            if (s.isTerminalState()) {
              try {
                recordDecision(s.getType());
              } catch (Exception e) {
              }
              // Return since we don't need to process any more messages.
              return;
            }
          }
        }
        if (!isParticipant() && isLastProcess()) {
          cId = findNewCid();
          config.logger.log(Level.INFO,
                  "Detected total failure.. Setting new cid to: " + cId);
          if (cId == -1) {
            config.logger.log(Level.SEVERE,
                    "upsetIntersection changed while finding new cid after"
                            + "total failure");
          }
          spawnNewThread();
        }
      }
    }

    public void shutdownTimers() {
      tt.cancel();
      for (Integer key : exisitingTimers.keySet()) {
        exisitingTimers.get(key).cancel();
      }
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
        if (i != pId && dc.Process.this.isAlive(i)) {
          addTimerToExistingTimer(i);
        }
      }

      while (true) {
        // If process has not reached a decision it keeps processing heart beats
        // of other processes.
        // if (!state.isTerminalState()) {
        processHeartBeat();
        // } else {
        // for (Integer key : exisitingTimers.keySet()) {
        // exisitingTimers.get(key).cancel();
        // }
        // }
      }
    }
  }

  /**
   * Coordinator thread spawned by process 1 at the beginning of each
   * transaction.
   */
  class Coordinator extends Thread {

    public void run() {
      Message msg = null;
      int steps = -1;
      try {
        dtLog.writeStartTransaction(
                recoveredState.writtenPlaylistInTransaction);
        // Notify controller about starting of 3PC.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.START, "");
        // Wait for controller's response.
        config.logger.info("Waiting for Controllers response to START");
        msg = coordinatorControllerQueue.take();
        config.logger.info(
                "Received Controllers response to START" + msg.toString());
        executeInstruction(msg);
        // Notify controller about send of VOTE_REQ.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.VOTE_REQ, Integer.toString(steps));
        // Wait for controller's response.
        config.logger.info("Waiting for Controllers response to send VOTE_REQ");
        msg = coordinatorControllerQueue.take();
        config.logger.info("Received Controllers response to send VOTE_REQ"
                + msg.toString());
        // Send partial or complete VOTE_REQ.
        steps = msg.getInstr().getPartialSteps();
        config.logger.log(Level.INFO, "Coord sending vote req");
        sendPartialMsg(ActionType.VOTE_REQ, steps);
        executeInstruction(msg);

        // Wait for votes from all processes.
        String overallVote = waitForParticipantMsg(ActionType.VOTE_RES);
        // Notify controller about receipt of VOTE_RES.
        notifyController(NodeType.COORDINATOR, NotificationType.RECEIVE,
                ActionType.VOTE_RES, msg.getAction().getValue());
        // Wait for controller's response.
        config.logger
                .info("Waiting for Controllers response to revceive VOTE_RES");
        msg = coordinatorControllerQueue.take();
        config.logger.info("Received Controllers response to revceive VOTE_RES"
                + msg.toString());
        executeInstruction(msg);

        // If some participant died before voting or voted No, abort.
        if (overallVote.compareToIgnoreCase("No") == 0 || !vote) {
          // Write Abort in DT Log.
          recordDecision(StateType.ABORTED);
          // Notify controller and send the decision.
          notifyAndSendDecision(StateType.ABORTED);
          return;
        }

        // Everyone voted Yes.
        dtLog.writeVote(vote, recoveredState.writtenPlaylistInTransaction);
        // Update state to commitable.
        synchronized (state) {
          state.setType(StateType.COMMITABLE);
        }
        // Write Commitable state to DT Log.
        dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
        // Notify controller about send of PRE_COMMIT.
        notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                ActionType.PRE_COMMIT, "");
        // Wait for controller's response.
        config.logger
                .info("Waiting for Controllers response to send PRE_COMMIT");
        msg = coordinatorControllerQueue.take();
        config.logger
                .info("Received Controllers response to revceive PRE_COMMIT"
                        + msg.toString());
        steps = msg.getInstr().getPartialSteps();
        sendPartialMsg(ActionType.PRE_COMMIT, steps);
        executeInstruction(msg);

        // Wait for Acks from all operational processes.
        waitForParticipantMsg(ActionType.ACK);
        // Write Decision in DT Log.
        recordDecision(StateType.COMMITED);
        // Notify controller and send the decision.
        notifyAndSendDecision(StateType.COMMITED);
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
        config.logger.info("Waiting for Controllers response to STATE_REQ");
        msg = coordinatorControllerQueue.take();
        config.logger.info("Received Controllers response to STATE_REQ: "
                + msg.toString());
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
          // Write Decision in DT Log.
          recordDecision(reportedSt);
          // Notify controller and send the decision.
          notifyAndSendDecision(reportedSt);
        } else {
          // Some processes reported committable state.
          reportedSt = StateType.COMMITABLE;
          // Update state to commitable.
          synchronized (state) {
            state.setType(StateType.COMMITABLE);
          }
          // Write Commitable state to DT Log.
          dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
          // Notify controller about send of PRE_COMMIT.
          notifyController(NodeType.COORDINATOR, NotificationType.SEND,
                  ActionType.PRE_COMMIT, "");
          // Wait for controller's response.
          config.logger
                  .info("Waiting for Controllers response to send PRE_COMMIT");
          msg = coordinatorControllerQueue.take();
          config.logger.info("Received Controllers response to PRE_COMMIT"
                  + msg.toString());
          steps = msg.getInstr().getPartialSteps();
          sendPartialMsg(ActionType.PRE_COMMIT, steps);
          executeInstruction(msg);

          // Wait for Acks from all operational processes.
          waitForParticipantMsg(ActionType.ACK);
          // Write Decision in DT Log.
          recordDecision(StateType.COMMITED);
          // Notify controller and send the decision.
          notifyAndSendDecision(StateType.COMMITED);
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
    public void run() {
      Message msg = null;
      try {
        dtLog.writeStartTransaction(
                recoveredState.writtenPlaylistInTransaction);
        // Notify controller about starting of 3PC.
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.START, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        boolean receivedVoteReq = waitForCoordinatorMsg(ActionType.VOTE_REQ,
                msg);
        if (!receivedVoteReq) {
          // Write abort in DT Log.
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
          // Notify the controller about the send of VOTE_RES.
          notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                  ActionType.VOTE_RES, "No");
          sendToCoordinator(ActionType.VOTE_RES, "No");
          // Wait for controller's response.
          msg = controllerQueue.take();
          executeInstruction(msg);
          // Write decision to DT Log.
          recordDecision(StateType.ABORTED);
          return;
        }

        // If participant's vote is Yes.
        // Write Yes vote to DT Log.
        dtLog.writeVote(vote, recoveredState.writtenPlaylistInTransaction);
        // Notify the controller about the send of VOTE_RES.
        notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                ActionType.VOTE_RES, "Yes");
        sendToCoordinator(ActionType.VOTE_RES, "Yes");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        boolean receivedPrecommit = waitForCoordinatorMsg(ActionType.PRE_COMMIT,
                msg);
        if (!receivedPrecommit) {
          config.logger.log(Level.INFO, "Returning on death of current coord");
          return;
        }

        if (msg.getAction().getType() == ActionType.DECISION) {
          notifyController(NodeType.PARTICIPANT, NotificationType.DELIVER,
                  ActionType.DECISION, "ABORT");
          // Wait for controller's response.
          msg = controllerQueue.take();
          executeInstruction(msg);
          // Write decision to DT Log.
          recordDecision(StateType.ABORTED);
          return;
        }

        // Notify the controller about receipt of Precommit.
        synchronized (state) {
          state.setType(StateType.COMMITABLE);
        }
        dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.PRE_COMMIT, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        // Notify the controller about sending of ACK.
        notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                ActionType.ACK, "");
        // Send Ack to coordinator.
        sendToCoordinator(ActionType.ACK, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        boolean receivedCommit = waitForCoordinatorMsg(ActionType.DECISION,
                msg);
        if (!receivedCommit) {
          // If received some unexpected action or STATE_REQ from higher
          // coordinator.
          return;
        }

        // Notify the controller about receipt of COMMIT.
        notifyController(NodeType.PARTICIPANT, NotificationType.DELIVER,
                ActionType.DECISION, "Commit");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);
        // Write Commit to DT Log.
        recordDecision(StateType.COMMITED);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  class NewParticipant extends Thread {
    public void run() {
      Message msg = null;
      try {
        boolean recvStateReq = waitForCoordinatorMsg(ActionType.STATE_REQ, msg);
        if (!recvStateReq) {
          // If received some unexpected action or STATE_REQ from higher
          // coordinator.
          return;
        }

        // Notify the controller about receipt of STATE_REQ.
        config.logger.log(Level.INFO, "Notifying receipt of STATE_REQ");
        notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                ActionType.STATE_REQ, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);

        StateType st = null;
        synchronized (state) {
          st = state.getType();
        }
        // Notify the controller about send of STATE_RES.
        config.logger.log(Level.INFO,
                "Notifying controller that I am abt to send STATE_RES");
        notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                ActionType.STATE_RES, "");
        // Wait for controller's response.
        msg = controllerQueue.take();
        executeInstruction(msg);
        sendToCoordinator(ActionType.STATE_RES, st.name());

        boolean recvPrecommitOrAbort = waitForCoordinatorMsg(
                ActionType.PRE_COMMIT, msg);
        if (!recvPrecommitOrAbort) {
          // If received some unexpected action or STATE_REQ from higher
          // coordinator.
          return;
        }
        StateType decision = StateType.valueOf(msg.getAction().getValue());

        if (dc.State.isTerminalStateType(decision)) {
          // Write decision to DT Log.
          recordDecision(decision);
          return;
        } else if (decision == StateType.COMMITABLE) {
          synchronized (state) {
            state.setType(StateType.COMMITABLE);
          }
          dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
          // Notify the controller about receipt of PRE_COMMIT.
          notifyController(NodeType.PARTICIPANT, NotificationType.RECEIVE,
                  ActionType.PRE_COMMIT, "");
          // Wait for controller's response.
          msg = controllerQueue.take();
          executeInstruction(msg);

          // Notify the controller about sending of ACK.
          notifyController(NodeType.PARTICIPANT, NotificationType.SEND,
                  ActionType.ACK, "");
          // Wait for controller's response.
          msg = controllerQueue.take();
          executeInstruction(msg);
          // Send Ack to coordinator.
          sendToCoordinator(ActionType.ACK, "");

          boolean receivedCommit = waitForCoordinatorMsg(ActionType.DECISION,
                  msg);
          if (!receivedCommit) {
            // If received some unexpected action or STATE_REQ from higher
            // coordinator.
            return;
          }

          // Write Commit to DT Log.
          recordDecision(StateType.COMMITED);
        } else {
          config.logger.log(Level.SEVERE,
                  "waitForCoordinatorResponse did not set correct decision");
          return;
        }
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
    for (int i = 1; steps > 0 && i < curUpset.length; i++) {
      if (i == pId || !curUpset[i]) {
        config.logger.log(Level.INFO,
                "Not sending " + at.name() + " to process: " + i);
        // Don't send the message to the following :
        // 1. Itself
        // 2. Non-operational processes
        continue;
      }
      if (!received.containsKey(i) && at != ActionType.VOTE_REQ
              && at != ActionType.STATE_REQ) {
        // Processes who are not uncertain(i.e. voted no or didn't send ACK)
        continue;
      }
      msg = new Message(pId, i, NodeType.COORDINATOR, NodeType.PARTICIPANT,
              action, getCurTime());
      nc.sendMsg(i, msg);
      steps--;
    }
  }

  /**
   * Waits till it receives messages from all participants except itself.
   * NOTE : Resets the received map every time it is called.
   * TODO : Make this more modular.
   */
  private String waitForParticipantMsg(ActionType expAction) {
    received = new HashMap<Integer, String>();
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
      receivedAllOperational = true;
      for (int i = 1; i < numProcesses; i++) {
        // Won't receive a message from itself.
        if (i == pId) {
          continue;
        }
        // If process is still operational but not received the message then
        // continue waiting.
        if (isOperational(i) && !received.containsKey(i)) {
          receivedAllOperational = false;
        }
      }
    }
    // Iterate over the hash map and return value appropriate for specified
    // action.
    String result = "";
    for (int key = 1; key < numProcesses; key++) {
      if (key == cId) {
        continue;
      }
      if (expAction == ActionType.STATE_RES) {
        result = StateType.UNCERTAIN.name();
        if (!isOperational(key)) {
          continue;
        }
        if (!received.containsKey(key)) {
          config.logger.log(Level.SEVERE,
                  "Did not receive msg from operational process:" + key);
          return "";
        }
        StateType st = StateType.valueOf(received.get(key));
        if (State.isTerminalStateType(st)) {
          return st.name();
        }
      } else if (expAction == ActionType.VOTE_RES) {
        result = "Yes";
        // Return No if someone did not vote or voted No.
        if (!received.containsKey(key)
                || received.get(key).compareToIgnoreCase("No") == 0) {
          config.logger.log(Level.INFO, "Inside xyz");
          return "No";
        }
      } else if (expAction == ActionType.ACK) {
        return result;
      }
    }
    return result;
  }

  /**
   * This inherently assumes that coordinator is fixed when this function is
   * being executed. This is fine since when a current coordinator dies,
   * heart beat thread should wait for process's participant thread to terminate
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

  private boolean waitForVoteReq() {
    config.logger.info("Waiting for Vote Req");
    while (true) {
      Message msg = null;
      ActionType recvAction = null;
      if (!Process.this.isOperational(1)) {
        config.logger.log(Level.INFO,
                "Detected death of 1st Coordinator while waiting for "
                        + "VOTE_REQ");
        return false;
      }

      // We can't use take here since we could receive a STATE_REQ from a new
      // coordinator which should be consumed by a new participant thread.
      synchronized (commonQueue) {
        if (commonQueue.isEmpty()) {
          continue;
        }
        msg = commonQueue.peek();
      }
      recvAction = msg.getAction().getType();

      // Received STATE_REQ.
      if (recvAction == ActionType.STATE_REQ) {
        config.logger.log(Level.INFO, "Waiting for VOTE_REQ. Revceived: "
                + msg.toString() + "\n Current Coordinator must have died.");
        // Heart beat should detect death of current coordinator and
        // spawn a new participant thread which will consume this STATE_REQ
        return false;
      } else if (recvAction != ActionType.VOTE_REQ) {
        // Received unexpected action.
        config.logger.log(Level.SEVERE, "Waiting for "
                + ActionType.VOTE_REQ.name() + " Revceived: " + msg.toString());
        return false;
      } else {
        // Received VOTE_REQ.
        msg = commonQueue.remove();
        config.logger.log(Level.INFO,
                "Received " + recvAction.name() + "from Coordinator");
        return true;
      }
    }
  }

  /**
   * Wait for specified message from Coordinator. If it instead gets
   * a STATE_REQ from higher coordinator here, then it returns false.
   * Heart beat should wait for Participant/NewParticipant threads to terminate
   * before updating the cId and spawning NewPaticipant threads.
   * 
   * NOTE : It does not consume STATE_REQ message from higher coordinator.
   * 
   * @return
   */
  private boolean waitForCoordinatorMsg(ActionType expAction,
          Message response) {
    if (expAction == ActionType.VOTE_REQ) {
      return waitForVoteReq();
    }
    while (true) {
      Message msg = null;
      ActionType recvAction = null;
      // We can't use take here since we could receive a STATE_REQ from a new
      // coordinator which should be consumed by a new participant thread.
      synchronized (commonQueue) {
        if (commonQueue.isEmpty()) {
          if (!isOperational(cId)) {
            return false;
          }
          continue;
        }
        msg = commonQueue.peek();
      }
      recvAction = msg.getAction().getType();

      // Received unexpected action.
      if (recvAction != expAction) {
        if (recvAction == ActionType.STATE_REQ) {
          config.logger.log(Level.INFO, "Waiting for " + expAction.name()
                  + " from : " + cId + " Revceived: " + msg.toString());
          return false;
        } else if (expAction == ActionType.PRE_COMMIT
                && recvAction == ActionType.DECISION) {
          config.logger.log(Level.INFO, "Waiting for " + expAction.name()
                  + " Revceived: " + msg.toString());
          response.setAction(
                  new Action(recvAction, msg.getAction().getValue()));
          return true;
        }
        config.logger.log(Level.SEVERE, "Waiting for " + expAction.name()
                + " Revceived: " + msg.toString());
        return false;
      }
      if (recvAction == ActionType.STATE_REQ && msg.getSrc() != cId) {
        config.logger.log(Level.INFO, "Waiting for " + expAction.name()
                + " from : " + cId + " Revceived: " + msg.getSrc());
        return false;
      } else if (recvAction == ActionType.DECISION) {
        StateType decision = StateType.valueOf(msg.getAction().getValue());
        if (decision != StateType.COMMITED) {
          // This will always be COMMIT because we never wait for
          // Coordinator to send Abort Decision in the protocol.
          config.logger.log(Level.SEVERE, "Waiting for " + expAction.name()
                  + " from : " + cId + " Revceived: " + msg.getSrc());
          return false;
        }
      } else if (recvAction == ActionType.PRE_COMMIT) {
        // Received PRE_COMMIT.
        response.setAction(new Action(recvAction, StateType.COMMITABLE.name()));
        config.logger.log(Level.INFO,
                "Received PreCommit from Coordinator:" + cId);
      }
      // Received expected action.
      msg = commonQueue.remove();
      config.logger.log(Level.INFO,
              "Received " + expAction.name() + "from Coordinator");
      return true;
    }
  }

  private boolean[] getUpset() {
    synchronized (state) {
      return state.getUpset();
    }
  }

  private boolean[] getAlive() {
    synchronized (state) {
      return state.getAlive();
    }
  }

  public Transaction getTransaction() {
    return transaction;
  }

  /**
   * Record the decision and update the state if decision is commit.
   */
  private void recordDecision(StateType st) {
    config.logger.info("INSIDE RECORD DECISION");
    // synchronized (state) {
    state.setType(st);
    if (st == StateType.COMMITED) {
      updatePlaylist();
    }
    dtLog.writeDecision(st.toString(),
            recoveredState.writtenPlaylistInTransaction);
    dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
    dtLog.writePlaylist(playlist, recoveredState.writtenPlaylistInTransaction);
    recoveredState.writtenPlaylistInTransaction = true;
    config.logger.log(Level.INFO,
            "Reached Decision : " + st.name() + " for current transaction.");
    config.logger.log(Level.INFO, "Updated Playlist:" + playlist.toString());
    // Notify the controller about the decision.
    notifyController(type, NotificationType.RECEIVE, ActionType.DECISION,
            st.name());
    // Wait for controller's response.
    Message msg = null;
    try {
      if (type == NodeType.COORDINATOR) {
        config.logger
                .info("Waiting for Controllers response to receive DECISION");
        msg = coordinatorControllerQueue.take();
        config.logger.info("Received Controllers response to receive DECISION"
                + msg.toString());
      } else {
        config.logger.info("Waiting as Participant");
        msg = controllerQueue.take();
      }
      config.logger.info("Received msg from Controller" + msg.toString());
    } catch (InterruptedException e) {
      // e.printStackTrace();
      return;
    }
    executeInstruction(msg);
    dtLog.writeEndTransaction();
    // }
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
        if (dest == pId || !isOperational(dest)) {
          continue;
        }
        msg = new Message(pId, dest, NodeType.COORDINATOR, NodeType.PARTICIPANT,
                action, getCurTime());
        nc.sendMsg(dest, msg);
      }
    } else {
      for (int dest = 1; dest <= steps && dest < numProcesses; dest++) {
        // Coordinator should not send a decision to its participant thread so
        // decision is not written to DT Log twice.
        if (dest == pId || !isOperational(dest)) {
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
      config.logger.info("Waiting for Controllers response to send DECISION");
      msg = coordinatorControllerQueue.take();
      config.logger.info("Received Controllers response to send DECISION"
              + msg.toString());
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
        config.logger
                .info("Deleting Song:" + transaction.getOldSong().getName());
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
    config.logger.info(srcType.name() + pId + "Notifying Controller abt : "
            + nt.name() + " of " + at.name() + " " + value);
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

  private boolean isParticipant() {
    if (type == Message.NodeType.COORDINATOR
            || type == Message.NodeType.PARTICIPANT) {
      return true;
    }
    return false;
  }

  private boolean isAlive(int procId) {
    return getAlive()[procId];
  }

  // NOTE: This should be used only by operational processes (Ones invloved in
  // the protocol)
  private boolean isOperational(int procId) {
    return (getUpset()[procId]);
  }

  // TODO Figure out if there is a better way to kill than stop.
  private void killThread(Thread t) {
    if (t != null) {
      t.stop();
    }
  }

  public void killThreads() {
    if (heartBeat != null) {
      heartBeat.shutdownTimers();
    }
    killThread(heartBeat);
    killThread(coordinator);
    killThread(participant);
    killThread(newCoordinator);
    killThread(newParticipant);
  }

  private void kill() {
    if (dtLog != null && state != null && recoveredState != null) {
      dtLog.writeState(state, recoveredState.writtenPlaylistInTransaction);
    }
    nc.shutdown();
    killThreads();
  }

  /**
   * 
   */
  public void halt() {
    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
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
   * Hashmap used by coordinator to store receipt of votes/acks from other
   * participants. This is used by sendPartialMessage for sending messages to
   * only those processes which are uncertain (who voted "Yes" for VOTE_REQ or
   * reported "Uncertain" for STATE_REQ)
   */
  private HashMap<Integer, String> received;
  /**
   * Time in milli sec when this process is started. Should be same for all the
   * processes so that their clock is synchronized.
   * NOTE : If process dies and recovers after that, startTime will remain same.
   */
  private long startTime;

  /**
   * DT Log handle for this process.
   */
  private DTLog dtLog;

  /**
   * Storing the recovered state from DTLog.
   */
  private RecoveredState recoveredState;
}
