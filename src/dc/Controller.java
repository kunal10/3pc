/**
 * 
 */
package dc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import dc.Action.ActionType;
import dc.Instruction.InstructionType;
import dc.Message.NodeType;
import dc.Message.NotificationType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

/**
 * @author av28895
 *
 */
public class Controller {

  /**
   * Controller reads its own config and the steps to be simulated.
   * Sets up the communication framework for itself
   * Initializes all the processes with their indiviual configs
   * Starts a sender thread for each process for each transaction.
   */

  /**
   * 
   * @param configFiles
   *          Array of filenames of all the configs to be used. 0th Element
   *          corresponds to the config for the controller.
   */
  public Controller(String[] configFiles) {
    try {

      // Read configs for each process and initialize each process with its
      // instruction list.
      config = new Config(configFiles[0]);
      initializeProcesses(configFiles);
      readSimulationConfig(config.simulationConfig);
      initializeMessageQueues();
      initializeCommunication();

      for (ConfigElement transaction : sc.getTransactionList()) {
        config.logger.info("Starting Transaction: "
                + transaction.getTransaction().toString());
        initializeTransaction(transaction);
        Thread[] threads = new SendHandler[config.numProcesses];
        decisionReached = new boolean[config.numProcesses];

        for (int i = 1; i < threads.length; i++) {
          messageQueue[i].clear();
          decisionReached[i] = false;
          threads[i] = new SendHandler(instructionQueue[i], i,
                  !(transaction.getNoVotes().contains(i)));
          threads[i].start();
        }
        for (int i = 1; i < threads.length; i++) {
          try {
            threads[i].join();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        for (int i = 1; i < config.numProcesses; i++) {
          processes[i].clearQueues();
          processes[i].killThreads();
        }
      }
      Thread.sleep(3000);
      for (int i = 1; i < config.numProcesses; i++) {
        processes[i].killThreads();
        processes[i].clearQueues();
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private boolean haveAllProcessesReachedDecision() {
    boolean rd = true;
    for (int i = 1; i < config.numProcesses; i++) {
      rd = rd && decisionReached[i];
    }
    return rd;
  }

  private void initializeProcesses(String[] configFiles)
          throws FileNotFoundException, IOException {
    processesConfigs = new Config[config.numProcesses];
    processes = new Process[config.numProcesses];
    instructionQueue = new LinkedList[config.numProcesses];
    for (int i = 1; i < config.numProcesses; i++) {
      processesConfigs[i] = new Config(configFiles[i]);
      processes[i] = new Process(processesConfigs[i].procNum, getCurrentTime(),
              processesConfigs[i]);
      instructionQueue[i] = new LinkedList<Instruction>();
    }
  }

  private void initializeTransaction(ConfigElement transaction) {
    // Set min inst value to 0.
    minSeqNumber = 0;
    // Set the current transaction to be executed.
    currentTransaction = transaction.getTransaction();
    config.logger.info("Current transaction :" + currentTransaction.toString());

    /*
     * process the transaction into n process inst lists start n
     * sender threads
     */
    for (int i = 1; i < instructionQueue.length; i++) {
      instructionQueue[i].clear();
    }
    splitTransactionsIntoIndiviualLists(transaction.instructions);
  }

  /**
   * Read the simulation config.
   * 
   * @param filename
   */
  private void readSimulationConfig(String filename) {
    sc = new SimulationConfig(filename);
    sc.processInstructions();
  }

  /**
   * Initialize the message queues for all the incoming processes.
   */
  private void initializeMessageQueues() {
    messageQueue = new LinkedBlockingQueue[config.numProcesses];
    for (int i = 0; i < messageQueue.length; i++) {
      messageQueue[i] = new LinkedBlockingQueue<Message>();
    }
    config.logger.info("Initialized message queues");
  }

  /**
   * Initialize communication.
   */
  private void initializeCommunication() {
    nc = new NetController(config, messageQueue);
    config.logger.info("Initialized net contorller for the controller");
  }

  /**
   * Look at all the instructions specified as a part of simulation config.
   * Split them into each process instruction list.
   * 
   * @param instructions
   */
  private void splitTransactionsIntoIndiviualLists(
          ArrayList<Instruction> instructions) {
    for (Instruction instruction : instructions) {
      int pId = instruction.getpId();
      instructionQueue[pId].add(instruction);
      config.logger.info(String.format("Adding %d to %d queue",
              instruction.getSeqNo(), pId));
    }
  }

  private void incrementNextInstructionSequenceNum() {
    ++minSeqNumber;
    config.logger.info("Updated Seq no to : " + minSeqNumber);
  }

  /**
   * Config corresponding to the controller.
   */
  private Config config;

  /**
   * All the configs to each of the process. Ignore 0th element.
   */
  private Config[] processesConfigs;

  /**
   * Simulation Config to be used.
   */
  private SimulationConfig sc;

  /**
   * All the processes in the simulation to which the controller has to
   * communicate to.
   */
  private Process[] processes;

  /**
   * Array of queues. Each array corresponding to the instruction queue of one
   * process. Ignore 0th element.
   */
  private LinkedList<Instruction>[] instructionQueue;

  /**
   * Queues for storing the incoming messages one for each process. These
   * queues should be passed to the net controller to initialize for the
   * incoming sock.
   */
  private LinkedBlockingQueue<Message>[] messageQueue;

  /**
   * Communication network object for controller.
   */
  private NetController nc;

  /**
   * Seq number of the next instruction to be executed.
   */
  private volatile int minSeqNumber;

  /**
   * The current transaction being executed.
   */
  private Transaction currentTransaction;

  /**
   * ID of the current coordinator
   */
  private volatile int currentCoordinatorId;
  /**
   * Array capturing whether a process has reached a decision. This can be
   * made out from the instruction sent by a process. Based on this move on to
   * the next transaction.
   */
  private boolean[] decisionReached;

  private long getCurrentTime() {
    return System.currentTimeMillis();
  }

  /**
   * A send thread used by the controller to communicate with a process.
   * 
   * @author av28895
   *
   */
  private class SendHandler extends Thread {

    private LinkedList<Instruction> indiviualInstructionQueue;
    private int procNum;
    // 0 means no and 1 means yes.
    private boolean vote;
    private int currentInstructionSeqNum;

    public SendHandler(LinkedList<Instruction> instructionQueue, int procNum,
            boolean vote) {
      this.indiviualInstructionQueue = instructionQueue;
      this.procNum = procNum;
      this.vote = vote;
    }

    /**
     * Check if the instruction to be executed corresponds to message which you
     * received.
     * Compare the action type and notification type.
     * 
     * @param i
     * @param m
     * @return
     */
    private boolean compareInstructionToMessage(Instruction i, Message m) {
      config.logger.info("Inside CompareItoM Message:" + m.toString()
              + "\nInstruction:" + i.toString());
      return (m.getAction().getType() == i.getActionType()
              && m.getNotificationType() == i.getNotificationType());
    }

    /**
     * Send halt to all processes.
     * For the coordinator process send to both participant and coordinator
     * Sleep for sometime and then send resume to all.
     * 
     * @param i
     * @param m
     */
    private void sendHaltToProcess(Instruction i, Message m) {
      m.setSrcType(NodeType.CONTROLLER);
      m.setDest(m.getSrc());
      m.setDestType(m.getSrcType());
      m.setSrc(0);
      m.setSrcType(NodeType.CONTROLLER);
      m.setInstr(i);
      nc.sendMsg(m.getDest(), m);
      config.logger.info("Detected halt");

      /*
       * for (int i1 = 1; i1 < config.numProcesses; i1++) {
       * m.setDest(i1);
       * m.setDestType(NodeType.PARTICIPANT); // Dummy Value doesn't matter
       * nc.sendMsg(i1, m);
       * config.logger.info("HALT Sent " + m.toString() + " to " + i1);
       * if (i1 == currentCoordinatorId) {
       * // Send another message to the coordinator
       * m.setDestType(NodeType.COORDINATOR);
       * nc.sendMsg(i1, m);
       * config.logger.info("HALT Sent " + m.toString() + " to " + i1);
       * }
       * }
       * try {
       * Thread.sleep(60000);
       * } catch (InterruptedException e) {
       * // TODO Auto-generated catch block
       * e.printStackTrace();
       * }
       * // Send resume after some halt.
       * config.logger.info("Sending Resume");
       * i.setInstructionType(InstructionType.RESUME);
       * m.setInstr(i);
       * for (int i1 = 1; i1 < config.numProcesses; i1++) {
       * m.setDest(i1);
       * m.setDestType(NodeType.PARTICIPANT); // Dummy Value doesn't matter
       * nc.sendMsg(i1, m);
       * config.logger.info("RESUME Sent " + m.toString() + " to " + i1);
       * if (i1 == currentCoordinatorId) {
       * // Send another message to the coordinator
       * m.setDestType(NodeType.COORDINATOR);
       * nc.sendMsg(i1, m);
       * config.logger.info("RESUME Sent " + m.toString() + " to " + i1);
       * }
       * }
       */
    }

    /**
     * Send the instruction to a process.
     * Update the sequence number of the controller after sending the message.
     * 
     * @param i
     * @param m
     */
    private void sendInstructionToProcess(Instruction i, Message m) {
      InstructionType iType = i.getInstructionType();
      if (iType == InstructionType.HALT) {
        // Send to all and make sure you send it to both the coordinator
        // and participant thread of the coordinator
        sendHaltToProcess(i, m);
      } else if (iType == InstructionType.KILL) {
        if (isTransactionComplete(m)) {
          decisionReached[procNum] = true;
        }
        m.setInstr(i);
        m.setDest(procNum);
        // Check: What did we decide ??
        m.setDestType(m.getSrcType());
        m.setSrc(0);
        m.setSrcType(NodeType.CONTROLLER);
        nc.sendMsg(procNum, m);
        messageQueue[procNum].clear();
        nc.outSockets[procNum] = null;
        config.logger.info("KILL Sent " + m.toString() + " to " + procNum);
      } else {
        config.logger.log(Level.WARNING, "Can't make out the Instruction type");
      }
      // Move to next instruction
      incrementNextInstructionSequenceNum();
    }

    @Override
    public void run() {
      // Call the process init transaction here
      processes[procNum].initTransaction(currentTransaction, vote);
      // TODO: Send a message to start the transaction.
      // TODO: Check the time u send here
      /*
       * Message startMessage = new Message(0, procNum, NodeType.CONTROLLER,
       * NodeType.PARTICIPANT, System.currentTimeMillis());
       * sendMessageToCoordinatorAndParticipant(startMessage);
       */
      config.logger.info("While entry");
      while (!indiviualInstructionQueue.isEmpty()) {
        config.logger.info("Inst queue for" + procNum);
        Instruction currentInstruction = indiviualInstructionQueue.peek();
        currentInstructionSeqNum = indiviualInstructionQueue.peek().getSeqNo();
        if (checkIfCurrentInstructionRevive(currentInstruction)) {
          // TODO: Call the revive method on the process
          config.logger.info("Detected revive instruction " + procNum);
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          processes[procNum] = new Process(procNum, getCurrentTime(),
                  processesConfigs[procNum]);
          processes[procNum].reviveProcessState(currentTransaction, vote);
          incrementNextInstructionSequenceNum();
          indiviualInstructionQueue.removeFirst();
          continue;
        }
        try {
          Message newMessage = messageQueue[procNum].take();
          config.logger.info(
                  String.format("Controller Consumed message %s from proc %d",
                          newMessage.toString(), procNum));
          // Check if the incoming message indicates whether its a
          // new coordinator then change your Cid.
          checkInstructionAndUpdateCoordinatorId(newMessage);

          if (compareInstructionToMessage(currentInstruction, newMessage)) {
            while (currentInstruction.getSeqNo() != minSeqNumber) {
            }
            config.logger.info("Executing instruction with seq no "
                    + currentInstruction.getSeqNo());
            // Send the instruction to process
            sendInstructionToProcess(currentInstruction, newMessage);
            indiviualInstructionQueue.removeFirst();
          } else {
            // Send Continue
            sendContinueToProcess(newMessage);
          }

        } catch (InterruptedException e) {
          config.logger.log(Level.WARNING,
                  "Interrupted wait on message for proc " + procNum);
        }
        config.logger.info("Cuurent inst being executed by conroller for "
                + procNum + ": " + currentInstruction.toString() + " Seq No:"
                + currentInstructionSeqNum);
        config.logger.info("Remaining instructions for :" + procNum);
        for (Instruction b : indiviualInstructionQueue) {
          config.logger.info(b.toString());
          config.logger.info("=========================\n");
        }
      }
      boolean isTransactionComplete = false;
      while (!haveAllProcessesReachedDecision()) {
        if (!messageQueue[procNum].isEmpty()) {
          try {
            Message m = messageQueue[procNum].take();
            config.logger.info(
                    "Checking if decision reached using :" + m.toString());
            isTransactionComplete = isTransactionComplete(m);
            decisionReached[procNum] = decisionReached[procNum]
                    || isTransactionComplete;
            sendContinueToProcess(m);
          } catch (InterruptedException e) {
            config.logger.log(Level.WARNING,
                    "Interrupted wait on decision message for proc " + procNum);
          }
        }
      }
      config.logger.info("Process " + procNum + " has ended");
    }

    /**
     * Send a continue instruction to a process.
     * 
     * @param newMessage
     */
    private void sendContinueToProcess(Message newMessage) {
      // Only the instruction type matters. All others are dummy values.
      config.logger.info("Sending continue to message " + newMessage.toString()
              + " Proc " + procNum);
      newMessage.setInstr(new Instruction(InstructionType.CONTINUE, "",
              NotificationType.DELIVER, ActionType.ACK, -1, procNum, -1));
      // Check
      newMessage.setDest(newMessage.getSrc());
      newMessage.setDestType(newMessage.getSrcType());
      newMessage.setSrc(0);
      newMessage.setSrcType(NodeType.CONTROLLER);
      newMessage.setState(null);
      nc.sendMsg(procNum, newMessage);
      config.logger.info(
              "CONTINUE Sent " + newMessage.toString() + " to " + procNum);
    }

    /**
     * Check whether the message is a state req before sending from the new
     * coordinator
     * and update your coordinator value.
     * 
     * @param m
     */
    private void checkInstructionAndUpdateCoordinatorId(Message m) {
      if (m.getAction().getType() == ActionType.STATE_REQ
              && m.getNotificationType() == NotificationType.SEND) {
        currentCoordinatorId = m.getSrc();
        config.logger
                .info("Updates the coordinator id to " + currentCoordinatorId);
        config.logger.info("Message for updating id : " + m.toString());
      }
    }

    /**
     * Check if a process has sent a decision message.
     * Use this in deciding whether you have to finish this transaction.
     * TODO: Add logic for coordinator. Match on notification type as well.
     * For coordinator its SEND. All others its RECEIVE.
     * 
     * @param m
     * @return
     */
    private boolean isTransactionComplete(Message m) {
      boolean complete = false;
      boolean decisionTaken = (m.getAction().getType() == ActionType.DECISION);
      if (decisionTaken) {
        if ((m.getSrcType() == NodeType.COORDINATOR
                && m.getNotificationType() == NotificationType.SEND)
                || (m.getSrcType() != NodeType.COORDINATOR && m
                        .getNotificationType() == NotificationType.RECEIVE)) {
          complete = true;
          config.logger.info("Process " + procNum + " has decided "
                  + m.getAction().getType());
        }
      } else {
        config.logger.info("Process " + procNum + " has not decided "
                + m.getAction().getType());
      }
      return complete;
    }

    /**
     * Check whether the current instruction is revive.
     * 
     * @param currentInstruction
     * @return
     */
    private boolean checkIfCurrentInstructionRevive(
            Instruction currentInstruction) {
      return (currentInstruction
              .getInstructionType() == InstructionType.REVIVE);
    }
  }

  public static void main(String[] args) {
    String[] s = { "config_p0.txt", "config_p1.txt", "config_p2.txt",
            "config_p3.txt", "config_p4.txt" };
    Controller controller = new Controller(s);
  }

}
