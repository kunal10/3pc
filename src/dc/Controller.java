/**
 * 
 */
package dc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import dc.Instruction.InstructionType;
import dc.Message.NodeType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

/**
 * @author av28895
 *
 */
public class Controller {
	
	/**
	 * Has the following components:
	 * List of configs for each process or there is a common config.- Done 
	 * Read from that make an array of config classes which indiviual proc ids. - Done  
	 * Array of 5 process messages queues. - Done
	 * Netcontroller which assigns each incoming thread its own queue.- Done 
	 * I/P with a given Simulation Config parse that and you get a list of transactions- Done 
	 * For each transaction :
	 * Take the list of instruction sequence and go over it add it to the indiviual queues.
	 * setVotes and transactions for all the processes in your array by using the startTransaction method.
	 * Then create 5 Sender threads which implement our dumb algorithm. 
	 * There is an array of 5 Instruction queues.  
	 */
	
	/**
	 * 
	 * @param configFiles Array of filenames of all the configs to be used. 
	 * 0th Element corresponds to the config for the controller.
	 */
	public Controller(String[] configFiles)
	{
		try {
			
			// Read configs for each process and initialize each process.
			config = new Config(configFiles[0]);
			processesConfigs = new Config[config.numProcesses];
			processes = new Process[config.numProcesses];
			instructionQueue = new LinkedList[config.numProcesses];
			for(int i = 1; i < config.numProcesses; i++){
				processesConfigs[i] = new Config(configFiles[i]);
				processes[i] = new Process(processesConfigs[i].procNum, getCurrentTime(), processesConfigs[i]);
				instructionQueue[i] = new LinkedList<Instruction>();
			}
			
			// Read the simulation Config.
			SimulationConfig sc = new SimulationConfig("SimulationConfig1");
			sc.processInstructions();
			
			// Initialize the message queues for all the incoming processes. 
			messageQueue = new LinkedBlockingQueue[config.numProcesses];
			for(int i = 0; i < messageQueue.length; i++){
				messageQueue[i] = new LinkedBlockingQueue<Message>(); 
			}
			
			config.logger.info("Initialized message queues");
			
			// Initialize the communication object 
			nc = new NetController(config, messageQueue);
			config.logger.info("Initialized net contorller for the controller");
			
			
			//Set min inst value to 0.
			minSeqNumber = 0;
			
			//Process the instruction list.
			for (ConfigElement transaction : sc.getTransactionList()) {
				
				// Set the current transaction to be executed.
				currentTransaction = transaction.getTransaction();
				config.logger.info("Current transaction :"+currentTransaction.toString());
				
				/* process the transaction into n process inst lists
				 * start n sender threads*/
				for (int i = 1; i < instructionQueue.length; i++) {
					instructionQueue[i].clear();
				}
				splitTransactionsIntoIndiviualLists(transaction.instructions);
				Thread[] threads = new SendHandler[config.numProcesses];
				for(int i = 1; i < threads.length; i++){
					threads[i] = new SendHandler(instructionQueue[i], i, !(transaction.getNoVotes().contains(i)));
					threads[i].start();
				}
				for(int i = 1; i < threads.length; i++){
					try {
						threads[i].join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Look at all the instructions specified as a part of simulation config. 
	 * Split them into each process instruction list.
	 * @param instructions
	 */
	private void splitTransactionsIntoIndiviualLists(
			ArrayList<Instruction> instructions) {
		for (Instruction instruction : instructions) {
			int pId = instruction.getpId();
			instructionQueue[pId].add(instruction);
			config.logger.info(String.format("Adding %d to %d queue",instruction.getSeqNo(), pId));
		}
	}
	
	private void incrementNextInstructionSequenceNum() {
		++ minSeqNumber;
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
	 * All the processes in the simulation to which the controller has to communicate to. 
	 */
	private Process[] processes;
	
	/**
	 * Array of queues. Each array corresponding to the instruction queue of one process. Ignore 0th element. 
	 */
	private LinkedList<Instruction>[] instructionQueue; 
	
	/**
	 * Queues for storing the incoming messages one for each process. 
	 * These queues should be passed to the net controller to initialize for the incoming sock.
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
	 * Array capturing whether a process has reached a decision. 
	 * This can be made out from the instruction sent by a process. 
	 * Based on this move on to the next transaction.  
	 */
	private boolean[] decisionReached;
	
	private long getCurrentTime()
	{
		return System.currentTimeMillis() + 3000;
	}
	
	private class SendHandler extends Thread{

		private LinkedList<Instruction> indiviualInstructionQueue;
		private int procNum;
		// 0 means no and 1 means yes.
		private boolean vote;
		private int currentInstructionSeqNum;
		
		public SendHandler(LinkedList<Instruction> instructionQueue, int procNum, boolean vote) {
			// TODO Auto-generated constructor stub
			this.indiviualInstructionQueue = instructionQueue; 
			this.procNum = procNum;
			this.vote = vote;
		}
		
		/**
		 * Send a message to both participant and coordinator thread of the current coordinator process.
		 * @param m
		 */
		private void sendMessageToCoordinatorAndParticipant(Message m){
			nc.sendMsg(procNum, m);	
			if(procNum == currentCoordinatorId){
				m.setDestType(NodeType.COORDINATOR);
				nc.sendMsg(procNum, m);
			}	
		}
		
		private boolean compareInstructionToMessage(Instruction i, Message m){
		
			return false;
		}
		
		private void sendInstructionToProcess(Instruction i, Message m){
			InstructionType iType = i.getInstructionType();
			if(iType == InstructionType.HALT){
				// Send to all and make sure you send it to both the coordinator and participant thread of the coordinator
			}
			else if(iType == InstructionType.KILL){
				
			}
			else{
				config.logger.log(Level.WARNING, "Can't make out the Instruction type");
			}
		}
		
		
		
		@Override
		public void run() {
			// Call the process init transaction here 
			processes[procNum].initNextTransaction(currentTransaction, vote);
			// Send a message to start the transaction.
			// Check the time u send here 
			Message startMessage = new Message(0, procNum, NodeType.CONTROLLER, NodeType.PARTICIPANT, System.currentTimeMillis());
			sendMessageToCoordinatorAndParticipant(startMessage);
			for (Instruction currentInstruction : indiviualInstructionQueue) {
				currentInstructionSeqNum = indiviualInstructionQueue.peek().getSeqNo();
				try {
					Message newMessage = messageQueue[procNum].take();
					// Check if the incoming message indicates whether its a new coordinator then change your Cid.
					if(checkIfCurrentInstructionRevive(currentInstruction)){
						//Call the revive method on the process
					}
					else if(compareInstructionToMessage(currentInstruction, newMessage)){
						if(currentInstruction.getSeqNo() == minSeqNumber){
							sendInstructionToProcess(currentInstruction, newMessage); 
							incrementNextInstructionSequenceNum();
						}
						while(currentInstruction.getSeqNo() == minSeqNumber){}
						sendInstructionToProcess(currentInstruction, newMessage);
						incrementNextInstructionSequenceNum();
					}
					else{
						// Send Continue
					}
						
						
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				config.logger.info("Cuurent inst being executed by conroller for "+procNum + ": "+currentInstruction.toString()+" Seq No:"+currentInstructionSeqNum);
			}
			// Wait for any other messages.
		}

		private boolean checkIfCurrentInstructionRevive(
				Instruction currentInstruction) {
			// TODO Auto-generated method stub
			return false;
		}

		private boolean checkIfCurrentInstructionHalt(
				Instruction currentInstruction) {
			// TODO Auto-generated method stub
			return false;
		}
	}
	
	
	public static void main(String[] args){
		String[] s = {"config_p0.txt", "config_p1.txt", "config_p2.txt", "config_p3.txt", "config_p4.txt"};
		Controller controller = new Controller(s);
	}
	

}
