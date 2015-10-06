/**
 * 
 */
package dc;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import ut.distcomp.framework.Config;

/**
 * @author av28895
 *
 */
public class Controller {
	
	/**
	 * Has the following components:
	 * List of configs for each process or there is a common config. 
	 * Read from that make an array of config classes which indiviual proc ids.  
	 * Then create 5 Sender threads which implement our dumb algorithm. 
	 * There is an array of 5 Instruction queues.
	 * Array of 5 process messages queues. 
	 * Netcontroller which assigns each incoming thread its own queue. 
	 * I/P with a given Simulation Config parse that and you get a list of transactions 
	 * For each transaction :
	 * Take the list of instruction sequence and go over it add it to the indiviual queues.
	 * setVotes and transactions for all the processes in your array by using the startTransaction method.  
	 */
	
	/**
	 * 
	 * @param configFiles Array of filenames of all the configs to be used. 
	 * 0th Element corresponds to the config for the controller.
	 */
	public Controller(String[] configFiles)
	{
		try {
			
			// Read configs for each process.
			config = new Config(configFiles[0]);
			processesConfigs = new Config[config.numProcesses];
			for(int i = 1; i < config.numProcesses; i++){
				processesConfigs[i] = new Config(configFiles[i]);
			}
			
			// Read the simulation Config.
			SimulationConfig sc = new SimulationConfig("SimulationConfig1");
			
			//Process the instruction list.
			
			
			//Set min inst value to 0.
			minSeqNumber = 0;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	 * Array of queues. Each array corresponding to the instruction queue of one process. Ignore 0th element. 
	 */
	private LinkedBlockingQueue<Instruction>[] instructionQueue; 
	
	/**
	 * Queues for storing the incoming messages one for each process. 
	 * These queues should be passed to the net controller to initialize for the incoming sock.
	 */
	private LinkedBlockingQueue<Message>[] messageQueue;
	
	/**
	 * Seq number of the next instruction to be executed.
	 */
	private volatile int minSeqNumber;
	
	public static void main(String[] args){
		String[] s = {"config_p0.txt", "config_p1.txt", "config_p2.txt", "config_p3.txt", "config_p4.txt"};
		Controller controller = new Controller(s);
	}
	

}
