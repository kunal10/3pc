package dc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import dc.Transaction.TransactionType;

/**
 * This class encapsulates the config used by controller to simulate a give set
 * of transactions.
 * 
 * NOTE : Controller should ensure that following are satisfied before starting
 *        a new transaction.
 * 1) Previous transaction is complete.
 * 2) All the processes are **ALIVE**. This is needed so that all processes are
 *    involved in the new transaction.
 */
public class SimulationConfig {
	
	
	public SimulationConfig(String simulationConfigFilename) {
		super();
		logger = getLogger();
		allTransactions = new ArrayList<ConfigElement>();
		this.filename = simulationConfigFilename;
	}

	public String getSimulationConfigFilename() {
		return filename;
	}

	public void setSimulationConfigFilename(String filename) {
		this.filename = filename;
	}
	
	private Logger getLogger() {
		Logger logger = Logger.getLogger("SimulationConfig");
		FileHandler fileHandler = null;
		try {
			fileHandler = new FileHandler("CommonLog.txt");
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		logger.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();  
        fileHandler.setFormatter(formatter);  
        return logger;
	}
	
	/**
	 * Parse the given config file and map a sequence of instructions to each 
	 * process. The Process ID which is the key starts from 1. Process ID 0 
	 * corresponds to the Controller process which would read these instructions. 
	 */
	public void processInstructions() {	
	    try(BufferedReader br = new BufferedReader(new FileReader(filename))) {
	    	String line = "";
	    	do{
	    		ConfigElement elem = new ConfigElement();
	    		// L1 : Extract Transaction.
	    		line = br.readLine();
	    		elem.setTransaction(processTransaction(line));
		      
	    		// L2 : Extract processes which should vote no for the transaction.
	    		line = br.readLine();
	    		elem.setNoVotes(parseVotes(line));
		      
	    		// All the remaining lines correspond to the action to be taken on each 
	    		// process.
	    		line = br.readLine();
	    		while (line != null && line != "\n" && !line.isEmpty()) {
	    			Instruction i = parsePerProcessInstructions(line);
	    			if(i !=null){
	    				elem.instructions.add(i);
	    			}	
	    			line = br.readLine();
	    		}
	    		// Add a single transaction to the list.
	    		allTransactions.add(elem);
	    	}while(line != null);
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Process instructions meant for each process. Ex format 
	 * 1: KILL after sending PRECOMMIT n
	 * @return 
	 */
	private Instruction parsePerProcessInstructions(String line) {
		Instruction i = Instruction.parseInstruction(line);
		return i;
	}

	/**
	 * Process a transaction add, remove or edit
	 * @return 
	 */
	private Transaction processTransaction(String line) {
		String[] splitParts = line.split(",");
		int len = splitParts.length;
		if(len == 3) {
			String songName = splitParts[1].trim();
			String songUrl = splitParts[2].trim();
			logger.info("Add "+songName+ " with URL : "+ songUrl);
			return new Transaction(TransactionType.ADD, null, new Song(songName, songUrl));
		} else if(len == 2) {
			String songName = splitParts[1].trim();
			logger.info("Remove "+songName);
			return new Transaction(TransactionType.DELETE, new Song(songName, ""), null);
		} else if(len == 4) {
			String songNameNew = splitParts[1].trim();
			String songUrlNew = splitParts[2].trim();
			String songNameOld = splitParts[3].trim();
			logger.info("Edit "+songNameNew+ " with URL : "+ songUrlNew+ " from "+songNameOld);
			return new Transaction(TransactionType.EDIT, new Song(songNameOld, ""), new Song(songNameNew, songUrlNew));
		} else {
			logger.log(Level.SEVERE, "Could not parse transaction");
			return null;
		}
	}

	/**
	 * Extract all processes which have to vote no for the transaction 
	 * Vote No : 1,2,3
	 */
	private ArrayList<Integer> parseVotes(String line) {
		String[] splits = line.split(":");
		ArrayList<Integer> pidsNo = new ArrayList<>();
		if(splits.length == 2) {
			String[] pid = splits[1].trim().split(",");
			for (String p : pid) {
				pidsNo.add(Integer.parseInt(p));
			}
		}
		for (Integer integer : pidsNo) {
			logger.info("Process " + integer + " votes no");
		}
		return pidsNo;
	}
	
	/*private void logInstructions() {
		for (Integer element : instructions.keySet()) {
			logger.info("Process " + element + " instructions");
			for (String inst : instructions.get(element)) {
				logger.info(inst);
			}
		}
	}*/

	public ArrayList<ConfigElement> getAllTransactions() {
		return allTransactions;
	}

	public void setAllTransactions(ArrayList<ConfigElement> allTransactions) {
		this.allTransactions = allTransactions;
	}

	private String filename;
	private ArrayList<ConfigElement> allTransactions; 
	private Logger logger;
	
	
}
