/**
 * 
 */
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

/**
 * @author av28895
 *
 */
public class SimulationConfig {
	
	
	public SimulationConfig(String simulationConfigFilename) {
		super();
		logger = getLogger();
		instructions = new HashMap<>();
		this.filename = simulationConfigFilename;
	}

	public String getSimulationConfigFilename() {
		return filename;
	}

	public void setSimulationConfigFilename(String simulationConfigFilename) {
		this.filename = simulationConfigFilename;
	}
	
	private Logger getLogger()
	{
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
	
	/*
	 * Parse the given config file and map a sequence of instructions to each process. 
	 * The Process ID which is the key starts from 1. Process ID 0 corresponds to the Controller process
	 * which would read these instructions. 
	 * */
	public void processInstructions()
	{	
		
			try(BufferedReader br = new BufferedReader(new FileReader(filename))) {
			
			/*
			 * L1 : Extract Transaction*/
		    String line = br.readLine();
		    processTransaction(line);
		    
		    /*
			 * L2 : Extract processes which should vote no for the transaction*/
		    line = br.readLine();
		    processNoVoteProcesses(line);
		    
		    /*
		     * All the remaining lines correspond to the action to be taken on each process*/
		    line = br.readLine();
		    while (line != null) {
		    	processPerProcessInstructions(line);
		        line = br.readLine();
		    }
		    
		    
		    // Remoove this after testing
		    logInstructions();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Process instructions meant for each process. Ex format 
	 * 1: KILL after sending PRECOMMIT n*/
	private void processPerProcessInstructions(String line) {
		String[] split = line.split(":");
		if(split.length == 2)
		{
			int pid = Integer.parseInt(split[0]); // split[0] corresponds to the pid for which the insts are meant
			String[] perProcessInstructions = split[1].trim().split(",");
			if(!instructions.containsKey(pid))
			{
				instructions.put(pid, new ArrayList<String>(Arrays.asList(perProcessInstructions)));
			}
			else
				logger.log(Level.WARNING, "Already found instructions for "+ pid+ " and ignoring new instructions.");
			
		}
	}

	/*
	 * Process a transaction add, remove or edit*/
	private void processTransaction(String line)
	{
		String[] splitParts = line.split(",");
		int len = splitParts.length;
		if(len == 3 && splitParts[0].trim().toLowerCase().startsWith("a"))
		{
			
				String songName = splitParts[1].trim();
				String songUrl = splitParts[2].trim();
				logger.info("Add "+songName+ " with URL : "+ songUrl);
				
				
		}
		else if(len == 2 && splitParts[0].trim().toLowerCase().startsWith("r"))
		{
			String songName = splitParts[1].trim();
			logger.info("Remove "+songName);
		}
		else if(len == 4 && splitParts[0].trim().toLowerCase().startsWith("e"))
		{
			String songNameNew = splitParts[1].trim();
			String songUrlNew = splitParts[2].trim();
			String songNameOld = splitParts[3].trim();
			logger.info("Edit "+songNameNew+ " with URL : "+ songUrlNew+ " from "+songNameOld);
		}
		else
		{
			logger.log(Level.SEVERE, "Could not find transaction");
		}
	}

	/*
	 * Extract all processes which have to vote no for the transaction 
	 * Vote No : 1,2,3
	 * */
	private ArrayList<Integer> processNoVoteProcesses(String line) {
		String[] splits = line.split(":");
		ArrayList<Integer> pidsNo = new ArrayList<>();
		if(splits.length == 2)
		{
			String[] pid = splits[1].trim().split(",");
			for (String p : pid) {
				pidsNo.add(Integer.parseInt(p));
			}
		}
		for (Integer integer : pidsNo) {
			logger.info("Process "+integer+" votes no");
		}
		return pidsNo;
	}
	
	private void logInstructions()
	{
		for (Integer element : instructions.keySet()) {
			logger.info("Process "+element +" instructions");
			for (String inst : instructions.get(element)) {
				logger.info(inst);
			}
		}
	}

	private String filename;
	private HashMap<Integer, ArrayList<String>> instructions; 
	private Logger logger;
	
}
