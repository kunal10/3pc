/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Config {

	

	/**
	 * Loads config from a file.  Optionally puts in 'procNum' if in file.
	 * See sample file for syntax
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Config(String filename) throws FileNotFoundException, IOException {
		
		Properties prop = new Properties();
		prop.load(new FileInputStream(filename));
		numProcesses = loadInt(prop,"NumProcesses");
		addresses = new InetAddress[numProcesses];
		// Add a file handler to the logger to output the logs to the file 
		// specified in the process config
		logger = Logger.getLogger("NetFramework"+prop.getProperty("procNum"));
		FileHandler fileHandler = new FileHandler(prop.getProperty("logfile"));
		logger.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();  
        fileHandler.setFormatter(formatter);  
		ports = new int[numProcesses];
		for (int i=0; i < numProcesses; i++) {
			ports[i] = loadInt(prop, "port" + i);
			addresses[i] = InetAddress.getByName(prop.getProperty("host" + i).trim());
		}
		simulationConfig = prop.getProperty("SimulationConfig");
		DTFilename = prop.getProperty("DTFilename");
		if (prop.getProperty("procNum") != null) {
			procNum = loadInt(prop,"procNum");
		} else {
			logger.info("procNum not loaded from file");
		}
	}
	
	private int loadInt(Properties prop, String s) {
		return Integer.parseInt(prop.getProperty(s.trim()));
	}
	
	/**
	 * Default constructor for those who want to populate config file manually.
	 */
	public Config() {
	}

	/**
	 * Array of addresses of other hosts.  All hosts should have identical info
	 * here.
	 */
	public InetAddress[] addresses;
	

	/**
	 * Array of listening port of other hosts. All hosts should have identical
	 * info here.
	 */
	public int[] ports;
	
	/**
	 * Total number of hosts
	 */
	public int numProcesses;
	
	/**
	 * This hosts number (should correspond to array above). Each host should have
	 * a different number.
	 */
	public int procNum;
	
	/**
	 * Logger.  Mainly used for console printing, though be diverted to a file.
	 * Verbosity can be restricted by raising level to WARN.
	 */
	public Logger logger;
	
	/**
	 * Simulation config to be simulated. Used by controller.
	 */
	public String simulationConfig;
	
	/**
	 * Name of the DT log to be created.
	 */
	public String DTFilename;
}
