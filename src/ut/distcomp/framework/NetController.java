/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

/**
* The sendMsg method has been modified by Navid Yaghmazadeh to fix a bug regarding to send a message to a reconnected socket.
*/

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import dc.Message;

/**
 * Public interface for managing network connections.
 * You should only need to use this and the Config class.
 * @author ilevy
 *
 */
public class NetController {
	private final Config config;
	private final IncomingSock[] inSockets;
	private final OutgoingSock[] outSockets;
	private final ListenServer listener;
	private BlockingQueue<Message> commonQueue;
	private BlockingQueue<Message> controllerQueue;
	private BlockingQueue<Message> heartbeatQueue;
	private BlockingQueue<Message> coordinatorQueue;
	private BlockingQueue<Message> coordinatorControllerQueue;
	private BlockingQueue<Message>[] concurrentControllerQueues;
	
	/**
	 * Used by non-controller process. 
	 * @param coordinatorControllerQueue 
	 */
	public NetController(Config config, 
			BlockingQueue<Message> controllerQueue, 
			BlockingQueue<Message> commonQueue, 
			BlockingQueue<Message> heartbeatQueue,
			BlockingQueue<Message> coordinatorQueue, 
			BlockingQueue<Message> coordinatorControllerQueue){
		this.config = config;
		this.commonQueue = commonQueue;
		this.controllerQueue = controllerQueue;
		this.heartbeatQueue = heartbeatQueue;
		this.coordinatorQueue = coordinatorQueue;
		this.coordinatorControllerQueue = coordinatorControllerQueue;
		inSockets = new IncomingSock[config.numProcesses];
		listener = new ListenServer(config, 
				inSockets, 
				commonQueue, 
				controllerQueue, 
				heartbeatQueue, 
				coordinatorQueue, 
				coordinatorControllerQueue);
		outSockets = new OutgoingSock[config.numProcesses];
		listener.start();
	}
	
	public NetController(Config config,
			BlockingQueue<Message>[] queues)
	{
		this.config = config;
		this.concurrentControllerQueues = queues;
		inSockets = new IncomingSock[config.numProcesses];
		listener = new ListenServer(config, 
				inSockets, 
				queues);
		outSockets = new OutgoingSock[config.numProcesses];
		listener.start();
	}
	
	
	// Establish outgoing connection to a process
	private synchronized void initOutgoingConn(int proc) throws IOException {
		if (outSockets[proc] != null)
			throw new IllegalStateException("proc " + proc + " not null");
		Socket bareSocket = new Socket(config.addresses[proc], config.ports[proc]);
		ObjectOutputStream outputStream = new ObjectOutputStream(bareSocket.getOutputStream());
		// Send your process ID to the server to which you just initiated the connection.
		outputStream.writeInt(config.procNum);
		outSockets[proc] = new OutgoingSock(bareSocket, outputStream);
		config.logger.info(String.format("Server %d: Socket to %d established", 
				config.procNum, proc));
	}
	
	/**
	 * Send a msg to another process.  This will establish a socket if one is not
	 * created yet. Will fail if recipient has not set up their own NetController 
	 * (and its associated serverSocket).
	 * @param process int specified in the config file - 0 based
	 * @param msg Do not use the "&" character.  This is hardcoded as a message 
	 *        separator. Sends as ASCII. Include the sending server ID in the
	 *        message.
	 * @return bool indicating success.
	 */
	public synchronized boolean sendMsg(int process, Message msg) {
		try {
			if (outSockets[process] == null)
				initOutgoingConn(process);
			outSockets[process].sendMsg(msg);
			if(!msg.isHeartbeatMessage()){
				config.logger.info("Sent "+msg.toString()+" to "+process);
			}
		} catch (IOException e) { 
			if (outSockets[process] != null) {
				outSockets[process].cleanShutdown();
				outSockets[process] = null;
				try{
					initOutgoingConn(process);
                        		outSockets[process].sendMsg(msg);	
				} catch(IOException e1){
					if (outSockets[process] != null) {
						outSockets[process].cleanShutdown();
	                	outSockets[process] = null;
					}
					config.logger.info(String.format("Server %d: Msg to %d failed. Message : %s",
                        config.procNum, process, msg.toString()));
        		    config.logger.log(Level.FINE, 
        		            String.format("Server %d: Socket to %d error",
                        config.procNum, process), e);
                    return false;
				}
				return true;
			}
			config.logger.info(String.format("Server %d: Msg to %d failed.", 
				config.procNum, process));
			config.logger.log(Level.FINE, String.format("Server %d: Socket to %d error", 
				config.procNum, process), e);
			return false;
		}
		return true;
	}
	
	/**
	 * Return a list of msgs received on established incoming sockets
	 * @return list of messages sorted by socket, in FIFO order. *not sorted by 
	 *         time received*
	 */
	public synchronized List<Message> getReceivedMsgs() {
		List<Message> objs = new ArrayList<Message>();
		synchronized(inSockets) {
			for (int i = 0; i < inSockets.length; i++) {
				if(inSockets[i] != null)
				{
					/*i*/
					config.logger.log(Level.INFO, 
							"No OF messages :" + concurrentControllerQueues[i].size() + " from "+i);
					
					
				}
			/*		
			ListIterator<IncomingSock> iter  = inSockets.listIterator();
			while (iter.hasNext()) {
				IncomingSock curSock = iter.next();
				try {
					objs.addAll(curSock.getMsgs());
				} catch (Exception e) {
					config.logger.log(Level.INFO, 
							"Server " + config.procNum + " received bad data on a socket", e);
					curSock.cleanShutdown();
					iter.remove();
				}
			}*/
		}}
		
		return objs;
	}
	
	/**
	 * Shuts down threads and sockets.
	 */
	public synchronized void shutdown() {
		listener.cleanShutdown();
        if(inSockets != null) {
		    for (IncomingSock sock : inSockets)
			    if(sock != null)
                    sock.cleanShutdown();
        }
		if(outSockets != null) {
            for (OutgoingSock sock : outSockets)
			    if(sock != null)
                    sock.cleanShutdown();
        }
		
	}

	public Config getConfig() {
		return config;
	}

	public IncomingSock[] getInSockets() {
		return inSockets;
	}

	public OutgoingSock[] getOutSockets() {
		return outSockets;
	}

	public ListenServer getListener() {
		return listener;
	}
}
