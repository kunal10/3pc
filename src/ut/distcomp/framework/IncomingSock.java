/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import dc.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IncomingSock extends Thread {
	Socket sock;
	ObjectInputStream in;
	private volatile boolean shutdownSet;
	
	/**
	 * Used by incoming thread of both controller and other processes. 
	 * If the controller initiates the incoming connection this queue will be set to the controller queue.
	 * Else it is the common queue.
	 */
	private BlockingQueue<Message> queue;
	
	/**
	 * Used only by non-controller processes to send heartbeat messages. 
	 */
	private BlockingQueue<Message> heartbeatQueue;
	int bytesLastChecked = 0;
	
	protected IncomingSock(Socket sock, BlockingQueue<Message> controllerQueue) throws IOException {
		this.sock = sock;
		in = new ObjectInputStream(sock.getInputStream());
		sock.shutdownOutput();
		this.queue = controllerQueue;
	}
	
	protected IncomingSock(Socket sock, BlockingQueue<Message> queue, BlockingQueue<Message> heartbeatQueue) throws IOException{
		this.sock = sock;
		in = new ObjectInputStream(sock.getInputStream());
		this.heartbeatQueue = heartbeatQueue;
		sock.shutdownOutput();
		this.queue = queue;
	}
	
	protected List<Message> getMsgs() {
		List<Message> msgs = new ArrayList<Message>();
		Message tmp;
		while((tmp = queue.poll()) != null)
			msgs.add(tmp);
		return msgs;
	}
	
	public void run() {
		while (!shutdownSet) {
			try {	
				Message msg = (Message) in.readObject();
				// Check if action is null and add that to heartbeat queue.
				if(msg.getAction() == null){
					heartbeatQueue.add(msg);
				}
				else{
					queue.add(msg);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		shutdown();
	}
	
	public void cleanShutdown() {
		shutdownSet = true;
	}
	
	protected void shutdown() {
		try { in.close(); } catch (IOException e) {}
		
		try { 
			sock.shutdownInput();
			sock.close(); }			
		catch (IOException e) {}
	}
}
