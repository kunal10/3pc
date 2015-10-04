/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.BufferedInputStream;
import dc.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IncomingSock extends Thread {
	final static String MSG_SEP = "&";
	Socket sock;
	//InputStream in;
	ObjectInputStream in;
	private volatile boolean shutdownSet;
	private final ConcurrentLinkedQueue<Message> queue;
	int bytesLastChecked = 0;
	//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
	protected IncomingSock(Socket sock, ConcurrentLinkedQueue<Message> queue) throws IOException {
		this.sock = sock;
		in = new ObjectInputStream(sock.getInputStream());
		//in = sock.getInputStream();
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
				int avail = in.available();
				Message msg = (Message) in.readObject();
				queue.add(msg);
				/*if (avail == bytesLastChecked) {
					sleep(10);
				} else {
					in.mark(avail);
					byte[] data = new byte[avail];
					in.read(data);
					String dataStr = new String(data);
					int curPtr = 0;
					int curIdx;
					while ((curIdx = dataStr.indexOf(MSG_SEP, curPtr)) != -1) {
						queue.offer(dataStr.substring(curPtr, curIdx));
						curPtr = curIdx + 1;
					}
					in.reset();
					in.skip(curPtr);
					bytesLastChecked = avail - curPtr;
				}*/
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
