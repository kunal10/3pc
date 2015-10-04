/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import dc.Message;

public class OutgoingSock {
	final static byte[] MSG_SEP = "&".getBytes();
	Socket sock;
	ObjectOutputStream out;
	
	protected OutgoingSock(Socket sock) throws IOException {
		this.sock = sock;
		
		out = new ObjectOutputStream(sock.getOutputStream());
		sock.shutdownInput();
	}
	
	/** 
	 * Do not use '&' character.  This is a hardcoded separator
	 * @param msg
	 * @throws IOException 
	 */
	protected synchronized void sendMsg(Message msg) throws IOException {
		out.writeObject(msg);
		out.flush();
	}
	
	public synchronized void cleanShutdown() {
		try { out.close(); } 
		catch (IOException e) {}

		try { 
			sock.shutdownOutput();
			sock.close(); 
		} catch (IOException e) {}
	}
}
