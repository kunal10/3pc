/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

import dc.Message;

public class ListenServer extends Thread {

	public volatile boolean killSig = false;
	final int port;
	final int procNum;
	final List<IncomingSock> socketList;
	final Config conf;
	final ServerSocket serverSock;
	ConcurrentLinkedQueue<Message> commonQueue;
	ConcurrentLinkedQueue<Message> controllerQueue;

	protected ListenServer(Config conf, List<IncomingSock> sockets, ConcurrentLinkedQueue<Message> commonQueue, ConcurrentLinkedQueue<Message> controllerQueue) {
		this.conf = conf;
		this.socketList = sockets;
		this.commonQueue = commonQueue;
		this.controllerQueue = controllerQueue;
		procNum = conf.procNum;
		port = conf.ports[procNum];
		try {
			serverSock = new ServerSocket(port);
			conf.logger.info(String.format(
					"Server %d: Server connection established", procNum));
		} catch (IOException e) {
			String errStr = String.format(
					"Server %d: [FATAL] Can't open server port %d", procNum,
					port);
			conf.logger.log(Level.SEVERE, errStr);
			throw new Error(errStr);
		}
	}

	public void run() {
		while (!killSig) {
			try {
				Socket incomingSocket = serverSock.accept();
				String incomingProcId = incomingSocket.getInetAddress().getHostName();
				IncomingSock incomingSock = null;
				if(incomingProcId == "0")
					incomingSock = new IncomingSock(serverSock.accept(), controllerQueue);
				else
					incomingSock = new IncomingSock(serverSock.accept(), commonQueue);
				socketList.add(incomingSock);
				incomingSock.start();
				conf.logger.fine(String.format(
						"Server %d: New incoming connection accepted from %s",
						procNum, incomingSock.sock.getInetAddress()
								.getHostName()));
			} catch (IOException e) {
				if (!killSig) {
					conf.logger.log(Level.INFO, String.format(
							"Server %d: Incoming socket failed", procNum), e);
				}
			}
		}
	}

	protected void cleanShutdown() {
		killSig = true;
		try {
			serverSock.close();
		} catch (IOException e) {
			conf.logger.log(Level.INFO,String.format(
					"Server %d: Error closing server socket", procNum), e);
		}
	}
}
