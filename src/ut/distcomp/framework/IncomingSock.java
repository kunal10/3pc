/**
 * This code may be modified and used for non-commercial
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import dc.Message;
import dc.Message.NodeType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IncomingSock extends Thread {
  Socket sock;
  ObjectInputStream in;
  private volatile boolean shutdownSet;

  /**
   * Used by incoming thread of both controller and other processes.
   * If the controller initiates the incoming connection this queue will be set
   * to the controller queue.
   * Else it is the common queue.
   */
  private BlockingQueue<Message> queue;

  /**
   * Used only by non-controller processes to send heartbeat messages.
   */
  private BlockingQueue<Message> heartbeatQueue;
  /**
   * Used by incoming thread of both controller and other processes.
   * If the controller initiates the incoming connection this queue will be set
   * to the controller coordinator queue.
   */
  private BlockingQueue<Message> coordinatorQueue;
  int bytesLastChecked = 0;

  protected IncomingSock(Socket sock, BlockingQueue<Message> controllerQueue,
          BlockingQueue<Message> coordinatorControllerQueue,
          ObjectInputStream inputStream) throws IOException {
    this.sock = sock;
    in = inputStream;
    sock.shutdownOutput();
    this.queue = controllerQueue;
    this.coordinatorQueue = coordinatorControllerQueue;
  }

  protected IncomingSock(Socket sock, BlockingQueue<Message> queue,
          BlockingQueue<Message> heartbeatQueue,
          BlockingQueue<Message> coordinatorQueue,
          ObjectInputStream inputStream) throws IOException {
    this.sock = sock;
    in = inputStream;
    this.heartbeatQueue = heartbeatQueue;
    this.coordinatorQueue = coordinatorQueue;
    sock.shutdownOutput();
    this.queue = queue;
  }

  protected IncomingSock(Socket incomingSocket,
          BlockingQueue<Message> blockingQueue, ObjectInputStream inputStream)
                  throws IOException {
    this.sock = incomingSocket;
    in = inputStream;
    incomingSocket.shutdownOutput();
    this.queue = blockingQueue;
  }

  protected List<Message> getMsgs() {
    List<Message> msgs = new ArrayList<Message>();
    Message tmp;
    while ((tmp = queue.poll()) != null)
      msgs.add(tmp);
    return msgs;
  }

  public void run() {
    while (!shutdownSet) {
      try {
        Message msg = (Message) in.readObject();
        // Check if action is null and add that to heartbeat queue.
        if (msg.isHeartbeatMessage()) {
          if (msg.getSrc() == 0 && msg.getDest() == 1) {
            System.out.println("SEVERE !! Controller sent HB msg");
          }
          heartbeatQueue.add(msg);
        } else {
          if (msg.getDestType() == NodeType.COORDINATOR) {
            System.out.println("Coordinator Queue size before add: "
                    + coordinatorQueue.size());
            coordinatorQueue.add(msg);
            System.out.println("Coordinator Queue size after add: "
                    + coordinatorQueue.size());
            System.out.println(
                    "\n\nCoordinator Received Controller msg" + msg.toString());
          } else {
            queue.add(msg);
            System.out
                    .println("Node type in participant :" + msg.getDestType());
            System.out.println(
                    "\n\nParticipant Received Controller msg" + msg.toString());
          }

        }
      } catch (IOException e) {
        try {
          in.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        // e.printStackTrace();
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        try {
          in.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        // e.printStackTrace();
      }
    }
    shutdown();
  }

  public void cleanShutdown() {
    shutdownSet = true;
  }

  protected void shutdown() {
    try {
      in.close();
    } catch (IOException e) {
    }

    try {
      sock.shutdownInput();
      sock.close();
    } catch (IOException e) {
    }
  }
}
