package dc;

import java.io.Serializable;

/**
 * Class which captures messages exchanged in the network. 
 */
public class Message implements Serializable  {
  /**
   * Nodes of message are classified on the basis of type of source and
   * destination. Currently we have following kinds of nodes:
   * 
   * 1) CONTROLLER : Controller which micro manages the simulation of protocol.
   * 2) COORDINATOR : Coordinator of the protocol at the time message is sent.
   * 3) PARTICIPANT : Process node participating in the protocol.
   * 4) NON_PARTICIPANT : Process node which can no longer participate in the
   *                      protocol.
   */
  public enum NodeType { 
    CONTROLLER, COORDINATOR, PARTICIPANT, NON_PARTICIPANT
  };
  
  /**
   * Enum for notifying the controller about next action(corresponds to the 
   * action in this message) of the process. 
   */
  public enum NotificationType { SEND, RECEIVE, DELIVER };
  public Message(int src, int dest, NodeType srcType, NodeType destType, 
                 long time) {
    this.src = src;
    this.dest = dest;
    this.srcType = srcType;
    this.destType = destType;
    this.time = time;
  }
  
  /*Should be used only by non-heartbeat messages. State is null and Action is non null*/
  public Message(int src, int dest, NodeType srcType, NodeType destType,
                 Action action, long time) {
    this(src, dest, srcType, destType, time);
    this.action = new Action(action);
  }
  
  /*Should only be used by heartbeat messages*/
  public Message(int src, int dest, NodeType srcType, NodeType destType, 
                 State state, long time) {
    this(src, dest, srcType, destType, time);
    this.state = new State(state);
  }
  
  public int getSrc() {
    return src;
  }
  public int getDest() {
    return dest;
  }
  public NodeType getSrcType() {
    return srcType;
  }
  public NodeType getDestType() {
    return destType;
  }
  public Action getAction() {
    return action;
  }
  public State getState() {
    return state;
  }
  public long getTime() {
    return time;
  }
  public NotificationType getNotificationType() {
    return notificationType;
  }
  
  private int src;
  private int dest;
  private NodeType srcType;
  private NodeType destType;
  private Action action;
  private State state;
  private long time;
  /** 
   * Set only for message sent from a Process -> Controller.
   * WARNING : Uninitialized for Process -> Process messages. 
   */
  private NotificationType notificationType;
  
  private static final long serialVersionUID = 1L;
}
