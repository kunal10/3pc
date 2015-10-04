package dc;


/**
 * Class which captures messages exchanged between different processes/
 * coordinator. 
 */
public class Message {
  /**
   * Nodes of message are classified on the basis
   * of type of source and destination. Currently we have 2 kinds of nodes:
   * 
   * 1. CONTROLLER : Controller which micro manages the simulation of protocol.
   * 2. PROCESS : Process nodes involved in the execution of the protocol.
   */
  public enum NodeType { CONTROLLER, PROCESS };
  
  /**
   * Enum for notifying the controller about next action(corresponds to the 
   * action in this message) of the process. 
   */
  public enum NotificationType { SEND, RECEIVE, DELIVER };
  
  public Message(int src, int dest, Action action, long time) {
    this.src = src;
    this.dest = dest;
    this.action = new Action(action);
    this.time = time;
    setNodeTypes();
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
  public long getTime() {
    return time;
  }
  public NotificationType getNotificationType() {
    return notificationType;
  }
  
  private void setNodeTypes() {
    if (src == 0) {
      srcType = NodeType.CONTROLLER;
    } else {
      srcType = NodeType.PROCESS;
    }
    if (dest == 0) {
      destType = NodeType.CONTROLLER;
    } else {
      destType = NodeType.PROCESS;
    }
  }
  
  private int src;
  private int dest;
  private NodeType srcType;
  private NodeType destType;
  private Action action;
  private long time;
  /** 
   * Set only for message sent from a Process -> Controller.
   * WARNING : Uninitialized for Process -> Process messages. 
   */
  private NotificationType notificationType;
}
