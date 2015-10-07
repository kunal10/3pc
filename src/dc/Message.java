package dc;

import java.io.Serializable;

/**
 * Class which captures messages exchanged in the network. 
 */
public class Message implements Serializable  {
  public Instruction getInstr() {
		return instr;
	}

	public void setInstr(Instruction instr) {
		this.instr = instr;
	}

	public void setSrc(int src) {
		this.src = src;
	}

	public void setDest(int dest) {
		this.dest = dest;
	}

	public void setSrcType(NodeType srcType) {
		this.srcType = srcType;
	}

	public void setAction(Action action) {
		this.action = action;
	}

	public void setState(State state) {
		this.state = state;
	}

	public void setTime(long time) {
		this.time = time;
	}

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
  
  /** Base contructor used by other constructors */
  public Message(int src, int dest, NodeType srcType, NodeType destType, 
                 long time) {
    this.src = src;
    this.dest = dest;
    this.srcType = srcType;
    this.destType = destType;
    this.time = time;
  }
  
  /** 
   * Should be used only by non-heartbeat messages. State is null and Action is 
   * non null
   */
  public Message(int src, int dest, NodeType srcType, NodeType destType,
                 Action action, long time) {
    this(src, dest, srcType, destType, time);
    this.action = new Action(action);
  }
  
  /** 
   * Should be used only by non-heartbeat messages. State is null and Action is 
   * non null
   */
  public Message(int src, int dest, NodeType srcType, NodeType destType,
                 Instruction instr, long time) {
    this(src, dest, srcType, destType, time);
    this.instr = new Instruction(instr);
  }
  
  /** Should only be used by heartbeat messages */
  public Message(int src, int dest, NodeType srcType, NodeType destType, 
                 State state, long time) {
    this(src, dest, srcType, destType, time);
    this.state = new State(state);
  }
  
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("Src: " + src);
    result.append("Dest: " + dest);
    result.append("SrcType: " + srcType.name());
    result.append("DestType: " + destType.name());
    if (instr != null)
      result.append("Instr: " + instr.toString());
    if (action != null)
      result.append("Action: " + action.toString());
    if (state != null)
      result.append("State: " + state.toString());
    result.append("Time: " + time);
    return result.toString();
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
  public Instruction getInstruction() {
    return instr;
  }
  public State getState() {
    return state;
  }
  public long getTime() {
    return time;
  }
  public void setNotificationType(NotificationType nt) {
    this.notificationType = nt;
  }
  public NotificationType getNotificationType() {
    return notificationType;
  }
  public boolean isHeartbeatMessage() {
	  // If state is populated then it is a heartbeat message.
	  return (state != null) ;
  }
  
  private int src;
  private int dest;
  private NodeType srcType;
  private NodeType destType;
  public void setDestType(NodeType destType) {
	this.destType = destType;
}

private Action action;
  private Instruction instr;
  private State state;
  private long time;
  /** 
   * Set only for message sent from a Process -> Controller.
   * WARNING : Uninitialized for Process -> Process messages. 
   */
  private NotificationType notificationType;
  
  private static final long serialVersionUID = 1L;
}
