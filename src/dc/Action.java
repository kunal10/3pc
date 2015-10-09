package dc;

import java.io.Serializable;

/*
 * Class which captures all the possible actions which are performed by
 * processes involved in 3pc. Execution of any particular process in 3pc
 * corresponds to some set of these actions performed in sequential order.
 * 
 * NOTE : Controller config is described in terms of these actions.
 */
public class Action implements Serializable {

  public Action(ActionType type, String value) {
    this.type = type;
    this.value = value;
  }

  public Action(Action action) {
    this(action.getType(), action.getValue());
  }

  public enum ActionType {
    START, VOTE_REQ, VOTE_RES, STATE_REQ, STATE_RES, DECISION, PRE_COMMIT, ACK,
    LOG
  };

  private ActionType type;

  public ActionType getType() {
    return type;
  }

  public String getValue() {
    return value;
  }
  
  @Override
	public String toString() {
		return "\n" + type.toString() + " " + value;
	}

  /**
   * String describing the action to be performed. For eg:
   * value can Yes/No in case type = VOTE_RES.
   * Empty for actions which don't need any description.
   */
  private String value;


  private static final long serialVersionUID = 1L;
}