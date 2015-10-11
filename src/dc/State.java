package dc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class State implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * @param type
   * @param upset
   */
  public State(StateType type, boolean[] upset, boolean[] alive) {
    super();
    this.type = type;
    this.upset = new boolean[upset.length];
    this.alive = new boolean[alive.length];
    System.arraycopy(upset, 0, this.upset, 0, upset.length);
    System.arraycopy(alive, 0, this.alive, 0, alive.length);
  }

  public State(State other) {
    this(other.getType(), other.getUpset(), other.getAlive());
  }

  public enum StateType {
    UNCERTAIN, COMMITABLE, COMMITED, ABORTED
  };

  public boolean isTerminalState() {
    return isTerminalStateType(type);
  }

  public void setType(StateType st) {
    type = st;
  }

  public StateType getType() {
    return type;
  }

  public boolean[] getUpset() {
    return upset;
  }

  public boolean[] getAlive() {
    return alive;
  }

  public void removeProcessFromUpset(int i) {
    upset[i] = false;
  }
  
  public void removeProcessFromAlive(int i) {
    alive[i] = false;
  }
  
  public void addProcessToAlive(int i) {
    alive[i] = true;
  }
  
  @Override
  public String toString() {
    String stateType = type.toString() + "#";
    for (int i = 0; i < upset.length; i++) {
      stateType += (upset[i]) ? 1 : 0;
    }
    return stateType;
  }

  public static State parseState(String s) throws Exception {
    String[] split = s.split("#");
    State parsedState = null;
    if (split.length != 2) {
      throw new Exception("Error in parsing state : " + s);
    } else {
      StateType stateType = StateType.valueOf(split[0].trim());
      ArrayList<Boolean> upset = new ArrayList<>();
      for (int i = 0; i < split[1].length(); i++) {
        boolean c = (split[1].charAt(i) == '1');
        upset.add(c);
      }
      int size = upset.size();
      boolean[] createdUpset = new boolean[size];
      boolean[] aliveSet = new boolean[size];
      for (int i = 0; i < size; i++) {
        createdUpset[i] = upset.get(i).booleanValue();
        // This should be set for all processes so that it sends atleast 1 
        // heartbeat to every process. If process is not alive then it will be 
        // detected as dead and aliveSet will be updated appropriately.
        aliveSet[i] = true;
      }
      parsedState = new State(stateType, createdUpset, aliveSet);
    }
    return parsedState;
  }

  /*
   * public String toString() {
   * StringBuilder result = new StringBuilder();
   * result.append("\nStateType: " + type);
   * result.append("\nUpset: ");
   * for (int i = 0; i < upset.length; i++) {
   * result.append("\t" + i + ":" + upset[i]);
   * }
   * return result.toString();
   * }
   */

  public static boolean isTerminalStateType(StateType st) {
    return (st == StateType.COMMITED || st == StateType.ABORTED);
  }

  private volatile StateType type;
  private volatile boolean[] upset;
  private volatile boolean[] alive;

  public void setUpset(boolean[] upset) {
    this.upset = Arrays.copyOf(upset, upset.length);
  }
  
  public void setAlive(boolean[] alive) {
    this.alive = Arrays.copyOf(alive, alive.length);
  }

  public static void main(String[] args) {
    boolean[] b = { true, false, false, true };
    State dc = new State(StateType.ABORTED, b, b);
    System.out.println(dc.toString());
    try {
      System.out.println(State.parseState(dc.toString()).toString());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
