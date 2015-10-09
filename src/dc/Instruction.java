/**
 * 
 */
package dc;

import java.io.Serializable;

import dc.Action.ActionType;
import dc.Message.NotificationType;

/**
 * @author av28895
 *         Instruction corresponding to a step which has to be executed by the
 *         controller based on the config.
 */
public class Instruction implements Serializable{

  public Instruction(InstructionType instructionType, String executionOrder,
          NotificationType notificationType, ActionType actionType, int n,
          int pId, int seqNo) {
    super();
    this.instructionType = instructionType;
    this.notificationType = notificationType;
    this.actionType = actionType;
    this.partialSteps = n;
    this.executionOrder = executionOrder;
    this.pId = pId;
    this.setSeqNo(seqNo);
  }

  public Instruction(Instruction other) {
    this(other.getInstructionType(), other.getExecutionOrder(),
            other.getNotificationType(), other.getActionType(),
            other.getPartialSteps(), other.getpId(), other.seqNo);
  }

  public enum InstructionType {
    KILL, HALT, RESUME, CONTINUE, REVIVE;
  }

  private InstructionType instructionType;

  private NotificationType notificationType;

  private ActionType actionType;

  private String executionOrder;

  private int partialSteps;

  private int pId;

  private int seqNo;

  final static int numOfInstPartsWithPartialSteps = 5;

  final static int numOfInstParts = 4;

  public InstructionType getInstructionType() {
    return instructionType;
  }

  public void setInstructionType(InstructionType instructionType) {
    this.instructionType = instructionType;
  }

  public NotificationType getNotificationType() {
    return notificationType;
  }

  public ActionType getActionType() {
    return actionType;
  }

  public String getExecutionOrder() {
    return executionOrder;
  }

  public int getPartialSteps() {
    return partialSteps;
  }

  // Given a line read from the config parse it into Instruction format
  public static Instruction parseInstruction(String line, int seqNo) {
    String[] splits = line.split(":");
    if (splits.length == 2) {
      int pId = Integer.parseInt(splits[0]);
      String[] instSplits = splits[1].split(" ");
      int len = instSplits.length;
      if (len == numOfInstPartsWithPartialSteps || len == numOfInstParts) {
        int n = (len == numOfInstPartsWithPartialSteps)
                ? Integer.parseInt(instSplits[4]) : -1;
        Instruction ins = new Instruction(
                InstructionType.valueOf(instSplits[0]), instSplits[1],
                NotificationType.valueOf(instSplits[2]),
                ActionType.valueOf(instSplits[3]), n, pId, seqNo);
        return ins;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("\nInstructionType: " + instructionType.name());
    result.append("\tExecutionOrder: " + executionOrder);
    result.append("\tActionType:" + actionType.name());
    result.append("\tPartialSteps:" + partialSteps);
    return result.toString();
  }

  public static void main(String[] args) {
    System.out.println(Instruction
            .parseInstruction("HALT after RECEIVE VOTE_REQ 4", 1).toString());
  }

  public int getpId() {
    return pId;
  }

  public void setpId(int pId) {
    this.pId = pId;
  }

  public int getSeqNo() {
    return seqNo;
  }

  public void setSeqNo(int seqNo) {
    this.seqNo = seqNo;
  }
}
