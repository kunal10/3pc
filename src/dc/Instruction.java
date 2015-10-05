/**
 * 
 */
package dc;

import dc.Action.ActionType;
import dc.Message.NotificationType;

/**
 * @author av28895
 * Instruction corresponding to a step which has to be executed by the controller based on the config. 
 */
public class Instruction {
	
	public Instruction(InstructionType instructionType, String executionOrder, NotificationType notificationType, ActionType actionType, int n) {
		super();
		this.instructionType = instructionType;
		this.notificationType = notificationType;
		this.actionType = actionType;
		this.partialSteps = n;
		this.executionOrder = executionOrder;
	}

	public enum InstructionType {
		KILL, HALT, CONTINUE, REVIVE;
	}
	
	private InstructionType instructionType;
	
	private NotificationType notificationType;
	
	private ActionType actionType;
	
	private String executionOrder;
	
	private int partialSteps;

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

	public int getPartialSteps() {
		return partialSteps;
	}
	
	//Given a line read from the config parse it into Instruction format
	public static Instruction parseInstruction(String line)
	{
		String[] splits = line.split(" ");
		int len = splits.length;
		if(len == 5 || len == 4)
		{
			int n = (len == 5) ? Integer.parseInt(splits[4]) : -1 ; 
			Instruction ins = new Instruction(InstructionType.valueOf(splits[0]), splits[1], NotificationType.valueOf(splits[2]), ActionType.valueOf(splits[3]), n);
			return ins;
		}
		return null;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return instructionType.toString()+" "+executionOrder+" "+notificationType.toString()+" "+actionType.toString()+" "+partialSteps;
	}
	
	public static void main(String[] args) {
		System.out.println(Instruction.parseInstruction("HALT after RECEIVE VOTE_REQ 4").toString());
	}
}
