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
	
	public Instruction(InstructionType instructionType, 
			String executionOrder, 
			NotificationType notificationType, 
			ActionType actionType, 
			int n, int pId) {
		super();
		this.instructionType = instructionType;
		this.notificationType = notificationType;
		this.actionType = actionType;
		this.partialSteps = n;
		this.executionOrder = executionOrder;
		this.pId = pId;
	}

	public enum InstructionType {
		KILL, HALT, CONTINUE, REVIVE;
	}
	
	private InstructionType instructionType;
	
	private NotificationType notificationType;
	
	private ActionType actionType;
	
	private String executionOrder;
	
	private int partialSteps;
	
	private int pId;

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
		String[] splits = line.split(":");
		if(splits.length == 2)
		{
			int pId = Integer.parseInt(splits[0]);
			String[] instSplits = splits[1].split(" ");
			int len = instSplits.length;
			if(len == 5 || len == 4)
			{
				int n = (len == 5) ? Integer.parseInt(instSplits[4]) : -1 ; 
				Instruction ins = new Instruction(InstructionType.valueOf(instSplits[0]), instSplits[1], NotificationType.valueOf(instSplits[2]), ActionType.valueOf(instSplits[3]), n, pId);
				return ins;
			}
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

	public int getpId() {
		return pId;
	}

	public void setpId(int pId) {
		this.pId = pId;
	}
}
