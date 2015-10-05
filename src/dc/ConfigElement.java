package dc;

import java.util.ArrayList;

public class ConfigElement {
	private Transaction transaction;
	private ArrayList<Integer> noVotes;
	public ArrayList<Instruction> instructions;
	public ConfigElement() {
		// TODO Auto-generated constructor stub
		instructions = new ArrayList<>();
	}
	public Transaction getTransaction() {
		return transaction;
	}
	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}
	public ArrayList<Integer> getNoVotes() {
		return noVotes;
	}
	public void setNoVotes(ArrayList<Integer> noVotes) {
		this.noVotes = new ArrayList<>(noVotes);
	}
	public ArrayList<Instruction> getInstructions() {
		return instructions;
	}
	public void setInstructions(ArrayList<Instruction> instructions) {
		this.instructions = new ArrayList<Instruction>(instructions);
	}
}

