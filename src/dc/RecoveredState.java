package dc;

import dc.State.StateType;

public class RecoveredState{
  public State state;
  public Playlist playlist;
  public String decision;
  public String vote;
  public boolean writtenPlaylistInTransaction;
  public RecoveredState(int numOfProcs) {
    state = new State(StateType.UNCERTAIN, new boolean[numOfProcs]);
    playlist = new Playlist();
    decision = "";
    vote = "No";
    writtenPlaylistInTransaction = false;
    
  }
  @Override
  public String toString() {
    // TODO Auto-generated method stub
    return "DT Log State : "+state.toString()+"..."+playlist.toString()+"..."+decision;
  }
}