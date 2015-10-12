package dc;

/**
 * This class encapsulates the different kinds of transactions which can be 
 * performed on the playlist class. 
 */
public class Transaction {
  public enum TransactionType { ADD, EDIT, DELETE };
  
  /**
   * @param type : Transaction type of this transaction.
   * @param oldSong : Song to be edited/deleted by this transaction.
   * @param newSong : New song which is going to be either added or replace old 
   *                  song in the playlist.
   */
  public Transaction(TransactionType type, Song oldSong, Song newSong) {
    super();
    this.type = type;
    this.oldSong = oldSong;
    this.newSong = newSong;
  }
  
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("\nTransactionType: " + type.name());
    if (oldSong != null) {
      result.append("\nOldSong: " + oldSong.toString());
    }
    if (newSong != null) {
      result.append("\nNew Song: " + newSong.toString());
    }
    return result.toString();
  }
  
  public TransactionType getType() {
    return type;
  }
  public Song getOldSong() {
    return oldSong;
  }
  public Song getNewSong() {
    return newSong;
  }

  private TransactionType type;
  /** Not set for ADD transactions */
  private Song oldSong;
  /** Not set for DELETE transactions */
  private Song newSong;
}
