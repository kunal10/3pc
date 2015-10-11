package dc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;

import dc.State.StateType;
import ut.distcomp.framework.Config;

/**
 * Format of DT Log :
 * Start Transaction
 * Up Set: 1,2,3
 * Decision: ABORT/COMMIT
 * End Transaction
 * 
 * @author av28895
 *
 */
public class DTLog {

  public DTLog(Config config) {
    super();
    this.config = config;
  }

  /**
   * Parse the DT Log and return the state of the process.
   * 
   * @return
   */
  public State retrieveProcessState() {
    return null;
  }

  /**
   * Write a start transaction line.
   */
  public void writeStartTransaction(boolean isTransactionComplete) {
    if (!isTransactionComplete) {
      try (PrintWriter out = new PrintWriter(
              new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
        out.println("Start Transaction");
        // TODO: Write upset.
      } catch (IOException e) {
        // Handle
      }
    }
  }

  /**
   * Write an end transaction line.
   */
  public void writeEndTransaction() {
    try (PrintWriter out = new PrintWriter(
            new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
      out.println("End Transaction");
    } catch (IOException e) {
      // Handle
    }
  }

  /**
   * Write the decision taken by a process for a transaction.
   * 
   * @param decision
   */
  public void writeDecision(String decision, boolean isTransactionComplete) {
    if (!isTransactionComplete) {
      try (PrintWriter out = new PrintWriter(
              new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
        // TODO: Check if there is a decision already for that transaction
        out.println("Decision :" + decision);
      } catch (IOException e) {
        // Handle
      }
    }
  }

  /**
   * Write the state of the process. Should be written everytime the state
   * changes.
   * 
   * @param s
   */
  public void writeState(State s, boolean isTransactionComplete) {
    if (!isTransactionComplete) {
      try (PrintWriter out = new PrintWriter(
              new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
        // TODO: Check if there is a decision already for that transaction
        out.println("State :" + s.toString());
      } catch (IOException e) {
        // Handle
      }
    }
  }

  /**
   * Write the playlist into the DT log.
   * 
   * @param pl
   */
  public void writePlaylist(Playlist pl, boolean isTransactionComplete) {
    if (!isTransactionComplete) {
      try (PrintWriter out = new PrintWriter(
              new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
        // TODO: Check if there is a decision already for that transaction
        out.println("Playlist :" + pl.toString());
      } catch (IOException e) {
        // Handle
      }
    }
  }

  /**
   * Record Vote in DT Log
   * 
   * @param vote
   */
  public void writeVote(boolean vote, boolean isTransactionComplete) {
    if (!isTransactionComplete) {
      try (PrintWriter out = new PrintWriter(
              new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
        // TODO: Check if there is a decision already for that transaction
        out.println("Vote :" + ((vote) ? "Yes" : "No"));
      } catch (IOException e) {
        // Handle
      }
    }
  }

  public RecoveredState parseDTLog() throws FileNotFoundException, IOException {
    RecoveredState rs = new RecoveredState(config.numProcesses);
    try (BufferedReader br = new BufferedReader(
            new FileReader(config.DTFilename))) {
      String line = br.readLine();
      while (line != null) {
        if (line.startsWith("Start")) {
          boolean[] b = new boolean[config.numProcesses];
          for (int i = 0; i < b.length; i++) {
            b[i] = true;
          }
          rs.state.setUpset(b);
          rs.state.setAlive(b);
          rs.state.setType(StateType.UNCERTAIN);
          rs.decision = "";
          rs.writtenPlaylistInTransaction = false;
          config.logger.info("Start DT : " + rs.toString());
        } else if (line.startsWith("End")) {
          config.logger.info("End DT : " + rs.toString());
        } else if (line.startsWith("Decision")) {
          rs.decision = line.split(":")[1];
          config.logger.info("Decision DT : " + rs.toString());
        } else if (line.startsWith("State")) {
          try {
            String[] splits = line.split(":");
            if (splits.length > 1) {
              rs.state = new State(State.parseState(splits[1]));
            }
            config.logger.info("State DT : " + rs.toString());
          } catch (Exception e) {
            config.logger.info("Couldn't parse state");
          }
        } else if (line.startsWith("Playlist")) {
          String[] splits = line.split(":");
          if (splits.length > 1) {
            rs.playlist = Playlist.parsePlaylist(splits[1]);
            rs.writtenPlaylistInTransaction = true;
            config.logger.info("Playlist DT : " + rs.toString());
          }
        } else if (line.startsWith("Vote")) {
          String[] splits = line.split(":");
          if (splits.length > 1) {
            rs.vote = splits[1];
            config.logger.info("Vote DT : " + rs.toString());
          }
        } else {
          config.logger.log(Level.SEVERE,
                  "Couldn't parse " + line + " in DT Log");
        }
        line = br.readLine();
      }
    }
    config.logger
            .info("Finished reading DT log for process: " + config.procNum);
    return rs;
  }

  private Config config;
}
