package lib;

import java.io.Serializable;

/**
 * This class is a wrapper for packing all the arguments that you might use in
 * the RequestVote call, and should be serializable to fill in the payload of
 * Message to be sent.
 *
 */
public class AppendEntriesArgs implements Serializable {
	public int term;
	public int leaderId;
	public int prevLogIndex;
	public int prevLogTerm;
	public int entries[];
	public int leaderCommit;
	
    public AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm,
    							int[] entries, int leaderCommit) {
    	this.term = term;
    	this.leaderId = leaderId;
    	this.prevLogIndex = prevLogIndex;
    	this.prevLogTerm = prevLogTerm;
    	this.entries = entries;
    	this.leaderCommit = leaderCommit;
    }
}
