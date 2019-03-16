import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class RaftNode implements MessageHandling {
    private int id;
    private static TransportLib lib;
    private int num_peers;
    
    private int currentTerm;
    private Integer votedFor;
    private ArrayList<LogEntry> log;

    private int commitIndex;
    private int lastApplied;
    
    private ArrayList<Integer> nextIndex;
    private ArrayList<Integer> matchIndex;

    private boolean isLeader;
    private int currentVotes;

    private static Timer electionTimer;
    private static int electionTimeout;
    private static Timer heartbeatTimer;
    private static int heartbeatTimeout;

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        lib = new TransportLib(port, id, this);
        
        currentTerm = 0;
        votedFor = null;
        log = new ArrayList<LogEntry>();
        
        commitIndex = 0;
        lastApplied = 0;
        
        nextIndex = new ArrayList<Integer>();
        for (int i = 0; i < num_peers; i++) {
        	nextIndex.add(1);
        }
        
        matchIndex = new ArrayList<Integer>();
        for (int i = 0; i < num_peers; i++) {
        	nextIndex.add(0);
        }

        isLeader = false;
        currentVotes = 0;
        
        electionTimer = null;
        electionTimeout = 0;
        heartbeatTimer = null;
        heartbeatTimeout = 0;
    }
    
    public byte[] objToByte(Object object) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
        objStream.writeObject(object);

        return byteStream.toByteArray();
    }
    
    public Object byteToObj(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);

        return objStream.readObject();
    }

    /*
     *call back.
     */
    @Override
    public StartReply start(int command) {
    	System.out.println("StartReply called");
        return null;
    }

    @Override
    public GetStateReply getState() {
    	System.out.println("getStateReply called");
        
    	return new GetStateReply(currentTerm, isLeader);
    }

    @Override
    public Message deliverMessage(Message message) {
		if (message == null) {
			return null;
		}
    	
		Message response = null;
		boolean voteGranted = false;
		boolean success = false;
    	
		// The node receives either a request for votes message or append entry message
		// It also needs to return the reply to the sender
		
		try {
	    	if (message.getType() == MessageType.RequestVoteArgs) {
	    		voteGranted = getVote((RequestVoteArgs) byteToObj(message.getBody()));
	    		
	    		byte[] body = objToByte(new RequestVoteReply(currentTerm, voteGranted));
	            response = new Message(MessageType.RequestVoteReply, id, 0, body);
	    	} else if (message.getType() == MessageType.AppendEntriesArgs) {
	    		success = updateLogs((AppendEntriesArgs) byteToObj(message.getBody()));
	    		
	    		byte[] body = objToByte(new AppendEntriesReply(currentTerm, success));
	            response = new Message(MessageType.AppendEntriesReply, id, 0, body);
	    	}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        return response;
    }
 
    // This does synchronized work when an Append Entry Request is sent
    public synchronized boolean updateLogs(AppendEntriesArgs arguments) {
		boolean check = true;
		
		if (check) {
			electionTimer.cancel();
			startElectionTimer(new ElectionTask());
			
			return true;
		}
		
		return false;
    }
    
    // This does synchronized work when an Request Vote Request is sent
    public synchronized boolean getVote(RequestVoteArgs arguments) {
		System.out.println("Message from: " + arguments.candidateId 
				+ " | Message to: " + id 
				+ " | Term: " + arguments.term);
		
		boolean voteCheck = votedFor == null || votedFor == arguments.candidateId;
		boolean termCheck = (arguments.term == currentTerm && voteCheck)
							|| arguments.term > currentTerm;
		
		int lastLogIndex = log.size() - 1;
		int lastLogTerm = (log.get(lastLogIndex)).term;
		boolean logCheck = (lastLogTerm < arguments.lastLogTerm 
							|| (lastLogTerm == arguments.lastLogTerm && lastLogIndex <= arguments.lastLogIndex));
		
		if (termCheck && logCheck) {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | Granted");
			votedFor = arguments.candidateId;
			currentTerm = arguments.term;
			
			electionTimer.cancel();
			startElectionTimer(new ElectionTask());
			
			return true;
		} else {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | NOT Granted");
		}
		
		return false;
    }
    
    // This does synchronized work when the response to the Request Vote is received
    public synchronized boolean becomeLeader(RequestVoteReply reply) {
    	if (reply.term == currentTerm && reply.voteGranted) {
			currentVotes++;
			
    		System.out.println("User: " + id + " | Votes: " + currentVotes);
			
			if (currentVotes >= num_peers / 2 + 1) {
				isLeader = true;
				
				return true;
			}
    	}
    	
    	return false;
    }
    
    // This function tries to broadcast a message for all servers
    // It creates a new thread for each communication because we need to wait for the reply
    public void sendMessageAll(MessageType type, byte[] body) {
		for (int i = 0; i < num_peers; i++) {
    		if (i != id) {
    			Runnable r = new MessageSender(type, i, body);
    			new Thread(r).start();
    		}
    	}
    }
    
    // This is the thread that handles sending messages for a server
    public class MessageSender implements Runnable {
    	private MessageType type;
    	private int des_add;
    	private byte[] body;
    	
    	public MessageSender(MessageType type, int des_add, byte[] body) {
    		this.type = type;
    		this.des_add = des_add;
    		this.body = body;
		}

    	public void run() {
    		try {
    			Message message = lib.sendMessage(new Message(type, id, des_add, body));
				
    			if (message == null) {
    				return;
    			}
			   
    			// The node receives a reply for his request
    			
				if (message.getType() == MessageType.RequestVoteReply) {
		    		RequestVoteReply reply = (RequestVoteReply) byteToObj(message.getBody());
		    		
		    		if (becomeLeader(reply)) {
	    				System.out.println("I AM LEADER pepeJAM id: " + id);
	    				
	    				startHeartbeatTimer(new HeartbeatTask(), true);
		    		}
		    	} else if (message.getType() == MessageType.AppendEntriesReply) {
		    		AppendEntriesReply reply = (AppendEntriesReply) byteToObj(message.getBody());
		    		
		    		if (!reply.success) {
	    				
		    		}
		    	}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
    	}
	}
    
    // This starts the election timer
    public static void startElectionTimer(ElectionTask task) {
    	electionTimeout = ((int) Math.random()) * 800 + 300;
    	electionTimer = new Timer();
    	electionTimer.schedule(task, electionTimeout);
    }
    
    // This starts the heartbeat timer with the option to send a heartbeat right away
    public static void startHeartbeatTimer(HeartbeatTask task, boolean doItNow) {
    	heartbeatTimeout = 150;
    	heartbeatTimer = new Timer();
    	if (doItNow) {
    		heartbeatTimer.schedule(task, 0, heartbeatTimeout);
    	} else {
    		heartbeatTimer.schedule(task, heartbeatTimeout);
    	}
    }

    // This is the heartbeat
    public class HeartbeatTask extends TimerTask {
        @Override
        public void run() {
        	startHeartbeatTimer(new HeartbeatTask(), false);
            
        	ArrayList<LogEntry> empty = new ArrayList<LogEntry>();
			try {
				int lastLogIndex = log.size() - 1;
				int lastLogTerm = (log.get(lastLogIndex)).term;
				
				byte[] body = objToByte(new AppendEntriesArgs(currentTerm, id, lastLogIndex, lastLogTerm, empty, commitIndex));
				sendMessageAll(MessageType.AppendEntriesArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
    
    // This is the election
    public class ElectionTask extends TimerTask {
        @Override
        public void run() {
        	currentTerm++;
        	currentVotes = 0;
        	votedFor = id;
        	
        	System.out.println("LEADER MISSING NOTICED REQUESTING FOR VOTES"
					+ " | ID: " + id 
					+ " | Term: " + currentTerm);
        	
        	startElectionTimer(new ElectionTask());
            
			try {
				int lastLogIndex = log.size() - 1;
				int lastLogTerm = (log.get(lastLogIndex)).term;
				
				byte[] body = objToByte(new RequestVoteArgs(currentTerm, id, lastLogIndex, lastLogTerm));
				sendMessageAll(MessageType.RequestVoteArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        
        startElectionTimer(UN.new ElectionTask());
    }
}
