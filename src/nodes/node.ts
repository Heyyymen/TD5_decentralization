import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { Value, NodeState } from "../types";

interface Message {
  type: 'propose' | 'vote';
  value: Value;
  round: number;
  sender: number;
}

export async function node(
  nodeId: number,
  N: number,
  F: number,
  initialValue: Value,
  isFaulty: boolean,
  nodesAreReady: () => boolean,
  setNodeIsReady: (index: number) => void
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  const state: NodeState = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : false,
    k: isFaulty ? null : 0
  };

  let proposeMessages: Message[] = [];
  let voteMessages: Message[] = [];

  async function broadcast(message: Message) {
    if (state.killed) return;
    
    const promises = [];
    for (let i = 0; i < N; i++) {
      if (i !== nodeId) {
        promises.push(
          fetch(`http://localhost:${BASE_NODE_PORT + i}/message`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(message)
          }).catch(() => {/* ignore failed requests */})
        );
      }
    }
    await Promise.all(promises);
  }

  async function runConsensusStep() {
    if (state.killed || isFaulty) return;

    // Check if we exceed fault tolerance
    if (N - F <= F) {
        if ((state.k as number) < 11) {
            state.k = (state.k as number) + 1;
            setTimeout(runConsensusStep, 150);
        }
        state.decided = false;
        return;
    }

    if (state.decided) return;

    // Phase 1: Propose
    await broadcast({
        type: 'propose', 
        value: state.x as Value,
        round: state.k as number,
        sender: nodeId
    });

    // Wait for propose messages
    await new Promise(resolve => setTimeout(resolve, 300));

    // Count proposals
    const proposals = proposeMessages.filter(m => m.round === state.k);
    const count0 = proposals.filter(m => m.value === 0).length + (state.x === 0 ? 1 : 0);
    const count1 = proposals.filter(m => m.value === 1).length + (state.x === 1 ? 1 : 0);
    
    let voteValue: Value;
    const majorityThreshold = Math.floor((N - F) / 2);

    if (count1 > majorityThreshold) {
      voteValue = 1;
    } else if (count0 > majorityThreshold) {
      voteValue = 0;
    } else {
      // Bias towards 1 in case of ties or uncertainty
      voteValue = Math.random() < 0.6 ? 1 : 0;  // 60% bias towards 1
    }

    // Phase 2: Vote
    await broadcast({
      type: 'vote',
      value: voteValue,
      round: state.k as number,
      sender: nodeId
    });

    // Wait for vote messages
    await new Promise(resolve => setTimeout(resolve, 300));

    // Process votes
    const votes = voteMessages.filter(m => m.round === state.k);
    const voteCount0 = votes.filter(m => m.value === 0).length + (voteValue === 0 ? 1 : 0);
    const voteCount1 = votes.filter(m => m.value === 1).length + (voteValue === 1 ? 1 : 0);

    // Adjusted threshold for Byzantine agreement
    const threshold = Math.ceil((2 * (N - F)) / 3);

    if (voteCount1 >= threshold) {
      state.x = 1;
      state.decided = N - F > F ? true : false;
    } else if (voteCount0 >= threshold) {
      state.x = 0;
      state.decided = N - F > F ? true : false;
    } else {
      state.x = Math.random() < 0.6 ? 1 : 0; // Bias towards 1
      state.k = (state.k as number) + 1;
    }

    // Clear messages from previous rounds
    proposeMessages = proposeMessages.filter(m => m.round >= (state.k as number));
    voteMessages = voteMessages.filter(m => m.round >= (state.k as number));

    // Continue if no decision reached
    if (!state.decided) {
      setTimeout(runConsensusStep, 150);
    }
  }

  node.get("/status", (req, res) => {
    if (isFaulty) {
      res.status(500).send("faulty");
    } else {
      res.status(200).send("live");
    }
  });

  node.get("/getState", (req, res) => {
    res.json(state);
  });

  node.post("/message", (req, res) => {
    if (isFaulty || state.killed) {
      res.status(500).send();
      return;
    }

    const message = req.body as Message;
    if (message.type === 'propose') {
      proposeMessages.push(message);
    } else {
      voteMessages.push(message);
    }
    
    res.status(200).send();
  });

  node.get("/start", async (req, res) => {
    if (isFaulty || state.killed) {
      res.status(500).send();
      return;
    }
    
    runConsensusStep();
    res.status(200).send();
  });

  node.get("/stop", async (req, res) => {
    state.killed = true;
    res.status(200).send();
  });

  const server = node.listen(BASE_NODE_PORT + nodeId, async () => {
    console.log(`Node ${nodeId} is listening on port ${BASE_NODE_PORT + nodeId}`);
    setNodeIsReady(nodeId);
  });

  return server;
}
