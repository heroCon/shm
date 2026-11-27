------------------------------ MODULE ShmPubsub ------------------------------
\* A small model of one queue, the block reference word and local repair.
\* It is not the C++ implementation; it is the argument that repair depends on:
\* a cell is a hole, a committed message, a claim, or a tombstone, and a block's
\* queue references are exactly its committed and claimed cells once every dead
\* writer has been repaired. Crashes happen between those steps.
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Cap

Blocks == {"b1", "b2", "b3"}
Pubs == {"p1", "p2"}
Cons == {"c1", "c2"}

KHole == "hole"
KCommit == "commit"
KClaim == "claim"
KTomb == "tomb"

VARIABLES queue, refs, holders, free, pstate, pinflight, palive, cstate, cinflight,
          calive, repaired

vars == <<queue, refs, holders, free, pstate, pinflight, palive, cstate, cinflight, calive,
         repaired>>

Init ==
  /\ queue = <<>>
  /\ refs = [b \in Blocks |-> 0]
  /\ holders = [b \in Blocks |-> {}]
  /\ free = Blocks
  /\ pstate = [p \in Pubs |-> "idle"]
  /\ pinflight = [p \in Pubs |-> "none"]
  /\ palive = [p \in Pubs |-> TRUE]
  /\ cstate = [c \in Cons |-> "idle"]
  /\ cinflight = [c \in Cons |-> "none"]
  /\ calive = [c \in Cons |-> TRUE]
  /\ repaired = TRUE

Count(b) ==
  Cardinality({i \in DOMAIN queue : queue[i].kind \in {KCommit, KClaim} /\ queue[i].block = b})

TypeOK ==
  /\ Len(queue) <= Cap
  /\ refs \in [Blocks -> 0..Cap + 2]
  /\ free \subseteq Blocks
  /\ \A p \in Pubs : pstate[p] \in {"idle", "resv"} /\ (pinflight[p] \in Blocks \/ pinflight[p] = "none")
  /\ \A c \in Cons : cstate[c] \in {"idle", "claimed", "holding"}
                     /\ (cinflight[c] \in Blocks \/ cinflight[c] = "none")

\* A free block is referenced by nobody.
NoUseAfterFree ==
  \A b \in free : refs[b] = 0 /\ holders[b] = {} /\ Count(b) = 0

\* A block is freed only from the all-zero word, so it cannot be freed twice:
\* the free set never contains a block that still has a reference.
NoDoubleFree ==
  \A b \in Blocks : (b \in free) => refs[b] = 0

\* After repair, and while nobody is mid-operation, the word matches the queue
\* and the live holders.
Quiescent ==
  /\ \A p \in Pubs : pstate[p] = "idle"
  /\ \A c \in Cons : cstate[c] \in {"idle", "holding"}

Conservation ==
  repaired /\ Quiescent =>
    \A b \in Blocks :
      /\ refs[b] = Count(b)
      /\ (b \in free) <=> (refs[b] = 0 /\ holders[b] = {})

Rest == UNCHANGED vars

Reserve(p) ==
  /\ palive[p] /\ pstate[p] = "idle" /\ Len(queue) < Cap /\ free /= {}
  /\ \E b \in free :
       /\ queue' = Append(queue, [block |-> b, kind |-> KHole])
       /\ refs' = [refs EXCEPT ![b] = @ + 1]
       /\ free' = free \ {b}
       /\ pstate' = [pstate EXCEPT ![p] = "resv"]
       /\ pinflight' = [pinflight EXCEPT ![p] = b]
       /\ repaired' = FALSE
       /\ UNCHANGED <<holders, palive, cstate, cinflight, calive>>

Commit(p) ==
  /\ palive[p] /\ pstate[p] = "resv"
  /\ LET b == pinflight[p]
         idx == CHOOSE i \in DOMAIN queue : queue[i].kind = KHole /\ queue[i].block = b
     IN queue' = [queue EXCEPT ![idx].kind = KCommit]
  /\ pstate' = [pstate EXCEPT ![p] = "idle"]
  /\ pinflight' = [pinflight EXCEPT ![p] = "none"]
  /\ UNCHANGED <<refs, holders, free, palive, cstate, cinflight, calive, repaired>>

\* Dies with a reserved hole, or idle. The hole stays until repair.
CrashPub(p) ==
  /\ palive[p]
  /\ palive' = [palive EXCEPT ![p] = FALSE]
  /\ repaired' = FALSE
  /\ UNCHANGED <<queue, refs, holders, free, pstate, pinflight, cstate, cinflight, calive>>

\* Claim the oldest committed cell, skipping tombstones in front of it.
Claim(c) ==
  /\ calive[c] /\ cstate[c] = "idle"
  /\ \E i \in DOMAIN queue :
       /\ queue[i].kind = KCommit
       /\ \A j \in DOMAIN queue : j < i => queue[j].kind = KTomb
       /\ queue' = [queue EXCEPT ![i].kind = KClaim]
       /\ cstate' = [cstate EXCEPT ![c] = "claimed"]
       /\ cinflight' = [cinflight EXCEPT ![c] = queue[i].block]
       /\ repaired' = FALSE
       /\ UNCHANGED <<refs, holders, free, pstate, pinflight, palive, calive>>

\* One step turns the queue reference into a holder bit and releases the cell.
Convert(c) ==
  /\ calive[c] /\ cstate[c] = "claimed"
  /\ LET b == cinflight[c]
         idx == CHOOSE i \in DOMAIN queue : queue[i].kind = KClaim /\ queue[i].block = b
     IN /\ queue' = [queue EXCEPT ![idx].kind = KTomb]
        /\ refs' = [refs EXCEPT ![b] = @ - 1]
        /\ holders' = [holders EXCEPT ![b] = @ \cup {c}]
  /\ cstate' = [cstate EXCEPT ![c] = "holding"]
  /\ UNCHANGED <<free, pstate, pinflight, palive, cinflight, calive, repaired>>

Release(c) ==
  /\ calive[c] /\ cstate[c] = "holding"
  /\ LET b == cinflight[c]
     IN /\ holders' = [holders EXCEPT ![b] = @ \ {c}]
        /\ free' = IF refs[b] = 0 /\ holders[b] = {c} THEN free \cup {b} ELSE free
        /\ cinflight' = [cinflight EXCEPT ![c] = "none"]
  /\ cstate' = [cstate EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<queue, refs, pstate, pinflight, palive, calive, repaired>>

CrashCons(c) ==
  /\ calive[c]
  /\ calive' = [calive EXCEPT ![c] = FALSE]
  /\ repaired' = FALSE
  /\ UNCHANGED <<queue, refs, holders, free, pstate, pinflight, palive, cstate, cinflight>>

\* Drop tombstones at the head so the queue can move.
Compact ==
  /\ queue /= <<>> /\ queue[1].kind = KTomb
  /\ queue' = Tail(queue)
  /\ UNCHANGED <<refs, holders, free, pstate, pinflight, palive, cstate, cinflight, calive, repaired>>

\* Local repair, atomic in the model because each of its real steps is a single
\* CAS and a crash resumes it. Holes of dead publishers become tombstones and
\* lose the reference that was never committed. Claims of dead consumers that
\* were not converted lose their queue reference. Holds of dead consumers are
\* dropped (at most once). Then every word matches the queue.
Repair ==
  /\ ~repaired
  /\ \A p \in Pubs : palive[p] => pstate[p] = "idle"
  /\ \A c \in Cons : calive[c] => cstate[c] = "idle"
  /\ LET deadHole(i) == queue[i].kind = KHole /\ \E p \in Pubs : ~palive[p] /\ pinflight[p] = queue[i].block
         deadClaim(i) == queue[i].kind = KClaim /\ \E c \in Cons : ~calive[c] /\ cstate[c] = "claimed"
                                          /\ cinflight[c] = queue[i].block
         q2 == [i \in DOMAIN queue |-> IF deadHole(i) \/ deadClaim(i)
                                        THEN [block |-> queue[i].block, kind |-> KTomb]
                                        ELSE queue[i]]
         liveHolders(b) == {c \in holders[b] : calive[c]}
         counted(b) == Cardinality({i \in DOMAIN q2 : q2[i].kind \in {KCommit, KClaim} /\ q2[i].block = b})
     IN /\ queue' = q2
        /\ refs' = [b \in Blocks |-> counted(b)]
        /\ holders' = [b \in Blocks |-> liveHolders(b)]
        /\ free' = {b \in Blocks : counted(b) = 0 /\ liveHolders(b) = {}}
        /\ pstate' = [p \in Pubs |-> "idle"]
        /\ pinflight' = [p \in Pubs |-> "none"]
        /\ cstate' = [c \in Cons |-> IF calive[c] THEN "idle" ELSE cstate[c]]
        /\ cinflight' = [c \in Cons |-> IF calive[c] THEN "none" ELSE cinflight[c]]
        /\ repaired' = TRUE
        /\ UNCHANGED <<palive, calive>>

Next ==
  \/ \E p \in Pubs : Reserve(p) \/ Commit(p) \/ CrashPub(p)
  \/ \E c \in Cons : Claim(c) \/ Convert(c) \/ Release(c) \/ CrashCons(c)
  \/ Compact
  \/ Repair

\* A committed cell at the head and a live idle consumer is eventually claimed.
LiveConsumer == \E c \in Cons : calive[c] /\ cstate[c] = "idle"
HeadCommitted == \E i \in DOMAIN queue : queue[i].kind = KCommit
                 /\ \A j \in DOMAIN queue : j < i => queue[j].kind = KTomb
EventualDelivery == (HeadCommitted /\ LiveConsumer) ~> (~HeadCommitted \/ ~LiveConsumer)

Fairness ==
  /\ \A c \in Cons : WF_vars(Claim(c)) /\ WF_vars(Convert(c)) /\ WF_vars(Release(c))
  /\ WF_vars(Compact)
  /\ WF_vars(Repair)

Spec == Init /\ [][Next]_vars /\ Fairness

=============================================================================
