#!/usr/bin/env python3

"""
Compute the markov model properties for the equilibrium approximation of a take-half parallel system.

S = number of workers
B = number of busy workers
I = number of idle workers
F = 1/2 = take fraction (use 1/2 for now)

This is a really rough approximation
- treat each composite task as exponential, even though it is a sum of exponentials.
- the service rate of the composite tasks is L*mu/S.
- assume the system is in a sort of equilibrium where all the tasks currently running have the same rate.

"""


import math
import sys
import os.path
from operator import truediv
import csv
import re
import numpy as np
np.set_printoptions(edgeitems=30, linewidth=100000, formatter={'float': lambda x: "{0:0.3f}".format(x)})
import argparse
import scipy.sparse
import scipy.sparse.linalg
from scipy.sparse import csr_matrix, csc_matrix, lil_matrix
from scipy.sparse.linalg import spsolve
from scipy.integrate import quad
from scipy.special import gammainc, gamma
import matplotlib.pyplot as plt

### for older versions of python
# from typing import Dict


class SysState:
    """The state of an take-all barrier-mode parallel system."""
    __global_id_counter__ = 0

    state_id = 0
    state_len = 0
    s = 0
    b = 0
    l = 0
    H = 0
    transitions = None
    visited = False
    cycle_class = None

    class Transition:
        """A transition between two SysStates

        A transition happens when one of the running tasks finishes,
        or a new job arrives.
        To assign the weight of this transition ..."""
        S1 = None
        S2 = None
        weight = 0.0
        job_start = False
        task_departure = False

        def __init__(self, S1, S2, weight, job_start=False, task_departure=False):
            self.S1 = S1
            self.S2 = S2
            self.weight = weight
            self.job_start = job_start
            self.task_departure = task_departure

        def __str__(self):
            return "Transition: ("+str(self.S1.l)+","+str(self.S1.H)+") --> ("+str(self.S2.l)+","+str(self.S2.H)+") \tweight= "+str(self.weight)+"\tstart="+str(self.job_start)+"\tdeparture="+str(self.task_departure)


    def __init__(self, s, l, H):
        self.state_id = SysState.__global_id_counter__
        SysState.__global_id_counter__ += 1
        self.s = s
        self.b = min(s - l, s)
        self.l = l
        self.H = H
        self.transitions = {}

        # sanity check
        print("\ncreate SysState: (S, B, L, H) = (", self.s, self.b, self.l, self.H, ")  ID=", self.state_id)

    def __str__(self):
        return "SysState: (s,b,l, H)=("+str(self.s)+","+str(self.b)+","+str(self.l)+","+str(self.H)+")"

    def add_transition(self, S2, weight, job_start=False, task_departure=False):
        self.transitions[S2.l,S2.H] = SysState.Transition(self, S2, weight, job_start, task_departure)
        print("\tadd_transition (", self.l, self.H, ") --> (", S2.l, S2.H, ")  weight=",weight)


def probability_n_bins_empty(b, H, n):
    """Compute the probability that exactly n bins are empty."""
    print(f"\t\tprobability_n_bins_empty(b={b}, H={H}, n={n})")
    k = b - n  # Bins that must have at least one ball
    if b < n or k < 0:
        return 0.0

    # Number of ways to distribute the balls such that exactly k bins are non-empty
    ways_to_fill_k_bins = math.comb(b, k) * math.comb(H-1, b-n-1)

    # Total number of ways to distribute H indistinguishable balls into b distinguishable bins
    total_ways = math.comb(H + b - 1, b - 1)

    # Probability that exactly n bins are empty
    probability = ways_to_fill_k_bins / total_ways

    return probability


def total_departure_probability(b, H, s):
    if H <= b:
        print(f"TASK DEPARTURE TRANSITION FORCED BECAUSE H <= b  ({H} <= {b})")
        return 1.0
    #if b <= (H/s):
    if (b-1) < ((H-1) / s):
        print(f"TASK DEPARTURE TRANSITION BLOCKED BECAUSE (b-1)*s >= (H-1)")
        return 0.0
    min_backlogged_workers = math.ceil((H-b)/(s-1))
    min_singletask_workers = max(1, b - (H-b))
    #print(f"min_backlogged_workers={min_backlogged_workers}\t min_singletask_workers={min_singletask_workers}")
    ss = 0.0
    for i in range(min_singletask_workers, b-min_backlogged_workers+1):
        pr = probability_n_bins_empty(b, H-b, i)
        #print(f"for {i} non-backlogged workers the prob is {pr:.6f}")
        ns = (i/b) * pr
        #print(f"additional sum = {ns}")
        ss += ns
    return ss

def num_possible_nonempty_bins_slimit(H, k, s):
    """
    Count the number of ways to distribute H identical balls into k distinguishable bins
    such that each bin has at least 1 and at most s balls.
    """
    print(f"\t\tnum_possible_nonempty_bins_slimit(H={H}, k={k}, s={s})")
    total = 0
    for j in range(k + 1):
        top = H - 1 - j * s
        if top < k - 1:
            continue  # Binomial coefficient is 0 if top < bottom
        term = (-1) ** j * math.comb(k, j) * math.comb(top, k - 1)
        total += term
    return total

def num_possible_bins_slimit(H, k, s):
    """
    Count the number of ways to distribute H identical balls into k distinguishable bins
    where each bin can have between 0 and s balls (bins may be empty).
    """
    print(f"\t\tnum_possible_bins_slimit(H={H}, k={k}, s={s})")
    total = 0
    max_j = H // (s + 1)
    for j in range(0, max_j + 1):
        top = H - (s + 1) * j + k - 1
        if top < k - 1:
            continue  # binomial coefficient becomes zero
        term = (-1) ** j * math.comb(k, j) * math.comb(top, k - 1)
        total += term
    return total


def probability_n_bins_empty_slimit_old(b, H, n, s):
    """Compute the probability that exactly n bins are empty."""
    print(f"probability_n_bins_empty_slimit(b={b}, H={H}, n={n})")
    k = b - n  # Bins that must have at least one ball

    if H == 0 and k == 0:
        return 1.0

    if k <= 0 or H < k:
        return 0.0

    # Number of ways to distribute H balls such that exactly k bins have between 1 and s balls
    ways_to_fill_k_bins = math.comb(b, k) * num_possible_nonempty_bins_slimit(H, k, s)
    sn = num_possible_nonempty_bins_slimit(H, k, s)
    mc = math.comb(b, k)
    print(f"\tways_to_fill_k_bins = {mc} * {sn} = {ways_to_fill_k_bins}")

    # Total number of ways to distribute H indistinguishable balls into b distinguishable bins
    total_ways = num_possible_bins_slimit(H, b, s)
    print(f"\ttotal_ways = {total_ways}")

    # Probability that exactly n bins are empty
    probability = ways_to_fill_k_bins / total_ways

    return probability

def inclision_exclusion_N(H, b, s):
    """
    Count the number of ways to distribute H identical balls into b distinguishable bins
    such that each bin has at least 1 and at most s balls.
    """
    print(f"\t\tnum_possible_nonempty_bins_slimit(H={H}, b={b}, s={s})")
    total = 0
    for j in range(b + 1):
        top = H - 1 - j * s
        if top < b - 1:
            continue  # Binomial coefficient is 0 if top < bottom
        term = (-1) ** j * math.comb(b, j) * math.comb(top, b - 1)
        total += term
    return total


def probability_n_workers_almost_ready_slimit(b, H, n, s):
    """
    Compute the probability that exactly n of b busy workers are almost ready (exactly one stage remaining).
    """
    print(f"probability_n_bins_empty_slimit(b={b}, H={H}, n={n})")
    k = b - n  # workers that are backlogged (at least two stages remaining)

    if H == b and k == 0:
        return 1.0

    if k <= 0 or H < (b+k):
        return 0.0

    # Number of ways to stack (H-b) stages onto k=b-n backlogged workers
    # (each workers gets between 1 and s-1 additional stages)
    ways_to_stack_k_workers = math.comb(b, k) * inclision_exclusion_N(H-b, b-n, s-1)
    sn = inclision_exclusion_N(H-b, b-n, s-1)
    mc = math.comb(b, k)
    print(f"\tways_to_stack_k_workers = {mc} * {sn} = {ways_to_stack_k_workers}")

    # Total number of ways to distribute H indistinguishable balls into b distinguishable bins
    total_ways = inclision_exclusion_N(H, b, s)
    print(f"\ttotal_ways = {total_ways}")

    # Probability that exactly n bins are empty
    probability = ways_to_stack_k_workers / total_ways

    return probability

def total_departure_probability_slimit(b, H, s):
    min_backlogged_workers = math.ceil((H-b)/(s-1))
    min_singletask_workers = max(1, b - (H-b))
    print(f"min_backlogged_workers={min_backlogged_workers}\t min_singletask_workers={min_singletask_workers}")
    ss = 0.0
    for i in range(min_singletask_workers, b-min_backlogged_workers+1):
        # need to use (s-1) along with (H-b) here because we ensure that each busy
        # worker has at least one stage.
        #pr = probability_n_bins_empty_slimit_old(b, H-b, i, s-1)
        pr = probability_n_workers_almost_ready_slimit(b, H, i, s)
        print(f"for {i} non-backlogged workers the prob is {pr:.6f}")
        ns = (i/b) * pr
        print(f"additional sum = {ns}")
        ss += ns
    return ss


def traverse_states_maxstack(states: dict[tuple,SysState], S: SysState, lmbda, mu, take_frac = 0.5) -> object:
    unvisited = {(S.l,S.H)}
    states[S.l,S.H] = S
    print("initial state: "+str(S))
    while len(unvisited) > 0:
        print("len(unvisited)=",len(unvisited), unvisited)
        (l1,H1) = unvisited.pop()
        S1 = states.get((l1,H1))
        print(f"\nTRAVERSING STATE: {S1}")

        # there is only one task(stage) completion transition here
        # it reduces H by one, and optionally increases l
        # XXX - Do I need to check any conditions on H here?
        if S1.l < S1.s:
            #task_departure_prob = total_departure_probability(S1.b, S1.H, S1.s)
            task_departure_prob = total_departure_probability_slimit(S1.b, S1.H, S1.s)
            print(f"\ttotal_departure_probability(b={S1.b}, H={S1.H}, s={S1.s}) = {task_departure_prob}")

            if (task_departure_prob < 1.0):
                print("task stage completion transition: NO task departure")

                l2 = S1.l
                H2 = H1 - 1
                if H2 >= (S1.s - l2):
                    if (l2,H2) not in states:
                        states[l2,H2] = SysState(S1.s, l2, H2)
                    S2 = states.get((l2,H2))
                    rate_factor = (1.0 - task_departure_prob) * S1.b
                    print("\t S1 rate_factor = ", rate_factor)
                    S1.add_transition(S2, mu * rate_factor, task_departure=True)
                    if not S2.visited:
                        unvisited.add((l2,H2))

            if (task_departure_prob > 0.0):
                print("task stage completion transition: WITH task departure")

                l2 = S1.l + 1
                H2 = H1 - 1
                if H2 >= (S1.s - l2):
                    if (l2,H2) not in states:
                        states[l2,H2] = SysState(S1.s, l2, H2)
                    S2 = states.get((l2,H2))
                    rate_factor = task_departure_prob * S1.b
                    print("\t S1 rate_factor = ", rate_factor)
                    S1.add_transition(S2, mu * rate_factor, task_departure=True)
                    if not S2.visited:
                        unvisited.add((l2,H2))

        # finally the job arrival transition
        if S1.l > 0:
            # check if we can add a job without exceeding the limit of H <= (b+1)*s
            if (S1.H + S1.s) <= ((S1.b+1) * S1.s):
                print("job arrival transition")
                parallelism = min(S1.l * take_frac, S1.s * take_frac)
                print("\t S2 parallelism = ", S1.l, "*", take_frac, " = ", (S1.l * take_frac), "=",
                      math.ceil(S1.l * take_frac), " = ", parallelism)
                l2 = S1.l - math.ceil(parallelism)
                H2 = H1 + S1.s
                if (l2,H2) not in states:
                    states[l2,H2] = SysState(S1.s, l2, H2)
                S2 = states.get((l2,H2))
                S1.add_transition(S2, lmbda, job_start=True)
                if not S2.visited:
                    unvisited.add((l2,H2))
            else:
                print(f"ERROR: inconsistent system state for arrival: {S1}")
                sys.exit(0)


        S1.visited = True
        print("len(unvisited) = ", len(unvisited), list(unvisited))


def lstate_condense(s:int, queue:int, states: dict[tuple,SysState], ctpi_dense):
    pi = np.zeros((2,s+1+queue))
    for l in range(-queue,s+1):
        pi[0,l+queue] = l
    for (l,h), S in states.items():
        print("\t aggregate state ("+str(l)+","+str(h)+") to state l="+str(l)+"\tpr="+str(ctpi_dense[S.state_id]))
        pi[1,l+queue] += ctpi_dense[S.state_id]
    # also sum up the probabability of being in a queue-backlogged state to the l=0 state.
    for i in range(0,queue):
        pi[1,queue] += pi[1,i]
    return pi

def add_queueing_states(states: dict[tuple,SysState], lmbda:float, mu:float, queue:int, s:int) -> object:
    # for each height from s(min number of stages in service to have l=0)
    # to s*s (max number of stages in service)
    print(f"BUILD QUEUE...")
    for H in range(s, s*s +1):
        print(f"QUEUE: building queue layer H={H}")
        # and for each height, build out the queue to length Q
        S1 = states.get((0,H))
        # the task departure probabilities for the base state will apply
        # across this layer of the queue
        task_departure_prob = total_departure_probability(S1.b, S1.H, S1.s)
        print(f"\tQUEUE: total_departure_probability(b={S1.b}, H={S1.H}, s={S1.s}) = {task_departure_prob}")
        
        for ll in range(0, -queue-1, -1):
            # the stage completion transition
            # this copies logic from traverse_states(), which is not ideal...
            print(f"\nQUEUE: iterating ll={ll}")
            if S1.l < 0:
                if (task_departure_prob < 1.0):
                    print("\t\tQUEUE: task stage completion transition: NO task departure")
                    l2 = S1.l
                    H2 = H - 1
                    if (l2,H2) not in states:
                        states[l2,H2] = SysState(S1.s, l2, H2)
                    S2 = states.get((l2,H2))
                    rate_factor = (1.0 - task_departure_prob) * S1.b
                    print("\t\t S1 rate_factor = ", rate_factor)
                    S1.add_transition(S2, mu * rate_factor, task_departure=True)

                if (task_departure_prob > 0.0):
                    print("\t\tQUEUE: task stage completion transition: WITH task departure AND a dequeue")
                    l2 = S1.l + 1
                    H2 = H + s - 1  # dequeue a task
                    if (l2,H2) not in states:
                        states[l2,H2] = SysState(S1.s, l2, H2)
                    S2 = states.get((l2,H2))
                    rate_factor = task_departure_prob * S1.b
                    print("\t\t S1 rate_factor = ", rate_factor)
                    S1.add_transition(S2, mu * rate_factor, task_departure=True)
            
            # finally the job arrival transition
            # just extends the queue
            if S1.l > -queue:
                print("\t\tQUEUE: job arrival transition")
                l2 = S1.l - 1
                H2 = H
                if (l2,H2) not in states:
                    states[l2,H2] = SysState(S1.s, l2, H2)
                S2 = states.get((l2,H2))
                S1.add_transition(S2, lmbda, job_start=True)
            S1.visited = True
            S1 = S2


def create_markov_matrix(states):
    m = np.zeros((len(states), len(states)))
    for vec, S in states.items():
        print("vec:",vec, "S:",S)
        for vec2, tr in S.transitions.items():
            print("\tvec2:",vec2, "tr:",tr)
            m[tr.S1.state_id, tr.S2.state_id] = tr.weight
    return m

def create_rate_matrix(states):
    print("\ncreate_rate_matrix()")
    #offset = min(states.keys())
    Q = np.zeros((len(states), len(states)))
    for (l1,h1), S1 in states.items():
        print("l1:",l1, "h1:", h1, "S1:",S1)
        for (l2,h2), tr in S1.transitions.items():
            print("\tl2:",l2, "h2:",h2, "tr:",tr)
            Q[tr.S1.state_id, tr.S2.state_id] = tr.weight
            #Q[tr.S1.l-offset, tr.S2.l-offset] = tr.weight
    for l, S in states.items():
        rs = sum(Q[S.state_id, :])
        Q[S.state_id, S.state_id] = -rs
        #rs = sum(Q[S.l-offset, :])
        #Q[S.l-offset, S.l-offset] = -rs
    return Q

def matrix_power(m,n):
    # computes m^(2^n)
    # matrices not positive-recurrent
    # alternating cycle of stationary distributions
    # https://jyyuan.wordpress.com/2014/03/23/on-stationary-distributions-of-discrete-markov-chains/
    P = m.T
    for i in range(n):
        P = np.matmul(P,P)
    return P

def compute_stationary_iterative(m, n):
    P = m.T
    size = P.shape[0]
    pi = np.zeros(size);  pi1 = np.zeros(size)
    pi[0] = 1;
    for i in range(n):
        pi[0] = 1;

def compute_rate_stationary(Q):
    dimension = Q.shape[0]
    b = np.zeros((dimension,1))
    Qt = Q.transpose()
    rk = np.linalg.matrix_rank(Qt)
    print("\ncompute_rate_stationary()\ndimension:",dimension, "\nrank:",rk, "\nb:\n",b, "\nQt:\n", Qt)
    return np.linalg.solve(Qt,b).transpose

def compute_steady_state(m):
    # https://vknight.org/blog/posts/continuous-time-markov-chains/
    print("compute_steady_state()")
    dimension = m.shape[0]
    MM = np.vstack((m.transpose()[:-1], np.ones(dimension)))
    b = np.vstack((np.zeros((dimension - 1, 1)), [1]))
    return np.linalg.solve(MM, b).transpose()[0]



def compute_stationary_linear(m):
    print("\nshape=",m.shape)
    dimension = m.shape[0]
    #MM = np.vstack((m.transpose()[:-1] - np.identity(dimension), np.ones(dimension)))
    MM = np.vstack((m.transpose(), np.ones(dimension)))
    print("\nMM=\n",MM)
    b = np.vstack((np.zeros((dimension - 1, 1)), [1]))
    print("\nb=\n",b)
    soln = np.linalg.solve(MM, b)
    print("\nsoln=\n",soln)

def build_cycle_classes(states:dict[tuple,SysState], m, m210):
    toler = 1e-15
    classes = {}
    for vec, S in states.items():
        # we could start in this state, and apply the transition matrix until the probability
        # of coming back is positive.  That would work in our case, but in general it is not valid,
        # because as long as the gcd of the cycles is 1, then the stationary distr. will exist.
        # Instead, we look at the power of the transition matrix.
        for j in range(len(states)):
            if m210[S.state_id, j] > toler:
                if j not in classes:
                    classes[j] = []
                classes[j].append(S)
                S.cycle_class = j
                break
        if S.cycle_class is None:
            print("ERROR: state was not found in any cycle class: ", S)
    for i in range(len(classes)):
        print("Cycle class ",str(i))
        for S in classes[i]:
            print("\t",S)

    # Add up the transitions between cycle classes
    # We know already from the structure of the cycle, that all the transitions
    # of all states of one class, go to states of the next class.
    # Therefore, the  weight we compute here will always be the number of states in
    # the class.  When we normalize it, it will just be a permuted identity matrix.
    num_classes = len(classes)
    class_transitions = np.zeros((num_classes, num_classes))
    for vec1, S in states.items():
        for vec2, tr in S.transitions.items():
            class_transitions[S.cycle_class, tr.S2.cycle_class] += tr.weight
    row_sums = class_transitions.sum(axis=1)
    class_transitions = class_transitions / row_sums[:, np.newaxis]
    print("Class transitions:\n",class_transitions)

    return classes, class_transitions



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", '--num_workers', type=int, default=12)
    parser.add_argument("-m", '--mu', type=float, default=1.0)
    parser.add_argument("-l", '--lmbda', type=float, default=0.5)
    parser.add_argument("-f", '--takefrac', type=float, default=0.5)
    parser.add_argument("-q", '--queue', type=int, default=0)
    args = parser.parse_args()

    s = args.num_workers
    mu = args.mu
    lmbda = args.lmbda
    queue = args.queue
    take_frac = args.takefrac

    S0 = SysState(s, s, 0)
    print(S0)
    # S0.long_form()
    states = {(S0.l, S0.H): S0}
    traverse_states_maxstack(states, S0, lmbda, mu, take_frac = take_frac)
    if queue > 0:
        add_queueing_states(states, lmbda, mu, queue, s)

    print("\nNumber of states: ", str(len(states)))
    #sys.exit()

    # compute stationary distribution by linearly solving the CTMC rate matrix
    Q_dense = create_rate_matrix(states)
    # Q = create_rate_matrix_sparse(states)
    print("\nweight matrix dense:\n", Q_dense)

    ##print("\nweight matrix:\n", Q.todense())
    ctpi_dense = compute_steady_state(Q_dense)

    # ctpi_trick = compute_rate_stationary_sparse_trick(Q)
    # ctpi = compute_rate_stationary_sparse(Q)
    print("\n ct pi dense:\n", ctpi_dense)
    # print("\n ct pi trick:\n", ctpi_trick)
    # print("\n ct pi:\n", ctpi)
    print("\n Qt * ct pi dense:\n", np.dot(Q_dense.transpose(), ctpi_dense.transpose()))
    # print("\n Qt * ct pi:\n", Q.transpose().dot(ctpi.transpose()))
    lstate_pi = lstate_condense(s, queue, states, ctpi_dense)
    print("\n lstate_pi:\n", lstate_pi)
    # sys.exit(0)

    plt.plot(lstate_pi[0, :], lstate_pi[1, :])
    if queue > 0:
        plt.axvline(x=0, color='red')
    plt.grid()
    plt.xlabel('queue length | idle workers')
    plt.ylabel('P(state)')
    #plt.show()

    # get corresponding simulator data
    lmbda_string = re.sub(r'[\.]', '', str(lmbda))
    filename = "../pdistr-tfb-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    #filename = "../pdistr-tfb10-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    if os.path.isfile(filename):
        pdist_data = np.zeros((s + 1, 2), float)
        pcount = 0
        with open(filename) as pdist_file:
            reader = csv.reader(pdist_file, delimiter='\t')
            for row in reader:
                pdist_data[int(row[3]), 1] += 1
                pcount += 1
        for i in range(0, s + 1):
            pdist_data[i, 0] = i
            pdist_data[i, 1] /= pcount

        print(pdist_data)
        plt.plot(pdist_data[:, 0], pdist_data[:, 1], '--')

    # get simulator data with departure barrier
    filename = "../pdistr-tfbb-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    #filename = "../pdistr-tfbb10-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    if os.path.isfile(filename):
        pdist_bb_data = np.zeros((s+1, 2), float)
        pcount = 0
        with open(filename) as pdist_bb_file:
            reader = csv.reader(pdist_bb_file, delimiter='\t')
            for row in reader:
                pdist_bb_data[int(row[3]), 1] += 1
                pcount += 1
        for i in range(0,s+1):
            pdist_bb_data[i, 0] = i
            pdist_bb_data[i, 1] /= pcount

        print(pdist_bb_data)
        plt.plot(pdist_bb_data[:,0], pdist_bb_data[:,1], '-.')

    plt.show()
    # compute the departure rate
    # In the backlogged steady state, what fraction of task completions lead to a job departure?
    # In the steady state, the starting rate has to be the same as the departure rate, so we
    # compute that too as a sanity check.
    #XXX Made a mistake earlier and considered the product of the stationary distribution and the
    # transition weight.  The problem is that the transition weights are normalized rel to the number
    # of tasks running in that state, so are on different rate scales.  You can't just add them together
    # without converting them to a common denominator.  Need to look at the raw rates, because those are
    # in the common units of \mu (task service rate).
    print("\nNumber of states: ", str(len(states)))

    # use the steady state distribution to predict the mean sojourn time
    def cdf_erlk(x, k, lambd):
        """CDF of Erlang-k distribution with rate parameter lambda."""
        # Regularized lower incomplete gamma function
        return gammainc(k, lambd * x)

    def pdf_erlk(x, k, lambd):
        """PDF of Erlang-k distribution with rate parameter lambda."""
        if x < 0:
            return 0
        return (lambd ** k * x ** (k - 1) * np.exp(-lambd * x)) / gamma(k)

    def pdf_max_erlk(x, n, k, lambd):
        """PDF of the maximum of n independent Erlang-k random variables."""
        if x < 0:
            return 0
        cdf_x = cdf_erlk(x, k, lambd)
        pdf_x = pdf_erlk(x, k, lambd)
        return n * (cdf_x ** (n - 1)) * pdf_x

    def expected_value_max_erlk(n, k, lambd):
        """Expected value of the maximum of n Erlang-k random variables."""
        integral, _ = quad(lambda x: x * pdf_max_erlk(x, n, k, lambd), 0, np.inf)
        return integral

    expected_runtime_sum = 0.0
    ctpi_sum = 0.0
    expected_runtime_queue_sum = 0.0
    ctpi_queue_sum = 0.0
    for l in range(0,s+queue):
        # for now ignore the small ammt of probability in the queue
        if l > queue:
            ctpi_sum += ctpi_dense[l]
        # There are z=max(1,l/2) composite tasks.  Each contains
        # (s/z) exponential stages.  To compute the expected runtime,
        # we need the expectation of the max order statistic of the z
        # composite tasks.
        num_tasks = max(1,l/2)
        num_stages = s/num_tasks
        print("num_tasks=", str(num_tasks), "num_stages=", str(num_stages))
        ert = expected_value_max_erlk(num_tasks, num_stages, mu)
        expected_runtime_sum += ctpi_dense[l+queue] * ert
    print("ctpi_sum=", ctpi_sum)
    print("expected_runtime_sum=", expected_runtime_sum)

    plt.show()

# ======================================
# ======================================
# ======================================

if __name__ == "__main__":
    main()

