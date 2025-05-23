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
    h = 0
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
            return "Transition: ("+str(self.S1.l)+","+str(self.S1.h)+") --> ("+str(self.S2.l)+","+str(self.S2.h)+") \tweight= "+str(self.weight)+"\tstart="+str(self.job_start)+"\tdeparture="+str(self.task_departure)


    def __init__(self, s, l, h):
        self.state_id = SysState.__global_id_counter__
        SysState.__global_id_counter__ += 1
        self.s = s
        self.b = min(s - l, s)
        self.l = l
        self.h = h
        self.transitions = {}

        # sanity check
        print("\ncreate SysState: (S, B, L, H) = (", self.s, self.b, self.l, self.h, ")  ID=", self.state_id)

    def __str__(self):
        return "SysState: (s,b,l, h)=("+str(self.s)+","+str(self.b)+","+str(self.l)+","+str(self.h)+")"

    def add_transition(self, S2, weight, job_start=False, task_departure=False):
        self.transitions[S2.l,S2.h] = SysState.Transition(self, S2, weight, job_start, task_departure)
        print("\tadd_transition (", self.l, self.h, ") --> (", S2.l, S2.h, ")  weight=",weight)


def traverse_states_maxstack(states: dict[tuple,SysState], S: SysState, lmbda, mu, take_frac = 0.5) -> object:
    unvisited = {(S.l,S.h)}
    states[S.l,S.h] = S
    print("initial state: "+str(S))
    while len(unvisited) > 0:
        #print("len(unvisited)=",len(unvisited), unvisited)
        (l1,h1) = unvisited.pop()
        S1 = states.get((l1,h1))

        # there is a "spacing" task completion transition (except the zero state)
        # don't allow transitions into the l=s state unless the stack height is back down to 1
        #if S1.l < S1.s and (S1.l < S1.s-1 or S1.h == 1):
        # try with no inter-stack lateral transitions
        if S1.l < S1.s and S1.h == 1:
            #if S1.l < S1.s and (S1.l < S1.s - 1 or S1.h == 1):
            print("task 'spacing' completion transition")

            l2 = S1.l + 1
            h2 = h1
            if (l2,h2) not in states:
                states[l2,h2] = SysState(S1.s, l2, h2)
            S2 = states.get((l2,h2))
            # factor of S1.l because we are looking for the first task to finish out of
            # the S1.b that are running, so it is the 1. order statistic.
            #S1.add_transition(S2, max(S1.b,1)*mu*parallelism/S1.s, task_departure=True)
            #rate_factor = (S1.s-S1.l) / S1.s
            rate_factor = (S1.s - S1.l)
            #rate_factor = (S1.s - S1.l) / S1.h
            #rate_factor = np.sqrt(S1.s - S1.l)
            print("\t S1 rate_factor = ", rate_factor)
            S1.add_transition(S2, mu * rate_factor, task_departure=True)
            if not S2.visited:
                unvisited.add((l2,h2))

        # there is a max-stack task departure transition
        # these are purely exponential
        # though we model a single one of them, and there could be several,
        # and the associated variance and order statistics...?
        if S1.h > 1:
            print("task 'max-stack' completion transition")
            l2 = S1.l
            h2 = S1.h - 1
            if (l2,h2) not in states:
                states[l2,h2] = SysState(S1.s, l2, h2)
            S2 = states.get((l2,h2))
            rate_factor = 1
            print("\t S1 rate_factor = ", rate_factor)
            S1.add_transition(S2, mu * rate_factor, task_departure=True)
            if not S2.visited:
                unvisited.add((l2,h2))

        # finally the job arrival transition
        if S1.l > 0:
            print("job arrival transition")
            parallelism = min(S1.l * take_frac, S1.s * take_frac)
            print("\t S2 parallelism = ", S1.l, "*", take_frac, " = ", (S1.l * take_frac), "=",
                  math.ceil(S1.l * take_frac), " = ", parallelism)
            l2 = S1.l - math.ceil(parallelism)
            h2 = round(S1.s / math.ceil(parallelism))  # round, or ceil here?  We want it to be the max stack...
            if (l2,h2) not in states:
                states[l2,h2] = SysState(S1.s, l2, h2)
            S2 = states.get((l2,h2))
            S1.add_transition(S2, lmbda, job_start=True)
            if not S2.visited:
                unvisited.add((l2,h2))

        S1.visited = True
        print("len(unvisited) = ", len(unvisited), list(unvisited))


def lstate_condense(s:int, queue:int, states: dict[tuple,SysState], ctpi_dense):
    pi = np.zeros((2,s+1+queue))
    for l in range(-queue,s+1):
        pi[0,l+queue] = l
    for (l,h), S in states.items():
        print("\t aggregate state ("+str(l)+","+str(h)+") to state l="+str(l)+"\tpr="+str(ctpi_dense[S.state_id]))
        pi[1,l+queue] += ctpi_dense[S.state_id]
    return pi

def add_queueing_states(states: dict[tuple,SysState], lmbda:float, mu:float, queue:int) -> object:
    # get the state with no idle workers
    S1 = states.get(0)
    # queued states are labeled with negative l
    # intuition: less than 0 workers are available.  There is worker debt.
    for l2 in range(-1, -queue, -1):
        S2 = SysState(S1.s, l2)
        S1.add_transition(S2, lmbda, job_start=True)
        # rate of individual task/jobs is mu/s, but with s of them running, the
        # min order statistic rate is s*mu/s = mu.
        S2.add_transition(S1, mu, task_departure=True)
        states[S2.l] = S2
        S2.visited = True
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

    S0 = SysState(s, s, 1)
    print(S0)
    # S0.long_form()
    states = {(S0.l, S0.h): S0}
    traverse_states_maxstack(states, S0, lmbda, mu, take_frac = take_frac)
    if queue > 0:
        add_queueing_states(states, lmbda, mu, queue)

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
    if os.path.isfile(filename):
        pdist_data = np.zeros((s + 1, 2), float)
        pcount = 0
        with open(filename) as pdist_file:
            reader = csv.reader(pdist_file, delimiter='\t')
            for row in reader:
                pdist_data[int(row[3]), 1] += 1
                pcount += 1
        for i in range(0, s + 1):
            pdist_data[i, 0] = i + queue
            pdist_data[i, 1] /= pcount

        print(pdist_data)
        plt.plot(pdist_data[:, 0], pdist_data[:, 1], '--')
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
    for l in range(0,s):
        # for now ignore the small ammt of probability in the queue
        ctpi_sum += ctpi_dense[l+queue]
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

