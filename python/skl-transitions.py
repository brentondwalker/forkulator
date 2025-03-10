#!/usr/bin/env python3

"""
Compute job start/departure rate correction factor for (s,k,l) parallel system with start barriers.

In an (s,k,l) parallel system with start barriers, tasks of each job must all start simultaneously.
S = number of workers
k = number of tasks per job
l = number of tasks that must complete for the job to depart

We assume that straggler tasks are killed when their job departs.
We assume the tasks have i.i.d. exponential service processes.

A SysState of such a system is the collection of running jobs, and how many unfinished tasks each job has.
The compact representation of this is a vector, where entry i is the number of jobs with ((k-l+1)+i) running tasks.
These states and their transitions form a Markov Model.
No job can have (k-l) or fewer tasks running, because it would just kill its stragglers and depart.
Because different states have different numbers of tasks running, they have different holding times.
For example (s,k,l)=(12,3,2)
State (0,4) has 0*2 + 4*3 = 12 tasks running, so the holding time is exp(1/12)
State (1,3) has 1*2 + 3*3 = 11 tasks running, so the holding time is exp(1/11)
State (5,0) has 5*2 + 0*3 = 10 tasks running, so the holding time is exp(1/10)

Job departures correspond to certain transitions.  To find the rate of job departure we have to look
at the stationary distribution on the states, taking into account the holding times, times the probability
of the transitions.
"""


import math
import sys
import numpy as np
np.set_printoptions(edgeitems=30, linewidth=100000, formatter={'float': lambda x: "{0:0.3f}".format(x)})
import argparse
import scipy.sparse
import scipy.sparse.linalg
from scipy.sparse import csr_matrix, csc_matrix
from scipy.sparse.linalg import spsolve

### for older versions of python
# from typing import Dict


class SysState:
    """The state of an (s,k,l) barrier-mode parallel system."""
    __global_id_counter__ = 0

    state_id = 0
    state_len = 0
    s = 0
    k = 0
    l = 0
    bmax = 0
    bmin = 0
    Svec = None
    transitions = None
    num_jobs = 0
    num_tasks = 0
    visited = False
    cycle_class = None

    class Transition:
        """A transition between two SysStates

        A transition happens when one of the running tasks finishes.
        The num_tasks here is the number of tasks with job_size tasks running.
        To get assign the weight of this transition you divide the num_tasks
        by the total number of tasks in S1.  That is already computed in the
        calling function."""
        S1 = None
        S2 = None
        num_tasks = 0
        job_size = 0
        weight = 0.0
        job_start = False
        job_departure = False

        def __init__(self, S1, S2, num_tasks, job_size, weight, job_start=False, job_departure = False):
            self.S1 = S1
            self.S2 = S2
            self.num_tasks = num_tasks
            self.job_size = job_size
            self.weight = weight
            self.job_start = job_start
            self.job_departure = job_departure

        def __str__(self):
            return "Transition: "+str(self.S1.Svec)+" --> "+str(self.S2.Svec)+"\tweight= "+str(self.weight)+"\tstart="+str(self.job_start)+"\tdeparture="+str(self.job_departure)


    def __init__(self, s, k, l, Svec):
        self.state_id = SysState.__global_id_counter__
        SysState.__global_id_counter__ += 1
        self.state_len = k - l + 1
        self.s = s
        self.k = k
        self.l = l
        self.bmin = math.floor(s / k)
        self.bmax = math.floor(s / (k - l + 1))
        self.transitions = {}

        # sanity check
        print("\ncreate SysState: Svec = ", Svec, "ID=", self.state_id)
        if self.state_vector_length(s, k, l) != len(Svec):
            print("ERROR: the state vector is the wrong length!!")
            sys.exit(-1)

        self.Svec = Svec
        self.num_jobs = sum(list(self.Svec))
        for i in range(len(Svec)):
            self.num_tasks += (i + (k-l+1)) * Svec[i]
        print("  jobs=", self.num_jobs, "  tasks=", self.num_tasks)
        if self.num_tasks > s:
            print("ERROR: creating invalid system state with too many tasks running. ", self.num_tasks, s)
            sys.exit(-1)

    def state_vector_length(self, s, k, l):
        """The position in the state vector represents the number of tasks in the job.
        The value is the number of jobs of that size.
        The length of the state vector is the number of possible job sizes:
        (k-l+1), ..., (k-l+(l-1)), (k-l+(l))=k
        So the length is l."""
        return l

    def __str__(self):
        return "SysState: (s,k,l)=("+str(self.s)+","+str(self.k)+","+str(self.l)+")   "+ str(self.Svec) + "  " + str(self.long_form())

    def long_form(self):
        lf = [] + [0]*(self.bmax - self.num_jobs)
        for i in range(len(self.Svec)):
            lf += [i+(self.k-self.l+1)]*self.Svec[i]
        #[0]*(self.s-self.jobs)
        #print("long_form=", lf)
        return lf

    def add_transition(self, S2, job_size, job_start, job_departure):
        num_tasks = job_size * self.Svec[job_size - (self.k - self.l +1)]
        self.transitions[S2.Svec] = SysState.Transition(self, S2, num_tasks, job_size, num_tasks / self.num_tasks, job_start, job_departure)
        print("\tadd_transition ", self.Svec, "-->", S2.Svec, num_tasks, self.num_tasks, (num_tasks/self.num_tasks))


def traverse_states(states: dict[tuple,SysState], S: SysState) -> object:
    if S.visited:
        return
    S.visited = True
    # determine which transitions this state can have
    for r in range(len(S.Svec)):
        if S.Svec[r] > 0:
            job_start = False
            job_departure = False
            job_size = r + (S.k - S.l + 1)
            num_tasks = S.num_tasks
            # now consider what happens when a task in this set finishes
            new_vec = list(S.Svec)
            # there is one less job of this size
            new_vec[r] -= 1
            # if the job size > (k-l+1), then the job still exists, but is smaller
            if job_size > (S.k - S.l + 1):
                #print("\t**TASK completion from ",S.Svec)
                new_vec[r-1] += 1
                num_tasks -= 1
            else:
                # otherwise a job departs
                #print("\t**JOB departure from ",S.Svec)
                num_tasks -= (S.k - S.l + 1)
                job_departure = True
            # if enough tasks have finished, and new job will start
            # since l<k, the completion of one task can only free up enough capacity for one new job
            if (S.s - num_tasks) >= S.k:
                #print("\t**JOB start from ", S.Svec)
                #print("\t\t", S.s, num_tasks, S.k)
                new_vec[-1] += 1
                num_tasks += S.k
                job_start = True
            # create the state we transition to, if it does not already exist
            S2vec = tuple(new_vec)
            if S2vec not in states:
                states[S2vec] = SysState(S.s, S.k, S.l, S2vec)
            S2 = states.get(S2vec)
            S.add_transition(S2, job_size, job_start, job_departure)
            traverse_states(states, S2)


def traverse_states_nonrec(states: dict[tuple,SysState], S: SysState) -> object:
    unvisited = {S}
    states[S.Svec] = S
    while len(unvisited) > 0:
        #print("len(unvisited)=",len(unvisited), unvisited)
        S1 = unvisited.pop()
        # compute all transitions from S
        for r in range(len(S1.Svec)):
            if S1.Svec[r] > 0:
                job_start = False
                job_departure = False
                job_size = r + (S1.k - S1.l + 1)
                num_tasks = S1.num_tasks
                # now consider what happens when a task in this set finishes
                new_vec = list(S1.Svec)
                # there is one less job of this size
                new_vec[r] -= 1
                # if the job size > (k-l+1), then the job still exists, but is smaller
                if job_size > (S1.k - S1.l + 1):
                    # print("\t**TASK completion from ",S.Svec)
                    new_vec[r - 1] += 1
                    num_tasks -= 1
                else:
                    # otherwise a job departs
                    # print("\t**JOB departure from ",S.Svec)
                    num_tasks -= (S1.k - S1.l + 1)
                    job_departure = True
                # if enough tasks have finished, and new job will start
                # since l<k, the completion of one task can only free up enough capacity for one new job
                if (S1.s - num_tasks) >= S1.k:
                    # print("\t**JOB start from ", S.Svec)
                    # print("\t\t", S.s, num_tasks, S.k)
                    new_vec[-1] += 1
                    num_tasks += S.k
                    job_start = True
                # create the state we transition to, if it does not already exist
                S2vec = tuple(new_vec)
                if S2vec not in states:
                    states[S2vec] = SysState(S.s, S.k, S.l, S2vec)
                S2 = states.get(S2vec)
                S1.add_transition(S2, job_size, job_start, job_departure)
                if not S2.visited:
                    unvisited.add(S2)
        # for all new states, if they are not visited, add them to unvisited
        S1.visited = True


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
    Q = np.zeros((len(states), len(states)))
    for vec, S in states.items():
        print("vec:",vec, "S:",S)
        for vec2, tr in S.transitions.items():
            print("\tvec2:",vec2, "tr:",tr)
            Q[tr.S1.state_id, tr.S2.state_id] = tr.num_tasks
    for vec, S in states.items():
        rs = sum(Q[S.state_id, :])
        Q[S.state_id, S.state_id] = -rs
    return Q

def create_rate_matrix_sparse(states):
    print("\ncreate_rate_matrix_sparse()")
    Q = csr_matrix((len(states), len(states)))
    for vec, S in states.items():
        #print("vec:",vec, "S:",S)
        for vec2, tr in S.transitions.items():
            #print("\tvec2:",vec2, "tr:",tr)
            Q[tr.S1.state_id, tr.S2.state_id] = tr.num_tasks
    rowsums = Q.sum(axis=1)
    #print("rowsums=\n", rowsums)
    for vec, S in states.items():
        rs = rowsums[S.state_id]
        #print("sum:", rs)
        Q[S.state_id, S.state_id] = -rs
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

def compute_rate_stationary_sparse(m):
    # https://vknight.org/blog/posts/continuous-time-markov-chains/
    print("compute_rate_stationary_sparse()")
    dimension = m.shape[0]
    #MM = scipy.sparse.vstack((m.transpose()[:-1], np.ones(dimension)))
    MM = m.transpose().copy()
    MM[-1,:] = 1
    #print("MM=\n", MM.todense())
    #b = np.vstack((np.zeros((dimension - 1, 1)), [1]))
    b = csc_matrix((dimension, 1))
    b[dimension-1] = 1
    return spsolve(MM, b).transpose()


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
    parser.add_argument("-k", '--num_tasks', type=int, default=3)
    parser.add_argument("-l", '--task_req', type=int, default=2)
    args = parser.parse_args()

    s = args.num_workers
    k = args.num_tasks
    l = args.task_req

    bmin = math.floor(s/k)
    bmax = math.floor(s/(k-l+1))

    #S0 = SysState(s, k, l, (bmax-1,) + (0,)*(k-l))
    S0 = SysState(s, k, l, (0,) * (l-1) + (bmin,))
    print(S0)
    #S0.long_form()
    states = {S0.Svec: S0}
    #traverse_states(states, S0)
    traverse_states_nonrec(states, S0)

    print("\nNumber of states: ", str(len(states)))

    #S = SysState(s, k, l, (bmax-2,) + (0,)*(k-l))
    #print(S.Svec)
    #S.long_form()
    #S = SysState(s, k, l, (bmax-2,) + (0,)*(k-l-1) + (1,))
    #print(S.Svec)
    #S.long_form()

    #m = create_markov_matrix(states)
    #print(m)
    #ss = compute_steady_state(m)
    #print("steady state:\n",ss)
    #ssit = matrix_power(m , 10)
    #print("stationary dist power: \n",ssit)
    #dotted = np.dot(ssit, np.ones(ssit.shape[0]))
    #print("\n dotted:\n", dotted)
    #print("\n normed:\n", dotted/sum(dotted))

    #print("\n  m.T * normed:\n", np.dot(m.T, dotted/sum(dotted)))

    #classes, class_transitions = build_cycle_classes(states, m, ssit)

    #print("\nm.T=\n",m.T)
    ##compute_stationary_linear(m)

    #evals, evecs = np.linalg.eig(m.T)
    #evec1 = -evecs[:, np.isclose(evals, 1)]
    ##normevec1 = evec1 / np.linalg.norm(evec1, ord=1)
    #normevec1 = np.real(evec1 / sum(evec1))
    #print("\nevals:\n", evals, "\nevecs:\n", evecs, "\nevec1:\n", evec1, "\nnormevec1:\n", normevec1)
    #print("\nm.T * normevec1=\n", np.dot(m.T, normevec1))

    # The rate of transitions out of the different states is different because they
    # have different numbers of tasks running.  This is a Continuous-Time Markov Model,
    # and we need to consider the holding time in the states.
    # https://www.probabilitycourse.com/chapter11/11_3_2_stationary_and_limiting_distributions.php
    #ct_stationary = np.zeros(len(states))
    #holding_sum = 0.0
    #for vec, S in states.items():
    #    ct_stationary[S.state_id] = normevec1[S.state_id,0] / S.num_tasks
    #    holding_sum += normevec1[S.state_id,0] / S.num_tasks
    #ct_stationary /= holding_sum
    #print("\nCT Stationary Distr:\n", ct_stationary)

    # compute stationary distribution by linearly solving the CTMC rate matrix
    #Q_dense = create_rate_matrix(states)
    Q = create_rate_matrix_sparse(states)
    #print("\nweight matrix dense:\n", Q_dense)
    ##print("\nweight matrix:\n", Q.todense())
    #ctpi_dense = compute_steady_state(Q_dense)
    ctpi = compute_rate_stationary_sparse(Q)
    #print("\n ct pi dense:\n", ctpi_dense)
    print("\n ct pi:\n", ctpi)
    #print("\n Qt * ct pi dense:\n", np.dot(Q_dense.transpose(), ctpi_dense.transpose()))
    print("\n Qt * ct pi:\n", Q.transpose().dot(ctpi.transpose()))

    # compute the departure rate
    # In the backlogged steady state, what fraction of task completions lead to a job departure?
    # In the steady state, the starting rate has to be the same as the departure rate, so we
    # compute that too as a sanity check.
    #XXX Made a mistake earlier and considered the product of the stationary distribution and the
    # transition weight.  The problem is that the transition weights are normalized rel to the number
    # of tasks running in that state, so are on different rate scales.  You can't just add them together
    # without converting them to a common denominator.  Need to look at the raw rates, because those are
    # in the common units of \mu (task service rate).
    ideal_depart_rate_sk = s/k
    worst_depart_rate_sk = (s-k+1)/k
    ideal_depart_rate_skl = s/l
    worst_depart_rate_skl = (s-k+1)/l
    no_depart_rate = 0.0
    depart_rate = 0.0
    no_start_rate = 0.0
    start_rate = 0.0
    no_depart_weight = 0.0
    depart_weight = 0.0
    no_start_weight = 0.0
    start_weight = 0.0
    for vec, S in states.items():
        for vec2, tr in S.transitions.items():
            if tr.job_departure:
                depart_rate += ctpi[S.state_id] * tr.num_tasks
                depart_weight += ctpi[S.state_id] * tr.weight
            else:
                no_depart_rate += ctpi[S.state_id] * tr.num_tasks
                no_depart_weight += ctpi[S.state_id] * tr.weight
            if tr.job_start:
                start_rate += ctpi[S.state_id] * tr.num_tasks
                start_weight += ctpi[S.state_id] * tr.weight
            else:
                no_start_rate += ctpi[S.state_id] * tr.num_tasks
                no_start_weight += ctpi[S.state_id] * tr.weight

    print("\nideal depart rate (sk=l):", ideal_depart_rate_sk, "ideal depart rate (skl):", ideal_depart_rate_skl)
    print("\nworst depart rate (sk=l):", worst_depart_rate_sk, "worst depart rate (skl):", worst_depart_rate_skl)
    print("\ndepart rate:", depart_rate, "no depart rate:", no_depart_rate)
    print("\nstart rate:", start_rate, "no start rate:", no_start_rate)
    print("\ndepart weight:", depart_weight, "no depart weight:", no_depart_weight)
    print("\nstart weight:", start_weight, "no start weight:", no_start_weight)
    print("\nNumber of states: ", str(len(states)))


# ======================================
# ======================================
# ======================================

if __name__ == "__main__":
    main()

