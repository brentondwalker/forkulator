#!/usr/bin/env python3
import math
import sys
import numpy as np
np.set_printoptions(edgeitems=30, linewidth=100000, formatter={'float': lambda x: "{0:0.3f}".format(x)})
from random import random
from queue import PriorityQueue
from enum import Enum
import itertools
import argparse
from scipy.integrate import odeint


class SysState:
    __global_id_counter__ = 0

    state_id = 0
    statelen = 0
    s = 0
    k = 0
    l = 0
    bmax = 0
    bmin = 0
    Svec = ()
    transitions = None
    num_jobs = 0
    num_tasks = 0
    visited = False

    class Transition:
        S1 = None
        S2 = None
        num_tasks = 0
        weight = 0.0

        def __init__(self, S1, S2, num_tasks, weight):
            self.S1 = S1
            self.S2 = S2
            self.num_tasks = num_tasks
            self.weight = weight

        def __str__(self):
            return "Transition: "+str(self.S1.Svec)+" --> "+str(self.S2.Svec)+"\tweight= "+str(self.weight)


    def __init__(self, s, k, l, Svec):
        self.state_id = SysState.__global_id_counter__
        SysState.__global_id_counter__ += 1
        self.statelen = k - l + 1
        self.s = s
        self.k = k
        self.l = l
        self.bmin = math.floor(s / k)
        self.bmax = math.floor(s / (k - l + 1))
        self.transitions = {}

        # sanity check
        print("create SysState: Svec = ", Svec, "ID=", self.state_id)
        self.Svec = Svec
        self.num_jobs = sum(list(self.Svec))
        for i in range(len(Svec)):
            self.num_tasks += (i + l) * Svec[i]
        print("  jobs=", self.num_jobs, "  tasks=", self.num_tasks)
        if self.num_tasks > s:
            print("ERROR: creating invalid system state with too many tasks running. ", self.num_tasks, s)
            sys.exit(-1)

    def __str__(self):
        return "SysState: (s,k,l)=("+str(self.s)+","+str(self.k)+","+str(self.l)+")   "+ str(self.Svec) + "  " + str(self.long_form())

    def long_form(self):
        lf = [] + [0]*(self.bmax - self.num_jobs)
        for i in range(len(self.Svec)):
            lf += [i+self.l]*self.Svec[i]
        #[0]*(self.s-self.jobs)
        #print("long_form=", lf)
        return lf

    def add_transition(self, S2, job_size):
        num_tasks = job_size * self.Svec[job_size - self.l]
        self.transitions[S2.Svec] = SysState.Transition(self, S2, num_tasks, num_tasks / self.num_tasks)
        print("\tadd_transition ", self.Svec, "-->", S2.Svec, num_tasks, self.num_tasks, (num_tasks/self.num_tasks))
        for vec2, tr in self.transitions.items():
            print("\t\t")


def traverse_states(states: dict[tuple], S: SysState) -> object:
    if S.visited:
        return
    S.visited = True
    # determine which transitions this state can have
    for r in range(len(S.Svec)):
        if S.Svec[r] > 0:
            job_size = r + S.l
            num_tasks = S.num_tasks
            # now consider what happens when a task in this set finishes
            new_vec = list(S.Svec)
            # there is one less job of this size
            new_vec[r] -= 1
            # if the job size > l, then the job still exists, but is smaller
            if job_size > S.l:
                new_vec[r-1] += 1
                num_tasks -= 1
            else:
                num_tasks -= S.l
            # if enough tasks have finished, and new job will start
            # since l<k, the completion of one task can only free up enough capacity for one new job
            if (S.s - num_tasks) >= S.k:
                new_vec[-1] += 1
                num_tasks += S.k
            # create the state we transition to, if it does not already exist
            S2vec = tuple(new_vec)
            if S2vec not in states:
                states[S2vec] = SysState(S.s, S.k, S.l, S2vec)
            S2 = states.get(S2vec)
            S.add_transition(S2, job_size)
            traverse_states(states, S2)

def create_markov_matrix(states):
    m = np.zeros((len(states), len(states)))
    for vec, S in states.items():
        print("vec:",vec, "S:",S)
        for vec2, tr in S.transitions.items():
            print("\tvec2:",vec2, "tr:",tr)
            m[tr.S1.state_id, tr.S2.state_id] = tr.weight
    return m

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

def compute_steady_state(m):
    # https://vknight.org/blog/posts/continuous-time-markov-chains/
    dimension = m.shape[0]
    MM = np.vstack((m.transpose()[:-1], np.ones(dimension)))
    b = np.vstack((np.zeros((dimension - 1, 1)), [1]))
    return np.linalg.solve(MM, b).transpose()[0]

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
    S0 = SysState(s, k, l, (0,) * (k-l-1) + (bmin,))
    print(S0.Svec)
    S0.long_form()
    states = {S0.Svec: S0}
    traverse_states(states, S0)

    print("Number of states: ", str(len(states)))

    #S = SysState(s, k, l, (bmax-2,) + (0,)*(k-l))
    #print(S.Svec)
    #S.long_form()
    #S = SysState(s, k, l, (bmax-2,) + (0,)*(k-l-1) + (1,))
    #print(S.Svec)
    #S.long_form()

    m = create_markov_matrix(states)
    print(m)
    #ss = compute_steady_state(m)
    #print("steady state:\n",ss)
    ssit = matrix_power(m , 10)
    print("stationary dist power: \n",ssit)
    print("\n dotted:\n",np.dot(ssit, np.ones(ssit.shape[0])))


# ======================================
# ======================================
# ======================================

if __name__ == "__main__":
    main()


