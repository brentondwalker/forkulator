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
import scipy.sparse as sp
import scipy.sparse.linalg as spla
from scipy.sparse import csr_matrix, csc_matrix, lil_matrix
from scipy.sparse.linalg import spsolve
from scipy.integrate import quad
from math import exp, factorial
from scipy.special import gammainc, gamma
import matplotlib.pyplot as plt

### for older versions of python
# from typing import Dict


def probability_n_bins_empty(b, H, n):
    """Compute the probability that exactly n bins are empty."""
    #print(f"\t\tprobability_n_bins_empty(b={b}, H={H}, n={n})")
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
        #print(f"TASK DEPARTURE TRANSITION FORCED BECAUSE H <= b  ({H} <= {b})")
        return 1.0
    #if b <= (H/s):
    if (b-1) < ((H-1) / s):
        #print(f"TASK DEPARTURE TRANSITION BLOCKED BECAUSE (b-1)*s >= (H-1)")
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


def lh_matrix_size(s, k, q=0):
    active_states = int((s+1) + (k-1)*s*(s+1)/2)
    queueing_states = int(k*(s-1) + 1) * q
    return active_states + queueing_states


# from the (l,H) coordinate of the state, compute the state number in the CTMC matrix
def lH2state(l, H, s):
    k = s
    if l < 0:
        state_idx = lH2state(0, s*s, s)
        q = -l
        state_idx += (q - 1) * (k * (s-1) + 1) + (H-s) + 1
        #print(f"state_base(l={l}, H={H}, s={s}): \tstate_idx={state_idx}")
    else:
        b = s - l
        if H < (s-l):
            print(f"ERROR: H={H} < b={b} is too small!")
            sys.exit(0)
        if H > b*k:
            print(f"ERROR: H={H} > b*k={b*k} is too large!")
            sys.exit(0)
        # start from the size of the submatrix one would have for a model with (b-1) states
        #state_base = int((b) + (k)*(b-1)*(b)/2)
        state_base = lh_matrix_size(b-1, k)
        # add on the height minus non-feasible states
        # and minus 1 because we index states from zero
        state_idx = state_base + H - (b-1) - 1
        #print(f"state_base(l={l}, H={H}, s={s}): state_base={state_base} \tstate_idx={state_idx}")
    return state_idx


def create_sparse_transition_matrix(s:int, mu:float, lmbda:float, q=0):
    # first compute the number of states
    k = s
    numstates:int = lh_matrix_size(s, k, q)
    print("numstates", numstates)

    # get a big sparse matrix...
    Q = np.zeros((numstates, numstates))

    # for convenience, keep an index of the state
    state_idx = 0

    # iterate over all b values
    # note to self: phi(l,h) = total_departure_probability(b, H, s)
    for b in range(1,s+1):
        l = s - b
        create_D_block(Q, s, b, mu)
        create_B_block(Q, s, b, mu)
        create_A_block(Q, s, b-1, lmbda)

    for b in range(s+1, s+q+1):
        l = s - b
        create_G_block(Q, s, b, mu)
        create_K_block(Q, s, b, mu)
        create_AQ_block(Q, s, b - 1, lmbda)

    #create_truncated_A_block(Q, s, lmbda)
    create_diagonal_entries(Q, s, mu, lmbda, q)

    # check that the rows sum to zero
    print("sum of rows:")
    #with np.printoptions(precision=2):
    print(Q.sum(1))

    return Q


def lH2state_debug(s, q):
    k = s
    for b in range(0, s+q+1):
        true_busy = b if b<s else s
        l = s - b
        for h in range(true_busy, true_busy*k + 1):
            ii = lH2state(l, h, s)
            print(f"lH2state({l}, {h}, {s}) = {ii}")


def create_sparse_transition_matrix_by_rows(s:int, mu:float, lmbda:float, q=0, frac=0.5):
    # first compute the number of states
    k = s
    numstates:int = lh_matrix_size(s, k, q)
    print("numstates", numstates)

    # get a big sparse matrix...
    #Q = np.zeros((numstates, numstates))
    Q = lil_matrix((numstates, numstates))

    # for convenience, keep an index of the state
    state_idx = 0

    for b in range(0, s+q+1):
        true_busy = b if b<s else s
        l = s - b
        for h in range(true_busy, true_busy*k + 1):
            total_outflow = 0.0
            phi = total_departure_probability(true_busy, h, s)
            phi_bar = 1.0 - phi
            i1 = lH2state(l, h, s)

            # arrival transitions
            if b < (s+q):
                #print("** arrival")
                if l > 0:
                    l2 = math.floor(l*frac)
                    h2 = h + k
                else:
                    l2 = l - 1
                    h2 = h
                i2 = lH2state(l2, h2, s)
                Q[i1,i2] = lmbda
                total_outflow += lmbda

            # stage completion transitions
            if h > true_busy:
                #print("** stage")
                i2 = lH2state(l, h-1, s)
                Q[i1, i2] = true_busy * mu * phi_bar
                total_outflow += Q[i1, i2]

            # task departure transitions
            if h <= ((true_busy-1) * k + 1):
                #print("** task")
                l2 = l + 1
                h2 = h-1 if l>=0 else h+k-1
                i2 = lH2state(l2, h2, s)
                Q[i1, i2] = true_busy * mu * phi
                total_outflow += Q[i1, i2]

            Q[i1, i1] = -total_outflow
    return Q


def lstate_condense(s:int, queue:int, ctpi):
    k = s
    pi = np.zeros((2,s+1+queue))
    for l in range(-queue,s+1):
        pi[0,l+queue] = l
    i = 0
    for b in range(0,s+queue+1):
        l = s - b
        pi[0, l + queue] = l
        true_busy = b if b<s else s
        for h in range(true_busy, true_busy*k+1):
            pi[1,l+queue] += ctpi[i]
            ii = lH2state(l, h, s)
            if ii != i:
                print(f"WARNING: lH2state({l}, {h}, {s}) = {ii} != {i}")
            i += 1
    return pi


def sparse_steady_state_ctmc(Q_lil):
    """
    Solve for the steady-state distribution of a CTMC given its generator matrix (sparse LIL).

    Parameters
    ----------
    Q_lil : scipy.sparse.lil_matrix
        Square sparse generator matrix (rows sum to zero).

    Returns
    -------
    pi : np.ndarray
        Steady-state probability vector.
    """
    # Ensure it's in CSR format for solving
    Q = Q_lil.tocsr()
    n = Q.shape[0]

    # Replace one equation with normalization condition: sum(pi) = 1
    A = Q.transpose().tolil()
    A[-1, :] = np.ones(n)

    b = np.zeros(n)
    b[-1] = 1.0

    # Solve the sparse system
    pi = spla.spsolve(A.tocsr(), b)

    return pi


def predict_sojourns(ctpi, s, q, lmbda, mu):
    k = s

    # use the steady state distribution to predict the mean sojourn time
    def cdf_erlk(x, k, lambd):
        """CDF of Erlang-k distribution with rate parameter lambda."""
        # Regularized lower incomplete gamma function
        return gammainc(k, lambd * x)

    def expected_max_erlang(R, k, lam):
        def integrand(x):
            F = cdf_erlk(x, k, lam)
            return 1 - F ** R
        val, _ = quad(integrand, 0, np.inf, limit=200)
        return val

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
    waittime_measure = 0.0
    expected_waittime_sum = 0.0
    expected_runtime_queue_sum = 0.0
    ctpi_queue_sum = 0.0
    for l in range(-q, s+1):
        b = s - l
        true_busy = b if b<s else s
        colsum = 0.0
        for h in range(true_busy, true_busy*k + 1):
            ii = lH2state(l, h, s)
            colsum += ctpi[ii]
            if l <= 0:
                waittime_measure += ctpi[ii]
                #print(f"\texpected waittime({l}, {h}) = {(-l) * h / (mu * s)}")
                expected_waittime_sum += ctpi[ii] * (1-l) * h / (mu * s * s)
        ctpi_sum += colsum
        #print(f"colsum[{l}] = {colsum}")
        num_tasks = max(1,l/2)
        num_stages = s/num_tasks
        ert = expected_max_erlang(num_tasks, num_stages, mu)
        expected_runtime_sum += colsum * ert
        #print(f"l={l}\tnum_tasks={num_tasks}\tnum_stages={num_stages}\tert={ert}")


    #XXX we should include the non-parallel runtime of jobs in the queue
    # weighted by the density of those queue states
    # we can estimate the waiting times of those queued jobs separately
    #print("ctpi_sum=", ctpi_sum)
    #print("expected_runtime_sum=", expected_runtime_sum)
    #print("waittime_measure=", waittime_measure)
    #print("expected_waittime_sum=", expected_waittime_sum)
    #print("expected_sojourn =", expected_runtime_sum+expected_waittime_sum)
    print(f"{s}\t{lmbda}\t{mu}\t{expected_runtime_sum}\t{expected_waittime_sum}\t{expected_runtime_sum+expected_waittime_sum}")

    # At depth l in the queue with total height h, what is the
    # expected waiting time until a job is dequeued?
    # The individual tasks are all approx independent and running in parallel.
    # Treat them as exponential with rate mu/h.
    # Then the min order statistic is exponential with rate mu*s/h.
    # at depth l<0, we need -l tasks to depart before we dequeue,
    # so we need the expectation of an Erlang(-l, mu*s/h), which is -l*mu*s/h


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

    #lH2state_debug(s, queue)
    Q_byrows = create_sparse_transition_matrix_by_rows(s, mu, lmbda, queue, take_frac)
    #with np.printoptions(precision=2):
    #    print(Q_byrows)
    ctpi = sparse_steady_state_ctmc(Q_byrows)
    #print(ctpi)
    lstate_pi = lstate_condense(s, queue, ctpi)
    #print("\n\n")
    print(lstate_pi)
    #print("\n\n")
    predict_sojourns(ctpi, s, queue, lmbda, mu)
    sys.exit(0)

    plt.plot(lstate_pi[0, :], lstate_pi[1, :], label='(H,l) Markov model')
    if queue > 0:
        plt.axvline(x=0, color='red')
    plt.grid()
    plt.xlabel('queue length | idle workers')
    plt.ylabel('P(state)')
    #plt.show()
    #sys.exit()

    # get corresponding simulator data
    lmbda_string = re.sub(r'[\.]', '', str(lmbda))
    filename = "../pdistr-tfb05-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    #filename = "../pdistr-tfb10-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    print("loading file %s...\n" % (filename))
    if os.path.isfile(filename):
        pdist_data = np.zeros((s + 1, 2), float)
        # record queue data going out to -s
        qdist_data = np.zeros((s + 1, 2), float)
        pcount = 0
        with open(filename) as pdist_file:
            reader = csv.reader(pdist_file, delimiter='\t')
            for row in reader:
                if int(row[3]) > 0 or int(row[4]) == 0:
                    pdist_data[int(row[3]), 1] += 1
                if int(row[4]) <= s and int(row[4]) > 0:
                    qdist_data[int(row[4])-1, 1] += 1
                pcount += 1
        for i in range(0, s + 1):
            pdist_data[i, 0] = i
            pdist_data[i, 1] /= pcount
            qdist_data[i, 0] = -i
            qdist_data[i, 1] /= pcount
        qdist_data[0, 1] = pdist_data[0, 1]

        pqdist_data = np.concatenate((pdist_data, qdist_data))
        pqdist_data = pqdist_data[pqdist_data[:, 0].argsort()]
        #print(pqdist_data)
        plt.plot(pqdist_data[:, 0], pqdist_data[:, 1], '--', label='simulation')
        #plt.plot(qdist_data[:, 0], qdist_data[:, 1], '--', label='simulation')
        plt.xlim(-queue, s)

    # get simulator data with departure barrier
    #filename = "../pdistr-tfbb10-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    filename = "../pdistr-tfbb05-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
    print("loading file %s...\n" % (filename))
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

        #print(pdist_bb_data)
        #plt.plot(pdist_bb_data[:,0], pdist_bb_data[:,1], '-.')

    plt.legend(loc='upper left')
    plt.show()

# ======================================
# ======================================
# ======================================

if __name__ == "__main__":
    main()

