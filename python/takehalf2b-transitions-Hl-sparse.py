#!/usr/bin/env python3

"""
Compute the markov model properties for the equilibrium approximation of a take-half parallel system.
** 2-barrier version **

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
from xmlrpc.client import boolean

import numpy as np
np.set_printoptions(edgeitems=30, linewidth=100000, formatter={'float': lambda x: "{0:0.3f}".format(x)})
import argparse
import scipy.sparse as sp
import scipy.sparse.linalg as spla
from scipy.sparse import csr_matrix, csc_matrix, lil_matrix
from scipy.sparse.linalg import spsolve
from scipy.integrate import quad
from math import exp, factorial
from scipy.special import gammainc, gammaincinv, gamma
from scipy.optimize import brentq
from scipy.stats import gamma
import matplotlib.pyplot as plt

### for older versions of python
# from typing import Dict

# some global variables for keeping track of the state mapping
lHj2state = {}
state2lHj = {}


def state_valid(s, k, l, H, j, warn = False):
    """Given all the state and model info, decide if a state is valid or not.
    NOTE: this version is only applicable for take-half (frac=0.5)
    :param s:
    :param k:
    :param l:
    :param H:
    :param j:
    :return:
    """
    valid = True
    # num jobs can't be more than the num busy workers
    if j > min(s-l, s):
        valid = False
    # num jobs in service can't be more than the number of stages in service
    if j > H:
        valid = False
    # stages per job is bounded by k
    if H > k*j:
        valid = False
    # the most complicated test of all
    # given the take-fraction, is it possible to occupy this many workers
    # with the many active jobs?
    #jmin = math.ceil(math.log2(s)) if l < 1 else math.ceil(math.log2(s / l))
    jmin = compute_jmin(s, l)
    if j < jmin:
        valid = False
    if warn and not valid:
        print(f"WARNING: invalid state check: (s={s}, k={k}, l={l}, H={H}, j={j})")
    # passed all the feasibility tests
    return valid


def compute_jmin(s, l, frac=0.5):
    """
    compute the minimum number of jobs necessary to get the idle number down to l
    :param s:
    :param l:
    :return:
    """
    queue_depth = 0
    if l < 0:
        queue_depth = -l
        l = 0
    workers_left = s
    j = 0
    while workers_left > l:
        workers_left -= math.ceil(frac * workers_left)
        j += 1
    #return j + queue_depth
    return j


def probability_n_bins_empty(b, H, n):
    """Compute the probability that exactly n bins are empty."""
    #print(f"\t\tprobability_n_bins_empty(b={b}, H={H}, n={n})")
    if b < n:
        return 0.0

    # Number of ways to distribute the balls such that exactly k=b-n bins are non-empty
    ways_to_fill_k_bins = math.comb(b, n) * math.comb(H-1, b-n-1)

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


def true_busy_beta(s, l, H, j):
    """Estimate the true number of busy workers
    :param l: the number of idle workers (not blocked)
    :param h: the total height of the stage stack
    :param j: the total number of active jobs
    :return:
    """
    #print(f"true_busy_beta({s}, {l}, {H}, {j})")
    if H == 0:
        return 0
    num_jobs = j
    num_blocked = s - max(l, 0)

    beta = 0.0
    prob_sum = 0.0
    for b in range(num_jobs, num_blocked+1):
        prob_trm = math.comb(num_blocked, b) * math.comb(H-1, b-1) / math.comb(H+num_blocked-1, num_blocked-1)
        prob_sum += prob_trm
        trm = b * prob_trm
        beta += trm
        #print(f"\tb={num_blocked}\t{trm}\t{beta}\t{prob_trm}\t{prob_sum}")
    #print(f"beta({s}, {l}, {H}, {j}) = {beta/prob_sum}")
    return beta/prob_sum


def enumerateStates(s, q, k):
    global lHj2state
    global state2lHj
    lHj2state = {}
    state2lHj = {}
    sid = 0
    for l in range(s, -q-1, -1):
        b = s-l
        true_blocked = b if b<s else s
        #jmin = math.ceil(math.log2(s)) if l < 1 else math.ceil(math.log2(s / l))
        jmin = compute_jmin(s, l)
        jmax = true_blocked
        for j in range(jmin, jmax + 1):
            #for H in range(j, true_blocked*k+1):
            for H in range(j, j * k + 1):
                state_valid(s, k, l, H, j, warn = True)
                lHj2state[(l,H,j)] = sid
                state2lHj[sid] = (l,H,j)
                #print(f"lHj2state[{(l,H,j)}] = {sid}")
                sid += 1
    print(f"total states: {len(lHj2state)}")

# from the (l,H) coordinate of the state, compute the state number in the CTMC matrix
def oldlH2state(l, H, s):
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
            print(f"ERROR: l={l}\tH={H} > b*k={b}*{k}={b*k} is too large!")
            raise RuntimeError(f"ERROR: l={l}\tH={H} > b*k={b}*{k}={b*k} is too large!")
            sys.exit(0)
        # start from the size of the submatrix one would have for a model with (b-1) states
        #state_base = int((b) + (k)*(b-1)*(b)/2)
        state_base = lh_matrix_size(b-1, k)
        # add on the height minus non-feasible states
        # and minus 1 because we index states from zero
        state_idx = state_base + H - (b-1) - 1
        #print(f"state_base(l={l}, H={H}, s={s}): state_base={state_base} \tstate_idx={state_idx}")
    return state_idx


def create_sparse_transition_matrix_by_rows(s:int, mu:float, lmbda:float, q=0, frac=0.5):
    print(f"create_sparse_transition_matrix_by_rows(s={s}, mu={mu}, lmbda={lmbda}, q={q}, frac={frac})")
    # first compute the number of states
    global lHj2state
    k = s
    numstates:int = len(lHj2state)
    print("numstates", numstates)

    # get a big sparse matrix...
    #Q = np.zeros((numstates, numstates))
    Q = lil_matrix((numstates, numstates))

    # for convenience, keep an index of the state
    state_idx = 0

    for l in range(s, -q-1, -1):
        b = s-l
        true_blocked = b if b<s else s
        #jmin = math.ceil(math.log2(s)) if l < 1 else math.ceil(math.log2(s / l))
        jmin = compute_jmin(s, l)
        jmax = true_blocked
        for j in range(jmin, jmax + 1):
            for h in range(j, j * k + 1):
                total_outflow = 0.0
                state1 = lHj2state[(l, h, j)]
                psi = total_departure_probability(j, h, s)
                psi_bar = 1.0 - psi
                true_b = true_busy_beta(s, l, h, j)

                # arrival transitions
                if b < (s + q):
                    # print("** arrival")
                    if l > 0:
                        # job starts immediately
                        l2 = math.floor(l * (1.0 - frac))
                        h2 = h + k
                        j2 = j + 1
                    else:
                        # job queues
                        l2 = l - 1
                        h2 = h
                        j2 = j
                    state2 = lHj2state[(l2, h2, j2)]
                    Q[state1, state2] = lmbda
                    total_outflow += lmbda

                # stage completion only transitions, (~T & ~J)
                if h > j:
                    # print("** stage")
                    if state_valid(s, k, l, h-1, j, warn=True):
                        state2 = lHj2state[(l, h - 1, j)]
                        Q[state1, state2] = true_b * mu * psi_bar
                        total_outflow += Q[state1, state2]

                # task departure, job departure transitions, (T & J)
                if h <= ((true_blocked - 1) * k + 1):
                    # print("** task")
                    #if j > 1 or b == j:
                    num_workers_freed = math.floor((s-max(l,0))/j)
                    max_jobs_starting = math.floor(math.log2(num_workers_freed)+1)
                    num_jobs_starting = min(-l, max_jobs_starting)
                    l2 = 0
                    h2 = 0
                    j2 = 0
                    if l >= 0:
                        # no queue
                        l2 = l + num_workers_freed
                        h2 = h - 1
                        j2 = j - 1
                    elif l < -num_jobs_starting:
                        # deep queue
                        l2 = l + num_jobs_starting
                        h2 = h - 1 + k*num_jobs_starting
                        j2 = j - 1 + num_jobs_starting
                    else:
                        # shallow queue
                        l2 = math.floor(num_workers_freed/math.pow(2,num_jobs_starting))
                        h2 = h - 1 + k*num_jobs_starting
                        j2 = j - 1 + num_jobs_starting
                    # check that the state is feasible w/r/t job sizes
                    #jmin = math.ceil(math.log2(s)) if l2 < 1 else math.ceil(math.log2(s / l2))
                    #if j2 >= jmin and j2 >= h2:
                    if state_valid(s, k, l2, h2, j2, warn=False):
                        state2 = lHj2state[(l2, h2, j2)]
                        Q[state1, state2] = true_b * mu * psi
                        total_outflow += Q[state1, state2]

                if total_outflow == 0.0:
                    print(f"WARNING: no transitions for state ({l}, {h}, {j})")

                Q[state1, state1] = -total_outflow
    return Q


def lstate_condense(s:int, queue:int, ctpi):
    print(f"lstate_condense(s={s}, queue={queue}, ctpi)")
    k = s
    pi = np.zeros((2,s+1+queue))
    for l in range(-queue,s+1):
        pi[0,l+queue] = l
    for b in range(0,s+queue+1):
        l = s - b
        pi[0, l + queue] = l
        true_blocked = b if b < s else s
        #jmin = math.ceil(math.log2(s)) if l < 1 else math.ceil(math.log2(s / l))
        jmin = compute_jmin(s, l)
        jmax = true_blocked
        for j in range(jmin, jmax + 1):
            for h in range(j, j*k+1):
                if state_valid(s, k, l, h, j, warn=True):
                    state_id = lHj2state[(l,h,j)]
                    pi[1,l+queue] += ctpi[state_id]
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
    print(f"sparse_steady_state_ctmc()")
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


def predict_sojourns(ctpi, s, q, lmbda, mu, take_frac=0.5):
    k = s

    # use the steady state distribution to predict the mean sojourn time
    def cdf_erlk(x, k, lambd):
        """CDF of Erlang-k distribution with rate parameter lambda."""
        # Regularized lower incomplete gamma function
        return gammainc(k, lambd * x)

    def erlang_cdf(x, k, lambd):
        #print(f"erlang_cdf({x}, {k}, {lambd}")
        return gamma.cdf(x, a=k, scale=1 / lambd)

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

    def max_erlang_cdf(x, r, k, lambd):
        """CDF of max of r Erlang(k, mu)"""
        #print(f"max_erlang_cdf({x}, {r}, {k}, {lambd})")
        #return cdf_erlk(x, k, lambd) ** r
        return erlang_cdf(x, k, lambd) ** r

    def mixture_cdf(x, params, weights):
        """Mixture CDF"""
        return sum(w * max_erlang_cdf(x, r, k, mu) for (r, k, mu), w in zip(params, weights))

    # Quantile of mixture of max order stats
    def mixture_max_quantile(p, params, weights, x_max=100):
        # Find root of F_Y(x) - p = 0
        func = lambda x: mixture_cdf(x, params, weights) - p
        # Expand search range if needed
        while func(x_max) < 0:
            x_max *= 2
        return brentq(func, 0, x_max)

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
        num_tasks = max(1,l*take_frac)
        num_stages = s/num_tasks
        ert = expected_max_erlang(num_tasks, num_stages, mu)
        expected_runtime_sum += colsum * ert
        #print(f"l={l}\tnum_tasks={num_tasks}\tnum_stages={num_stages}\tert={ert}")

    # try to estimate the sojourn and waiting quantiles based on the steady state
    # start with just the non-queueing states
    #for l in range(-q, s+1):
    params = []
    weights = []
    wait_params = []
    wait_weights = []  # yuk yuk.,...
    for l in range(-q, s + 1):
        b = s - l
        true_busy = b if b<s else s
        colsum = 0.0
        num_tasks = max(1,l*take_frac)
        num_stages = s/num_tasks
        for h in range(true_busy, true_busy*k + 1):
            ii = lH2state(l, h, s)
            colsum += ctpi[ii]
            if l <= 0:
                wait_params.append((1, 1-l, mu*s*s/h))
                wait_weights.append(ctpi[ii])
        params.append((num_tasks, num_stages, mu))
        weights.append(colsum)
        #print(f"params: l={l}, num_tasks={num_tasks}, num_stages={num_stages}, mu={mu}, weight={colsum}")
    #print(f"exec weight coverage: {sum(weights)}")
    #print(f"queue weight coverage: {sum(wait_weights)}")
    # we need to do this normalization for now when computing quantiles, because if the
    # actual quantile lies beyond the total weight of the pi vector that we are summing over,
    # the algorithm will explode trying to find it.
    weights /= sum(weights)
    quantile_runtime = mixture_max_quantile(0.999, params, weights)
    wait_weights /= sum(wait_weights)
    quantile_waittime = mixture_max_quantile(0.999, wait_params, wait_weights)

    #XXX we should include the non-parallel runtime of jobs in the queue
    # weighted by the density of those queue states
    # we can estimate the waiting times of those queued jobs separately
    #print("ctpi_sum=", ctpi_sum)
    #print("expected_runtime_sum=", expected_runtime_sum)
    #print("waittime_measure=", waittime_measure)
    #print("expected_waittime_sum=", expected_waittime_sum)
    #print("expected_sojourn =", expected_runtime_sum+expected_waittime_sum)
    print(f"{s}\t{lmbda}\t{mu}\t{expected_runtime_sum}\t{expected_waittime_sum}\t{expected_runtime_sum+expected_waittime_sum}\t{quantile_runtime}\t{quantile_waittime}")

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
    parser.add_argument("-p", '--pdist', action='store_true')
    args = parser.parse_args()

    s = args.num_workers
    mu = args.mu
    lmbda = args.lmbda
    queue = args.queue
    take_frac = args.takefrac
    pdist_plot = args.pdist

    #lH2state_debug(s, queue)
    enumerateStates(s, queue, s)
    global lHj2state
    print(f"total states in main(): {len(lHj2state)}")
    Q_byrows = create_sparse_transition_matrix_by_rows(s, mu, lmbda, queue, take_frac)
    #with np.printoptions(precision=2):
    #    print(Q_byrows)
    ctpi = sparse_steady_state_ctmc(Q_byrows)
    #print(ctpi)
    lstate_pi = lstate_condense(s, queue, ctpi)
    #print("\n\n")
    print(lstate_pi)
    #print("\n\n")
    predict_sojourns(ctpi, s, queue, lmbda, mu, take_frac)
    if not pdist_plot:
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
    take_frac_str = re.sub(r'[\.]', '', str(take_frac))
    lmbda_string = re.sub(r'[\.]', '', str(lmbda))
    filename = "../pdistr-tfbb%s-Ax%s-Sx10-t%d-w%d.dat" % (take_frac_str, lmbda_string, s, s)
    #filename = "../pdistr-tfbb-Ax%s-Sx10-t%d-w%d.dat" % (lmbda_string, s, s)
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
    filename = "../pdistr-tfbb%s-Ax%s-Sx10-t%d-w%d.dat" % (take_frac_str, lmbda_string, s, s)
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
    plt.savefig(f"../pdistr-tfbb{take_frac_str}-Ax{lmbda_string}-Sx10-t{s}-w{s}.png")
    plt.savefig(f"../pdistr-tfbb{take_frac_str}-Ax{lmbda_string}-Sx10-t{s}-w{s}.pdf")
    plt.show()

# ======================================
# ======================================
# ======================================

if __name__ == "__main__":
    main()

