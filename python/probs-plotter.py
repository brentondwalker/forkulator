#!/usr/bin/env python3

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



def probability_n_bins_empty(b, H, n):
    """Compute the probability that exactly n bins are empty."""
    #print(f"\t\tprobability_n_bins_empty(b={b}, H={H}, n={n})")
    k = b - n  # Bins that must have at least one ball
    if b < n or k < 0 or (H-1) < 0:
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
    min_backlogged_workers = math.ceil((H-b)/s)
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
    if ways_to_stack_k_workers == 0:
        return 0.0

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


s = 32
H = 10
b = 32

HH = s*s + 1
pnar = probability_n_workers_almost_ready_slimit(b, HH, 0, s)
print(f"pnar(b={b}, H={HH}, n={0}, s={s}) = {pnar}")
#sys.exit(0)

prbs = np.zeros((3,32))
total_prob = 0.0
for i in range(0,32):
    prbs[0,i] = i
    prbs[1,i] = probability_n_bins_empty(b,H,i)
    prbs[2,i] = probability_n_workers_almost_ready_slimit(b, H+s, i, s)
    total_prob += prbs[1,i]

print(prbs)
print(f"total_prob = {total_prob}")

plt.plot(prbs[0,:], prbs[1, :], label=f"b={b}  H={H}")
plt.plot(prbs[0,:], prbs[2, :], '--', label=f"b={b}  H={H}")
plt.grid()
plt.xlabel('number of empty bins')
plt.ylabel('probability')
plt.show()

plt.figure(figsize=(8, 6))
plt.grid()
plt.xlabel('number of balls (H)')
plt.ylabel(f"probability of n empty bins")
for n in range(0,s-1):
    prbh = np.zeros((3,s*s))
    total_prbh = 0.0
    for hh in range(1,s*s+1):
        prbh[0,hh-1] = hh
        prbh[1,hh-1] = probability_n_bins_empty(b,hh-s,n)
        #prbh[2,hh-1] = probability_n_workers_almost_ready_slimit(b, hh+s, n, s)
        prbh[2, hh - 1] = probability_n_workers_almost_ready_slimit(b, hh, n, s)
        total_prbh += prbh[1,hh-1]

    print(prbh)
    print(f"total_prbh = {total_prbh}")
    plt.plot(prbh[0,:], prbh[1, :]) #, label=f"s={s}  b={b}  n={n}")
    plt.plot(prbh[0, :], prbh[2, :], '--') #, label=f"s={s}  b={b}  n={n}")
plt.legend()
plt.show()

plt.figure(figsize=(8, 6))
plt.grid()
plt.xlabel('number of balls (H)')
plt.ylabel(f"total task departure prob.")
for b in range(1,s):
    prbd = np.zeros((3,s*s))
    total_prbd = 0.0
    for hh in range(1,s*s+1):
        prbd[0,hh-1] = hh
        prbd[1,hh-1] = total_departure_probability(b,hh,s)
        prbd[2,hh-1] = total_departure_probability_slimit(b,hh,s)
        total_prbd += prbd[1,hh-1]

    print(prbd)
    print(f"total_prbd = {total_prbd}")
    plt.plot(prbd[0,:], prbd[1, :], label=f"s={s}  b={b}  n={n}")
    plt.plot(prbd[0,:], prbd[2, :], '--', label=f"s={s}  b={b}  n={n}")
plt.legend()
plt.show()

plt.figure(figsize=(8, 6))
plt.grid()
plt.xlabel('number of balls (H)')
plt.ylabel(f"total task departure rate.")
for b in range(1,s):
    prbd = np.zeros((3,s*s))
    total_prbd = 0.0
    for hh in range(1,s*s+1):
        prbd[0,hh-1] = hh
        prbd[1,hh-1] = b*total_departure_probability(b,hh,s)
        prbd[2,hh-1] = b*total_departure_probability_slimit(b,hh,s)
        total_prbd += prbd[1,hh-1]

    print(prbd)
    print(f"total_prbd = {total_prbd}")
    plt.plot(prbd[0,:], prbd[1, :], label=f"s={s}  b={b}")
    plt.plot(prbd[0,:], prbd[2, :], '--', label=f"s={s}  b={b}")
plt.legend()
plt.show()
