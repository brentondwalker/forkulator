#!/usr/bin/env python3

import numpy as np
from scipy.integrate import quad
from scipy.special import gammainc, gamma

def cdf_erlk(x, k, lambd):
    """CDF of Erlang-k distribution with rate parameter lambda."""
    # Regularized lower incomplete gamma function
    return gammainc(k, lambd * x)

def pdf_erlk(x, k, lambd):
    """PDF of Erlang-k distribution with rate parameter lambda."""
    if x < 0:
        return 0
    return (lambd**k * x**(k-1) * np.exp(-lambd * x)) / gamma(k)

def pdf_max_erlk(x, n, k, lambd):
    """PDF of the maximum of n independent Erlang-k random variables."""
    if x < 0:
        return 0
    cdf_x = cdf_erlk(x, k, lambd)
    pdf_x = pdf_erlk(x, k, lambd)
    return n * (cdf_x**(n-1)) * pdf_x

def expected_value_max_erlk(n, k, lambd):
    """Expected value of the maximum of n Erlang-k random variables."""
    integral, _ = quad(lambda x: x * pdf_max_erlk(x, n, k, lambd), 0, np.inf)
    return integral

# Parameters
n = 5       # Number of Erlang-k random variables
k = 3       # Shape parameter
lambd = 1.0 # Rate parameter

# Compute the expectation
expected_maximum = expected_value_max_erlk(n, k, lambd)
print(f"Expected value of the maximum: {expected_maximum}")

