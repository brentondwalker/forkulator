#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import expon


def simulate_erlang_k_spacings(n, k, rate, num_simulations):
    # To store spacings separately
    # here I'll include the first order stat as the zero-th spacing
    all_spacings = [[] for _ in range(n)]

    for _ in range(num_simulations):
        # Generate n Erlang-k random variables
        erlangs = np.random.gamma(shape=k, scale=1 / rate, size=n)

        # Sort the random variables to get order statistics
        order_stats = np.sort(erlangs)

        # Calculate spacings and store them separately
        spacings = np.diff(order_stats)
        all_spacings[0].append(order_stats[0])
        for idx, space in enumerate(spacings):
            all_spacings[idx+1].append(space)

    return all_spacings


# Parameters
n = 2  # Number of random variables
k = 64  # Shape parameter (Erlang-k)
rate = 1.0  # Rate parameter (lambda)
num_simulations = 100000  # Number of simulations
include_x0 = True

# Simulate spacings
all_spacings = simulate_erlang_k_spacings(n, k, rate, num_simulations)

# Collect means for each spacing
spacing_means = []

# Plotting the distributions of spacings and exponential fit
plt.figure(figsize=(12, 8))

spacing_offset = 1
for i, spacing in enumerate(all_spacings):
    # Calculate the mean of the current spacing
    mean_spacing = np.mean(spacing)
    spacing_means.append(mean_spacing)
    #print(f"Mean of Spacing {i + 1}: {mean_spacing:.4f}")
    print(f"Mean of Spacing {i}: {mean_spacing:.4f}")

    if include_x0 or i>0:
        # Plot histogram of the spacing
        plt.hist(spacing, bins=50, density=True, alpha=0.5, histtype='step', linewidth=2, label=f'Spacing {i}')

        # Plot exponential distribution with rate = 1/mean_spacing
        x = np.linspace(0, np.max(spacing), 1000)
        plt.plot(x, expon.pdf(x, scale=mean_spacing), linestyle='--', linewidth=1.5,
                 label=f'Exponential fit for Spacing {i}')

plt.yscale('log')  # Set y-axis to logarithmic scale
plt.title('Distributions of Successive Spacings of Order Statistics with Exponential Fits')
plt.xlabel('Spacing')
plt.ylabel('Density (log scale)')
plt.legend()
plt.grid(True)
plt.show()

# Plot of spacing mean vs. spacing number
plt.figure(figsize=(8, 6))
if include_x0:
    plt.plot(range(0, n), spacing_means, marker='o', linestyle='-')
else:
    plt.plot(range(1, n), spacing_means[1:], marker='o', linestyle='-')
plt.title('Spacing Mean vs. Spacing Number')
plt.xlabel('Spacing Number')
plt.ylabel('Mean of Spacing')
plt.grid(True)
plt.show()
