#!/usr/bin/python3
import argparse
import matplotlib.pyplot as plt
import numpy as np

# fig, (plt_lat) = plt.subplots(1, 1)
# fig.set_size_inches(2.1, 2.4)
plt.figure(figsize=(4.0, 2.5))
# fig.subplots_adjust(wspace=0.2)
# fig.subplots_adjust(left=0.0,right=1.0,bottom=0,top=0.75)

labels = ['Multi-Paxos', 'Multi-Paxos\n(Encrypted)', 'Multi-OPaxos\n(Shamir)','Multi-OPaxos\n(SSMS)']
labels = ['MPaxos', 'MPaxos\n(Encrypted)', 'MOPaxos\n(Shamir)','MOPaxos\n(SSMS)']
colors = ['#1f77b4', 'darkturquoise', '#ff7f0e', '#2ca02c']

colors.reverse()
labels.reverse()
x_ticks = np.arange(len(labels))
width = 0.5

# value size = 25bytes

# data
avg_latencies = [16.461795,   16.485527,     16.49483,   16.486479]
sdv_latencies = [0.160728,    0.159040,      0.083113,   0.083234]
avg_latencies.reverse()
sdv_latencies.reverse()

# plot1: latency
plt.bar(
    x_ticks, 
    avg_latencies, 
    width, 
    yerr=[
        np.zeros(len(sdv_latencies)), 
        sdv_latencies], 
    capsize=5, 
    edgecolor='black', 
    zorder=3, 
    color=colors, 
    alpha=0.9)
plt.xticks(x_ticks, labels, fontsize=8)
plt.ylabel("Average Latency (ms)")
plt.grid(zorder=0)
# plt.invert_xaxis()
# plt.title("WAN")
# arrowprops=dict(arrowstyle='|-|, widthA=0.3, widthB=0.3', facecolor='gray')

# set the spacing between subplots
# plt.subplots_adjust(wspace=0.3)

# print("hass")
plt.show()