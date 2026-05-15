import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ── Verify input CSV ───────────────────────────────────────────────────────────
if not os.path.exists("benchmark/benchmark_results.csv"):
    print("benchmark_results.csv not found. Please run performance_client.py first.")
    sys.exit(1)

# ── Load & prepare data ────────────────────────────────────────────────────────
df = pd.read_csv("benchmark/benchmark_results.csv")

if not all(
    w in df[df['mode'] == "local"]['workers'].values
    for w in df['workers'].unique()
):
    print("Missing `local` entries for some worker counts. Check your CSV.")
    sys.exit(1)

df['total_latency'] = df['api_lat_mean'] + df['exec_lat_mean']

# ── 1) Combined Latency Flow Chart ────────────────────────────────────────────
def plot_combined_latency_flow():
    plt.figure(figsize=(15, 6))
    
    modes = ['local', 'pull', 'push']
    x = np.arange(len(modes))
    width = 0.35
    
    api_latencies = []
    exec_latencies = []
    
    for mode in modes:
        df_mode = df[df['mode'] == mode]
        api_latencies.append(df_mode['api_lat_mean'].mean())
        exec_latencies.append(df_mode['exec_lat_mean'].mean())
    
    plt.bar(x - width/2, api_latencies, width, label='API Request')
    plt.bar(x + width/2, exec_latencies, width, label='Task Execution')
    
    for i, v in enumerate(api_latencies):
        plt.text(i - width/2, v, f'{v:.3f}s', ha='center', va='bottom')
    for i, v in enumerate(exec_latencies):
        plt.text(i + width/2, v, f'{v:.3f}s', ha='center', va='bottom')
    
    plt.title('Component-wise Latency Comparison')
    plt.ylabel('Latency (seconds)')
    plt.xticks(x, [m.upper() for m in modes])
    plt.legend()
    plt.tight_layout()
    plt.savefig('benchmark/latency_flowchart.png')
    plt.close()
    print("Saved benchmark/latency_flowchart.png")

# ── 2) Weak-Scaling Latency Line Graph ────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

df_nop = df[df['fn'] == 'nop']
pivot_nop = (
    df_nop.pivot(index='workers', columns='mode', values='total_latency')
    .reindex(columns=["local", "pull", "push"])
)

for mode in pivot_nop.columns:
    ax1.plot(pivot_nop.index, pivot_nop[mode], marker='o', label=mode)
ax1.set_title("NOP Function")
ax1.set_xlabel("Number of Workers")
ax1.set_ylabel("Total Latency (seconds)")
ax1.set_xscale('log', base=2)
ax1.set_xticks(pivot_nop.index)
ax1.set_xticklabels(pivot_nop.index)
ax1.grid(True)
ax1.legend()

df_sleep = df[df['fn'] == 'sleep']
pivot_sleep = (
    df_sleep.pivot(index='workers', columns='mode', values='total_latency')
    .reindex(columns=["local", "pull", "push"])
)

for mode in pivot_sleep.columns:
    ax2.plot(pivot_sleep.index, pivot_sleep[mode], marker='o', label=mode)
ax2.set_title("Sleep Function")
ax2.set_xlabel("Number of Workers")
ax2.set_ylabel("Total Latency (seconds)")
ax2.set_xscale('log', base=2)
ax2.set_xticks(pivot_sleep.index)
ax2.set_xticklabels(pivot_sleep.index)
ax2.grid(True)
ax2.legend()

plt.suptitle("Weak-Scaling Latency", y=0.98)
plt.tight_layout()
plt.savefig("benchmark/weak_scaling_latency.png")
plt.close()
print("Saved benchmark/weak_scaling_latency.png")

# ── 3) Function-Type Comparison at Turning Point ────────────────────────────
local_curve = pivot_nop['local']
diff = np.diff(local_curve)
if any(diff > 0):
    turn_idx = np.where(diff > 0)[0][0]
    turn_point = local_curve.index[turn_idx]
else:
    turn_point = local_curve.index[-1]
print(f"Using turning point from local mode: {turn_point} workers")

local_baseline = df[(df['mode'] == 'local') & (df['workers'] == turn_point)].set_index('fn')['throughput']

df_tp = df[df['workers'] == turn_point].copy()
df_tp.loc[:, 'relative_speedup'] = df_tp.apply(
    lambda row: row['throughput'] / local_baseline[row['fn']] if row['mode'] != 'local' else 1.0,
    axis=1
)

functions = sorted(df_tp['fn'].unique())
modes = ['pull', 'push']
x = range(len(functions))
width = 0.35

plt.figure(figsize=(10, 6))
for i, mode in enumerate(modes):
    vals = (
        df_tp[df_tp['mode'] == mode]
        .set_index('fn')['relative_speedup']
        .reindex(functions)
        .values
    )
    bars = plt.bar([xi + i*width for xi in x], vals, width=width, label=mode)
    
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}x',
                ha='center', va='bottom')

plt.axhline(y=1.0, color='r', linestyle='--', alpha=0.5, label='Local Mode')
plt.text(len(functions)-1 + width, 1.0, 'Local Mode', 
         ha='right', va='bottom', color='r')

plt.xticks([xi + width/2 for xi in x], functions)
plt.title(f"Function Comparison at {turn_point} Workers\nvs Local Mode")
plt.xlabel("Function Type")
plt.ylabel("Relative Speedup vs Local Mode")
plt.legend()
plt.tight_layout()
plt.savefig("benchmark/function_comparison.png")
plt.close()
print("Saved benchmark/function_comparison.png")

plot_combined_latency_flow()

# ── 4) Weak-Scaling Throughput Line Graph ─────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

df_nop = df[df['fn'] == 'nop']
pivot_nop = (
    df_nop.pivot(index='workers', columns='mode', values='throughput')
    .reindex(columns=["local", "pull", "push"])
)

for mode in pivot_nop.columns:
    ax1.plot(pivot_nop.index, pivot_nop[mode], marker='o', label=mode)
ax1.set_title("NOP Function")
ax1.set_xlabel("Number of Workers")
ax1.set_ylabel("Throughput (tasks/second)")
ax1.set_xscale('log', base=2)
ax1.set_xticks(pivot_nop.index)
ax1.set_xticklabels(pivot_nop.index)
ax1.grid(True)
ax1.legend()

df_sleep = df[df['fn'] == 'sleep']
pivot_sleep = (
    df_sleep.pivot(index='workers', columns='mode', values='throughput')
    .reindex(columns=["local", "pull", "push"])
)

for mode in pivot_sleep.columns:
    ax2.plot(pivot_sleep.index, pivot_sleep[mode], marker='o', label=mode)
ax2.set_title("Sleep Function")
ax2.set_xlabel("Number of Workers")
ax2.set_ylabel("Throughput (tasks/second)")
ax2.set_xscale('log', base=2)
ax2.set_xticks(pivot_sleep.index)
ax2.set_xticklabels(pivot_sleep.index)
ax2.grid(True)
ax2.legend()

plt.suptitle("Weak-Scaling Throughput", y=0.98)
plt.tight_layout()
plt.savefig("benchmark/weak_scaling_throughput.png")
plt.close()
print("Saved benchmark/weak_scaling_throughput.png")
