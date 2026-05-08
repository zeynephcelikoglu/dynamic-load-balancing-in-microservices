import pandas as pd
import matplotlib.pyplot as plt

print("[INFO] Generating charts...")

df = pd.read_csv('metrics.csv')

plt.figure(figsize=(14, 8))

plt.subplot(2, 1, 1)
plt.plot(df['Time'], df['Total_Messages'], color='crimson', linewidth=2, label='Total Pending Messages')
plt.title('Distributed System Load Analysis: Time vs. Queue Length', fontsize=14, fontweight='bold')
plt.ylabel('Message Queue Depth', fontsize=12)
plt.xticks(df['Time'][::10], rotation=45) 
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)

plt.subplot(2, 1, 2)
plt.step(df['Time'], df['Stock_Workers'], color='blue', linewidth=2, label='Stock Workers (London - 80ms)')
plt.step(df['Time'], df['Order_Workers'], color='green', linewidth=2, label='Order Workers (Rome - 50ms)')
plt.step(df['Time'], df['Notif_Workers'], color='purple', linewidth=2, label='Notif Workers (Tokyo - 120ms)')
plt.step(df['Time'], df['DB_Workers'], color='orange', linewidth=2, label='DB Worker (Istanbul - 0ms)')
plt.title('Autonomous Load Balancing: Active Container Count vs. Time', fontsize=14, fontweight='bold')
plt.ylabel('Active Container Count', fontsize=12)
plt.xlabel('Time (HH:MM:SS)', fontsize=12)
plt.yticks(range(0, 12)) 
plt.xticks(df['Time'][::10], rotation=45) 
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)

plt.tight_layout()
output_filename = 'Distributed_Scaling_Analysis_Report.png'
plt.savefig(output_filename, dpi=300) 
print(f"[SUCCESS] Charts saved to '{output_filename}'.")