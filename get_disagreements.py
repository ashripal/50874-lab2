import pandas as pd
import json
import matplotlib.pyplot as plt

def load_ndjson(filename):
    data = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        return pd.DataFrame() # Handle missing files
    return pd.DataFrame(data)

def plot_disagreement_patterns(merged):
    # Check if V1 and V3 are identical
    v1_v3_diff = (merged['out_v1'] != merged['out_v3']).sum()
    print(f"Number of disagreements between V1 and V3: {v1_v3_diff}")

    # Scatter plot V1 vs V2
    plt.figure(figsize=(10, 5))

    # Plot 1: V1 vs V2
    plt.subplot(1, 2, 1)
    plt.scatter(merged['out_v1'], merged['out_v2'], alpha=0.5, s=10)
    plt.plot([merged['out_v1'].min(), merged['out_v1'].max()], 
            [merged['out_v1'].min(), merged['out_v1'].max()], 
            'r--', label='y=x')
    plt.xlabel('Implementation 1 (Synchronous)')
    plt.ylabel('Implementation 2 (Async)')
    plt.title('Correlation: V1 vs V2')
    plt.legend()
    plt.grid(True)

    # Plot 2: V1 vs V3
    plt.subplot(1, 2, 2)
    plt.scatter(merged['out_v1'], merged['out_v3'], alpha=0.5, s=10, color='green')
    plt.plot([merged['out_v1'].min(), merged['out_v1'].max()], 
            [merged['out_v1'].min(), merged['out_v1'].max()], 
            'r--', label='y=x')
    plt.xlabel('Implementation 1 (Synchronous)')
    plt.ylabel('Implementation 3 (Threaded)')
    plt.title('Correlation: V1 vs V3')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig('disagreement_patterns.png')


def get_disagreements():
    # Load data
    df1 = load_ndjson('single_thread/out.ndjson')
    df2 = load_ndjson('async_event_loop/taxes_full.ndjson')
    df3 = load_ndjson('shared_state/out.ndjson')

    # Calculate total tax
    if not df1.empty:
        df1['out_v1'] = df1['federal_tax'] + df1['state_tax']
        df1 = df1[['taxpayer_id', 'out_v1']]
    else:
        df1 = pd.DataFrame(columns=['taxpayer_id', 'out_v1'])

    if not df2.empty:
        df2['out_v2'] = df2['federal_tax'] + df2['state_tax']
        df2 = df2[['taxpayer_id', 'out_v2']]
    else:
        df2 = pd.DataFrame(columns=['taxpayer_id', 'out_v2'])

    if not df3.empty:
        df3['out_v3'] = df3['federal_tax'] + df3['state_tax']
        df3 = df3[['taxpayer_id', 'out_v3']]
    else:
        df3 = pd.DataFrame(columns=['taxpayer_id', 'out_v3'])

    # Merge all three on taxpayer_id
    merged = df1.merge(df2, on='taxpayer_id', how='outer') \
                .merge(df3, on='taxpayer_id', how='outer')

    # Fill NaN with a distinct value to indicate missing data if any, 
    # or keep as NaN and handle in comparison. Let's fill with -1 for simplicity in checking inequality, 
    # assuming tax is always >= 0.
    merged_filled = merged.fillna(-1)

    # Find disagreements
    # A disagreement is where any of the outputs differ.
    disagreements = merged_filled[
        (merged_filled['out_v1'] != merged_filled['out_v2']) |
        (merged_filled['out_v2'] != merged_filled['out_v3']) |
        (merged_filled['out_v1'] != merged_filled['out_v3'])
    ].copy()

    # Rename columns for output
    csv_output = disagreements.rename(columns={'taxpayer_id': 'input_id'})

    # Save CSV
    csv_output.to_csv('disagreement_table.csv', index=False)

    # Plotting patterns
    if not csv_output.empty:
        plt.figure(figsize=(12, 6))
        
        # We want to show the pattern of disagreement.
        # Maybe Plot the difference V2-V1 and V3-V1.
        
        # Calculate differences for plotting (using original values, not filled)
        # Re-merge without fillna to get NaNs for missing data
        plot_data = merged.loc[disagreements.index].copy()
        
        # Check for NaNs
        v1 = plot_data['out_v1']
        v2 = plot_data['out_v2']
        v3 = plot_data['out_v3']
        
        # Differences relative to V1 (assuming V1 is "ground truth" or baseline)
        # If V1 is missing, we can't diff.
        
        print("Plotting disagreement patterns...")
        # Plot 1: V2 vs V1
        plt.subplot(1, 2, 1)
        diff_2_1 = v2 - v1
        # Filter out NaNs
        mask_2_1 = diff_2_1.notna()
        print("V2 vs V1")
        if mask_2_1.any():
            plt.scatter(plot_data.loc[mask_2_1, 'taxpayer_id'], diff_2_1[mask_2_1], label='V2 - V1', alpha=0.7)
            plt.axhline(0, color='red', linestyle='--')
            plt.xlabel('Taxpayer ID')
            plt.ylabel('Difference (V2 - V1)')
            plt.title('Disagreement Pattern: V2 vs V1')
            plt.xticks(rotation=90)
        else:
            plt.text(0.5, 0.5, 'No valid V2-V1 data', ha='center')

        # Plot 2: V3 vs V1
        print("V3 vs V1")
        plt.subplot(1, 2, 2)
        diff_3_1 = v3 - v1
        mask_3_1 = diff_3_1.notna()
        if mask_3_1.any():
            plt.scatter(plot_data.loc[mask_3_1, 'taxpayer_id'], diff_3_1[mask_3_1], label='V3 - V1', color='green', alpha=0.7)
            plt.axhline(0, color='red', linestyle='--')
            plt.xlabel('Taxpayer ID')
            plt.ylabel('Difference (V3 - V1)')
            plt.title('Disagreement Pattern: V3 vs V1')
            plt.xticks(rotation=90)
        else:
            plt.text(0.5, 0.5, 'No valid V3-V1 data', ha='center')

        plt.tight_layout()
        plt.savefig('disagreement_plot.png')
        
        print(f"Disagreements found: {len(csv_output)}")
        print(csv_output.head())
    else:
        print("No disagreements found.")
    
    return merged

if __name__ == "__main__":
    merged = get_disagreements()
    plot_disagreement_patterns(merged)