#!/usr/bin/env python3
"""Demonstration script for essential genes plotting functionality."""

import matplotlib.pyplot as plt
import sys
import os

# Add the src directory to the path so we can import our module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from gentropy.method.essential_genes_plotting import EssentialGenesPlotter


def main():
    """Run the essential genes plotting demonstration."""
    print("Essential Genes Analysis Demo")
    print("=" * 40)
    
    # Create sample data
    print("1. Creating sample essential genes data...")
    df = EssentialGenesPlotter.create_sample_data(n_genes=1000)
    print(f"   Created dataset with {len(df)} genes")
    print(f"   Columns: {', '.join(df.columns[:5])}...")
    
    # Initialize plotter
    print("\n2. Initializing plotter...")
    plotter = EssentialGenesPlotter(df)
    print(f"   Identified {len(plotter.essential_columns)} essential gene columns")
    
    # Create main analysis plots
    print("\n3. Creating disease group analysis plots...")
    plotter.create_disease_group_plots(save_path='essential_genes_analysis.png')
    
    # Create correlation heatmap
    print("\n4. Creating correlation heatmap...")
    plotter.create_correlation_heatmap()
    
    print("\n5. Analysis complete!")
    print("   - Main analysis plot saved as: essential_genes_analysis.png")
    print("   - Correlation heatmap displayed")


if __name__ == "__main__":
    main()