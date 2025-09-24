#!/usr/bin/env python3
"""Standalone demonstration script for essential genes plotting functionality."""

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from scipy import stats
from typing import Any

# Include the EssentialGenesPlotter class inline to avoid import issues
class EssentialGenesPlotter:
    """Class for creating essential genes analysis plots grouped by disease associations."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the plotter with essential genes data."""
        self.df = df.copy()
        self._validate_data()
        self._create_disease_groups()
        self._identify_essential_columns()

    def _validate_data(self) -> None:
        """Validate that the input data has required columns."""
        required_columns = ['uniqueDiseases']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        if self.df.empty:
            raise ValueError("DataFrame is empty")

    def _create_disease_groups(self) -> None:
        """Create disease groups (<10 and >=10)."""
        self.df['disease_group'] = self.df['uniqueDiseases'].apply(
            lambda x: '<10 diseases' if x < 10 else '>=10 diseases'
        )

    def _identify_essential_columns(self) -> None:
        """Identify columns that contain essential gene data."""
        potential_columns = [
            col for col in self.df.columns 
            if any(keyword in col.lower() for keyword in [
                'essential', 'score', 'rank', 'fitness', 'viability'
            ]) and col not in ['disease_group', 'uniqueDiseases']
        ]
        
        if not potential_columns:
            potential_columns = [
                col for col in self.df.select_dtypes(include=[np.number]).columns
                if col not in ['uniqueDiseases', 'disease_group']
            ]
        
        self.essential_columns = potential_columns[:8]
        
        if not self.essential_columns:
            raise ValueError("No essential gene columns found in the data")

    def create_disease_group_plots(
        self, 
        figsize: tuple[int, int] = (15, 20),
        save_path: str | None = None
    ) -> None:
        """Create subplots for each essential gene column, grouped by disease count."""
        n_cols = len(self.essential_columns)
        n_rows = (n_cols + 1) // 2
        
        fig, axes = plt.subplots(n_rows, 2, figsize=figsize)
        fig.suptitle(
            'Essential Genes Analysis by Disease Group', 
            fontsize=16, 
            y=0.98
        )
        
        if n_rows == 1:
            axes = axes.reshape(1, -1)
        
        axes_flat = axes.flatten()
        
        for i, col in enumerate(self.essential_columns):
            ax = axes_flat[i]
            self._create_subplot(ax, col)
        
        for i in range(len(self.essential_columns), len(axes_flat)):
            axes_flat[i].set_visible(False)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Plot saved to: {save_path}")
        
        self._print_summary_statistics()
        return fig

    def _create_subplot(self, ax: plt.Axes, col: str) -> None:
        """Create individual subplot for an essential gene column."""
        group_data = []
        group_labels = []
        
        for group in ['<10 diseases', '>=10 diseases']:
            group_values = self.df[self.df['disease_group'] == group][col].dropna()
            if len(group_values) > 0:
                group_data.append(group_values)
                group_labels.append(f'{group}\n(n={len(group_values)})')
        
        if len(group_data) < 2:
            ax.text(0.5, 0.5, f'Insufficient data for {col}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(col.replace('_', ' ').title(), fontsize=12, pad=10)
            return
        
        if 'rank' in col.lower():
            self._create_box_plot(ax, group_data, group_labels)
        else:
            self._create_histogram_plot(ax, group_data, group_labels)
        
        ax.set_title(col.replace('_', ' ').title(), fontsize=12, pad=10)
        ax.grid(True, alpha=0.3)
        
        self._add_statistical_test(ax, group_data)

    def _create_box_plot(
        self, 
        ax: plt.Axes, 
        group_data: list[pd.Series], 
        group_labels: list[str]
    ) -> None:
        """Create box plot for ranking-type data."""
        try:
            bp = ax.boxplot(group_data, tick_labels=group_labels, patch_artist=True)
        except TypeError:
            bp = ax.boxplot(group_data, labels=group_labels, patch_artist=True)
        
        colors = ['lightblue', 'lightcoral']
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
        ax.set_ylabel('Rank')
        ax.set_xlabel('Group')

    def _create_histogram_plot(
        self, 
        ax: plt.Axes, 
        group_data: list[pd.Series], 
        group_labels: list[str]
    ) -> None:
        """Create histogram plot for score-type data."""
        colors = ['blue', 'red']
        alpha = 0.6
        
        for j, (data, label) in enumerate(zip(group_data, group_labels)):
            ax.hist(data, bins=30, alpha=alpha, color=colors[j], 
                   label=label, density=True)
        
        ax.set_ylabel('Density')
        ax.set_xlabel('Value')
        ax.legend()

    def _add_statistical_test(self, ax: plt.Axes, group_data: list[pd.Series]) -> None:
        """Add statistical test results to the plot."""
        if len(group_data) == 2 and len(group_data[0]) > 0 and len(group_data[1]) > 0:
            try:
                stat, p_value = stats.mannwhitneyu(group_data[0], group_data[1])
                significance = "***" if p_value < 0.001 else "**" if p_value < 0.01 else "*" if p_value < 0.05 else "ns"
                ax.text(0.02, 0.98, f'p-value: {p_value:.3f} {significance}', 
                       transform=ax.transAxes, verticalalignment='top',
                       bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            except Exception:
                pass

    def _print_summary_statistics(self) -> None:
        """Print summary statistics for the analysis."""
        print("\n=== Summary Statistics ===")
        print(f"Total genes: {len(self.df)}")
        
        group_counts = self.df['disease_group'].value_counts()
        for group, count in group_counts.items():
            percentage = count / len(self.df) * 100
            print(f"Genes with {group}: {count} ({percentage:.1f}%)")
        
        print(f"Mean unique diseases: {self.df['uniqueDiseases'].mean():.2f}")
        print(f"Median unique diseases: {self.df['uniqueDiseases'].median():.2f}")
        print(f"Essential gene columns analyzed: {len(self.essential_columns)}")
        print(f"Columns: {', '.join(self.essential_columns)}")

    @staticmethod
    def create_sample_data(n_genes: int = 1000) -> pd.DataFrame:
        """Create sample essential genes data for testing and demonstration."""
        np.random.seed(42)
        
        gene_ids = [f"ENSG{i:08d}" for i in range(n_genes)]
        
        essential_gene_columns = {
            'essential_score_crispr': np.random.beta(2, 5, n_genes),
            'essential_score_rnai': np.random.beta(2, 4, n_genes),
            'essential_score_combined': np.random.beta(3, 3, n_genes),
            'essential_rank_crispr': np.random.randint(1, n_genes+1, n_genes),
            'essential_rank_rnai': np.random.randint(1, n_genes+1, n_genes),
            'essential_probability': np.random.beta(1, 3, n_genes),
            'cell_viability_score': np.random.beta(2, 3, n_genes),
            'fitness_score': np.random.beta(2, 4, n_genes)
        }
        
        unique_diseases = np.random.pareto(1.5, n_genes) * 2 + 1
        unique_diseases = np.round(unique_diseases).astype(int)
        unique_diseases = np.clip(unique_diseases, 1, 50)
        
        data = {
            'geneId': gene_ids,
            'uniqueDiseases': unique_diseases,
            **essential_gene_columns
        }
        
        return pd.DataFrame(data)


def main():
    """Run the essential genes plotting demonstration."""
    print("Essential Genes Analysis Demonstration")
    print("=" * 50)
    
    # Create sample data
    print("1. Creating sample essential genes data...")
    df = EssentialGenesPlotter.create_sample_data(n_genes=1000)
    print(f"   Created dataset with {len(df)} genes")
    print(f"   Columns: {list(df.columns)}")
    
    # Initialize plotter
    print("\n2. Initializing plotter...")
    plotter = EssentialGenesPlotter(df)
    print(f"   Identified {len(plotter.essential_columns)} essential gene columns")
    print(f"   Essential columns: {plotter.essential_columns}")
    
    # Create main analysis plots
    print("\n3. Creating disease group analysis plots...")
    fig = plotter.create_disease_group_plots(save_path='essential_genes_analysis.png')
    
    # Create correlation heatmap
    print("\n4. Creating correlation heatmap...")
    plt.figure(figsize=(12, 10))
    corr_data = df[plotter.essential_columns + ['uniqueDiseases']].corr()
    sns.heatmap(corr_data, annot=True, cmap='coolwarm', center=0, 
                square=True, fmt='.3f')
    plt.title('Correlation Matrix: Essential Gene Scores and Disease Associations')
    plt.tight_layout()
    plt.savefig('essential_genes_correlation.png', dpi=300, bbox_inches='tight')
    print("   Correlation heatmap saved to: essential_genes_correlation.png")
    
    print("\n5. Analysis complete!")
    print("   Files generated:")
    print("   - essential_genes_analysis.png: Main analysis plots")
    print("   - essential_genes_correlation.png: Correlation heatmap")


if __name__ == "__main__":
    main()