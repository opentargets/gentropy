# Essential Genes Analysis

This directory contains functionality for analyzing essential genes data with disease association groupings.

## Overview

The essential genes plotting functionality allows you to:

1. **Analyze 8 essential gene columns** with different scoring methods:
   - `essential_score_crispr`: CRISPR essentiality scores
   - `essential_score_rnai`: RNAi essentiality scores  
   - `essential_score_combined`: Combined essentiality scores
   - `essential_rank_crispr`: CRISPR ranking (1 = most essential)
   - `essential_rank_rnai`: RNAi ranking (1 = most essential)
   - `essential_probability`: Probability of being essential
   - `cell_viability_score`: Cell viability impact scores
   - `fitness_score`: Fitness impact scores

2. **Group genes by disease associations**:
   - `<10 diseases`: Genes associated with fewer than 10 diseases
   - `>=10 diseases`: Genes associated with 10 or more diseases

3. **Generate comprehensive visualizations**:
   - Individual subplots for each essential gene column
   - Appropriate plot types (histograms for scores, box plots for rankings)
   - Statistical significance testing (Mann-Whitney U test)
   - Correlation heatmap showing relationships between all variables

## Files

- `essential_genes_plotting.py`: Main plotting class implementation
- `essential_genes_analysis.ipynb`: Jupyter notebook with interactive analysis
- `demo_essential_genes_standalone.py`: Standalone demo script 
- `test_essential_genes_plotting.py`: Unit tests
- `essential_genes_analysis.png`: Example output plot (8 subplots)
- `essential_genes_correlation.png`: Example correlation heatmap

## Usage

### Quick Start (Standalone)

```bash
cd notebooks
python demo_essential_genes_standalone.py
```

This will generate sample data and create two output files:
- `essential_genes_analysis.png`: Main analysis plots
- `essential_genes_correlation.png`: Correlation heatmap

### Using the Class Directly

```python
from gentropy.method.essential_genes_plotting import EssentialGenesPlotter

# Create sample data (or use your own DataFrame with the required columns)
df = EssentialGenesPlotter.create_sample_data(n_genes=1000)

# Initialize the plotter
plotter = EssentialGenesPlotter(df)

# Create the main analysis plots
plotter.create_disease_group_plots(save_path='my_analysis.png')

# Create correlation heatmap
plotter.create_correlation_heatmap()
```

### Required Data Format

Your DataFrame must include:
- `uniqueDiseases`: Integer column with disease association counts
- Essential gene columns (any columns containing keywords like 'essential', 'score', 'rank', 'fitness', 'viability')

Example DataFrame structure:
```
geneId              uniqueDiseases  essential_score_crispr  essential_rank_crispr  ...
ENSG00000001       5               0.23                    456                    ...
ENSG00000002       15              0.87                    23                     ...
```

## Statistical Analysis

The plotting functionality includes:

- **Mann-Whitney U Test**: Compares distributions between disease groups
- **P-value indicators**: 
  - `***` p < 0.001 (highly significant)
  - `**` p < 0.01 (significant) 
  - `*` p < 0.05 (marginally significant)
  - `ns` p >= 0.05 (not significant)

## Output Interpretation

### Main Analysis Plot (8 subplots)

- **Score columns**: Shown as overlapping histograms comparing the two disease groups
- **Rank columns**: Shown as side-by-side box plots
- **Sample sizes**: Displayed in group labels (e.g., "<10 diseases (n=904)")
- **Statistical tests**: P-values shown in top-left corner of each subplot

### Correlation Heatmap

- Shows Pearson correlations between all essential gene columns and `uniqueDiseases`
- Color scale: Blue (negative correlation) to Red (positive correlation)
- Values range from -1.0 to +1.0

## Example Results

Based on the sample data (1000 genes):
- 904 genes (90.4%) associated with <10 diseases
- 96 genes (9.6%) associated with >=10 diseases  
- Mean unique diseases: 4.65
- Median unique diseases: 2.00

This distribution follows a realistic pattern where most genes are associated with few diseases, while a small number of genes are associated with many diseases (reflecting hub genes or broadly important pathways).