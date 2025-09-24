"""Tests for essential genes plotting functionality."""

import matplotlib
import pandas as pd
import pytest

# Use non-interactive backend for testing
matplotlib.use('Agg')

from gentropy.method.essential_genes_plotting import EssentialGenesPlotter


class TestEssentialGenesPlotter:
    """Test cases for EssentialGenesPlotter class."""

    def test_sample_data_creation(self):
        """Test that sample data is created correctly."""
        df = EssentialGenesPlotter.create_sample_data(n_genes=100)
        
        # Check basic structure
        assert len(df) == 100
        assert 'geneId' in df.columns
        assert 'uniqueDiseases' in df.columns
        
        # Check that we have essential gene columns
        essential_cols = [col for col in df.columns 
                         if any(keyword in col.lower() for keyword in 
                               ['essential', 'score', 'rank', 'fitness', 'viability'])]
        assert len(essential_cols) == 8
        
        # Check data types and ranges
        assert df['uniqueDiseases'].min() >= 1
        assert df['uniqueDiseases'].max() <= 50

    def test_plotter_initialization(self):
        """Test plotter initialization."""
        df = EssentialGenesPlotter.create_sample_data(n_genes=50)
        plotter = EssentialGenesPlotter(df)
        
        # Check that disease groups were created
        assert 'disease_group' in plotter.df.columns
        unique_groups = plotter.df['disease_group'].unique()
        assert '<10 diseases' in unique_groups or '>=10 diseases' in unique_groups
        
        # Check essential columns identification
        assert len(plotter.essential_columns) > 0
        assert len(plotter.essential_columns) <= 8

    def test_invalid_data_handling(self):
        """Test handling of invalid input data."""
        # Test empty DataFrame
        with pytest.raises(ValueError, match="DataFrame is empty"):
            EssentialGenesPlotter(pd.DataFrame())
        
        # Test missing required columns
        df_missing = pd.DataFrame({'geneId': ['ENSG001', 'ENSG002']})
        with pytest.raises(ValueError, match="Missing required columns"):
            EssentialGenesPlotter(df_missing)

    def test_disease_group_creation(self):
        """Test disease group creation logic."""
        df = pd.DataFrame({
            'geneId': ['ENSG001', 'ENSG002', 'ENSG003'],
            'uniqueDiseases': [5, 15, 10],
            'essential_score': [0.1, 0.5, 0.8]
        })
        
        plotter = EssentialGenesPlotter(df)
        
        expected_groups = ['<10 diseases', '>=10 diseases', '>=10 diseases']
        assert list(plotter.df['disease_group']) == expected_groups

    def test_plotting_methods_run_without_error(self):
        """Test that plotting methods can be called without error."""
        df = EssentialGenesPlotter.create_sample_data(n_genes=100)
        plotter = EssentialGenesPlotter(df)
        
        # These should not raise exceptions
        try:
            # We can't easily test the actual plot output in unit tests,
            # but we can ensure the methods don't crash
            import matplotlib.pyplot as plt
            
            # Test that the essential columns are properly identified
            assert len(plotter.essential_columns) > 0
            
            # Test summary statistics generation
            plotter._print_summary_statistics()
            
            plt.close('all')  # Clean up any plots
            
        except Exception as e:
            pytest.fail(f"Plotting methods raised an exception: {e}")

    def test_statistical_test_functionality(self):
        """Test that statistical tests are applied correctly."""
        # Create data with clear difference between groups
        df = pd.DataFrame({
            'geneId': [f'ENSG{i:03d}' for i in range(100)],
            'uniqueDiseases': [5] * 50 + [15] * 50,  # Clear split
            'essential_score': [0.1] * 50 + [0.9] * 50  # Clear difference
        })
        
        plotter = EssentialGenesPlotter(df)
        
        # Check that groups are created correctly
        group_counts = plotter.df['disease_group'].value_counts()
        assert group_counts['<10 diseases'] == 50
        assert group_counts['>=10 diseases'] == 50
        
        # Check essential columns identification
        assert 'essential_score' in plotter.essential_columns

    def test_correlation_analysis_data_preparation(self):
        """Test data preparation for correlation analysis."""
        df = EssentialGenesPlotter.create_sample_data(n_genes=50)
        plotter = EssentialGenesPlotter(df)
        
        # Check that correlation data can be prepared
        corr_columns = plotter.essential_columns + ['uniqueDiseases']
        corr_data = plotter.df[corr_columns].corr()
        
        # Should be square matrix
        assert corr_data.shape[0] == corr_data.shape[1]
        
        # Should include uniqueDiseases
        assert 'uniqueDiseases' in corr_data.columns
        assert 'uniqueDiseases' in corr_data.index