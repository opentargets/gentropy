# Trans-pQTL Colocalisation Feature for L2G Prediction

## Overview

This feature adds trans-pQTL (protein quantitative trait loci) colocalisation scoring to the Locus-to-Gene (L2G) prediction pipeline. It identifies and scores genetic interactions between disease-associated loci and trans-acting protein QTL effects, enhancing gene prioritization in disease mapping.

## Feature Details

### What It Does

The `TransPQtlColocH4MaximumFeature` extracts the maximum H4 colocalisation probability between:

- **Left side**: Disease-associated credible sets (GWAS loci)
- **Right side**: Trans-pQTL study loci

For each gene in a locus, it computes the maximum colocalisation H4 score from all trans-pQTL colocalizations. Genes without trans-pQTL colocalizations receive a score of 0.

### Biological Rationale

Trans-pQTLs represent protein expression changes that affect multiple cell types and tissues (trans-acting effects). When disease associations colocalize with trans-pQTLs, it suggests:

- The implicated gene's protein expression is a likely mediator of disease risk
- Cross-tissue/cross-cell-type effects strengthen the causal inference
- Gene prioritization based on mechanistic evidence rather than correlation alone

## Implementation

### New Components

#### 1. **`TransPQtlColocH4MaximumFeature` Class**

- Located in: `src/gentropy/dataset/l2g_features/colocalisation.py`
- Inherits from: `L2GFeature`
- Feature name: `transPQtlColocH4Maximum`
- Dependency types: `[Colocalisation, StudyIndex, StudyLocus]`

#### 2. **`common_trans_pqtl_colocalisation_feature_logic()` Function**

- Implements the core logic for trans-pQTL feature computation
- Filters colocalisation dataset for trans-pQTL-specific results
- Returns features in long format (studyLocusId, geneId, featureName, featureValue)

#### 3. **Feature Factory Registration**

- Added to `src/gentropy/method/l2g/feature_factory.py`
- Enables feature discovery and automatic instantiation
- Maps feature name `"transPQtlColocH4Maximum"` to the feature class

### Algorithm

1. **Identify trans-pQTL study loci**: Filter study locus dataset for `isTransQtl == True`
2. **Filter colocalisation results**: Keep only colocalizations where the right study is a trans-pQTL
3. **Extract gene information**: Join with study index to map genes to trans-pQTL studies
4. **Compute maximum**: For each left study locus and gene pair, find the maximum H4 score
5. **Handle missing values**: Genes without trans-pQTL colocalizations get score 0.0

### Integration with L2G Pipeline

The feature integrates seamlessly with the existing L2G infrastructure:

```python
from gentropy.method.l2g.feature_factory import FeatureFactory, L2GFeatureInputLoader

# Feature is automatically available in the feature mapper
features_list = ["transPQtlColocH4Maximum", "pQtlColocH4Maximum", ...]

feature_factory = FeatureFactory(study_loci, features_list)
features = feature_factory.generate_features(
    L2GFeatureInputLoader(
        colocalisation=coloc_dataset,
        study_index=study_index,
        study_locus=study_locus,
    )
)
```

## Testing

### Test Coverage

All tests are located in `tests/gentropy/dataset/test_l2g_feature.py` under `TestTransPQtlColocH4Feature` class:

1. **`test_trans_pqtl_coloc_h4_maximum`**
   - Verifies feature computation with trans-pQTL data
   - Tests correct column structure
   - Tests feature name in long format DataFrame

2. **`test_trans_pqtl_coloc_with_no_trans_qtls`**
   - Verifies genes without trans-pQTL colocalizations receive score 0
   - Tests handling of cis-only study loci

3. **`test_trans_pqtl_feature_factory_inclusion`**
   - Tests feature factory registration
   - Verifies correct class mapping
   - Validates feature discoverability

4. **Parametrized factory test**
   - Included in existing `test_feature_factory_return_type` test
   - Verifies feature returns proper L2GFeature instance
   - Tests dependency injection

### Running Tests

```bash
# Run all trans-pQTL feature tests
pytest tests/gentropy/dataset/test_l2g_feature.py::TestTransPQtlColocH4Feature -v

# Run with coverage
pytest tests/gentropy/dataset/test_l2g_feature.py::TestTransPQtlColocH4Feature \
  --cov=src/gentropy/dataset/l2g_features/colocalisation \
  --cov-report=term-missing

# Run factory test for new feature
pytest tests/gentropy/dataset/test_l2g_feature.py::test_feature_factory_return_type \
  -k "TransPQtl" -v
```

## Example Usage

```python
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

# Create feature matrix with trans-pQTL feature
feature_matrix = L2GFeatureMatrix.from_features_list(
    study_loci_to_annotate=credible_set,
    features_list=["transPQtlColocH4Maximum", "pQtlColocH4Maximum", ...],
    features_input_loader=L2GFeatureInputLoader(
        colocalisation=coloc_dataset,
        study_index=study_index,
        study_locus=study_locus,
    ),
)

# Use in L2G model training
from gentropy.method.l2g.trainer import LocusToGeneTrainer

trainer = LocusToGeneTrainer(
    model=model,
    feature_matrix=feature_matrix,
    features_list=["transPQtlColocH4Maximum", ...],
)

trained_model = trainer.fit()
```

## Files Modified

1. **src/gentropy/dataset/l2g_features/colocalisation.py**
   - Added `common_trans_pqtl_colocalisation_feature_logic()` function
   - Added `TransPQtlColocH4MaximumFeature` class
   - Total: ~140 lines added

2. **src/gentropy/method/l2g/feature_factory.py**
   - Updated import to include `TransPQtlColocH4MaximumFeature`
   - Added feature to `feature_mapper` dictionary
   - Total: 2 lines added

3. **tests/gentropy/dataset/test_l2g_feature.py**
   - Added `TestTransPQtlColocH4Feature` test class
   - Added import for `TransPQtlColocH4MaximumFeature`
   - Updated parametrized test to include new feature
   - Total: ~200 lines added

## Design Decisions

### Why H4 vs CLPP?

The feature uses H4 (posterior probability of shared causal variant) rather than CLPP (colocalized likelihood ratio) because:

- H4 is more interpretable (direct probability)
- Consistent with existing pQTL features
- Better calibrated for L2G training

### Why Trans-pQTL Specific?

A dedicated feature for trans-pQTLs captures:

- Cross-tissue protein effects
- Broader biological impact
- Mechanistic evidence for disease causality
- Distinct from cis-pQTL effects (already covered by `pQtlColocH4Maximum`)

### No Neighbourhood Feature

As requested, only the single feature is implemented (not a neighbourhood variant). This design:

- Focuses on strongest mechanistic evidence
- Reduces feature dimensionality
- Avoids overfitting on weak trans-effects

## Performance Characteristics

- **Computation time**: Linear in colocalisation dataset size
- **Memory usage**: Minimal (only filters and aggregates)
- **Sparsity**: Likely high (most genes have no trans-pQTL colocalizations)
- **Distribution**: Skewed towards 0, with occasional high values

## Future Enhancements

Potential extensions for future work:

1. Add trans-pQTL features for other colocalisation metrics (CLPP)
2. Implement neighbourhood aggregation (if needed)
3. Add tissue-specific trans-pQTL features
4. Integration with drug target predictions
5. Validation studies using orthogonal methods

## References

- Original trans-pQTL analysis in notebook: `07_trans_pQTLs_CHEMBL_enrich.ipynb`
- L2G feature framework: `src/gentropy/dataset/l2g_features/`
- Colocalisation methods: `src/gentropy/method/colocalisation.py`
- L2G prediction: `src/gentropy/method/l2g/`

## Questions?

For questions about this feature:

1. Check the implementation in `src/gentropy/dataset/l2g_features/colocalisation.py`
2. Review tests in `tests/gentropy/dataset/test_l2g_feature.py`
3. See original analysis in Notebook: `07_trans_pQTLs_CHEMBL_enrich.ipynb`
