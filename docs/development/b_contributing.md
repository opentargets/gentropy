---
Title: Contributing
---

# Contributing guidelines

## Contributing checklist

When making changes, and especially when implementing a new module or feature, it's essential to ensure that all relevant sections of the code base are modified.

- [ ] Run `make check`. This will run the linter and formatter to ensure that the code is compliant with the project conventions.
- [ ] Develop unit tests for your code and run `make test`. This will run all unit tests in the repository, including the examples appended in the docstrings of some methods.
- [ ] Update the configuration if necessary.
- [ ] Update the documentation and check it with `make build-documentation`. This will start a local server to browse it (URL will be printed, usually `http://127.0.0.1:8000/`)

For more details on each of these steps, see the sections below.

## Documentation

- If during development you had a question which wasn't covered in the documentation, and someone explained it to you, add it to the documentation. The same applies if you encountered any instructions in the documentation which were obsolete or incorrect.
- Documentation autogeneration expressions start with `:::`. They will automatically generate sections of the documentation based on class and method docstrings. Be sure to update them for:
  - Dataset definitions in `docs/python_api/datasource/STEP` (example: `docs/python_api/datasource/finngen/study_index.md`)
  - Step definition in `docs/python_api/step/STEP.md` (example: `docs/python_api/step/finngen.md`)

## Classes

- Dataset class in `src/gentropy/datasource/STEP` (example: `src/gentropy/datasource/finngen/study_index.py` → `FinnGenStudyIndex`)
- Step main running class in `src/gentropy/STEP.py` (example: `src/gentropy/finngen.py`)

## Tests

- Test study fixture in `tests/conftest.py` (example: `mock_study_index_finngen` in that module)
- Test sample data in `tests/data_samples` (example: `tests/data_samples/finngen_studies_sample.json`)
- Test definition in `tests/` (example: `tests/dataset/test_study_index.py` → `test_study_index_finngen_creation`)
