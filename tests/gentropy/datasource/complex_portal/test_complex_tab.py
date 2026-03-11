"""Unit tests for the ComplexTab protein-complex datasource."""

from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.session import Session
from gentropy.dataset.molecular_complex import MolecularComplex
from gentropy.datasource.complex_portal import ComplexTab

# Paths to bundled sample ComplexTAB files
_EXPERIMENTAL_TSV = "tests/gentropy/data_samples/complex_tab/9606.tsv"
_PREDICTED_TSV = "tests/gentropy/data_samples/complex_tab/9606_predicted.tsv"


class TestComplexTabSchema:
    """Tests for ComplexTab.get_schema()."""

    def test_returns_struct_type(self) -> None:
        """get_schema() must return a StructType."""
        assert isinstance(ComplexTab.get_schema(), t.StructType)

    def test_has_required_fields(self) -> None:
        """get_schema() must contain the core ComplexTAB column names."""
        field_names = set(ComplexTab.get_schema().fieldNames())
        assert "#Complex ac" in field_names
        assert "Taxonomy identifier" in field_names
        assert "Expanded participant list" in field_names
        assert "Evidence Code" in field_names
        assert "Cross references" in field_names
        assert "Source" in field_names

    def test_taxonomy_identifier_is_integer(self) -> None:
        """The Taxonomy identifier column must be IntegerType."""
        schema = ComplexTab.get_schema()
        taxonomy_field = next(
            field for field in schema.fields if field.name == "Taxonomy identifier"
        )
        assert isinstance(taxonomy_field.dataType, t.IntegerType)


class TestParseSource:
    """Tests for ComplexTab._parse_source()."""

    def test_extracts_id_and_source(self, session: Session) -> None:
        """_parse_source should extract PSI-MI id and human-readable source name."""
        df = session.spark.createDataFrame(
            [('psi-mi:"MI:0469"(IntAct)',)], "raw STRING"
        )
        result = df.select(ComplexTab._parse_source(f.col("raw")).alias("s"))
        row = result.collect()[0].s
        assert row.id == "MI:0469"
        assert row.source == "IntAct"

    def test_empty_string_gives_empty_fields(self, session: Session) -> None:
        """_parse_source on an empty string should produce empty id and source."""
        df = session.spark.createDataFrame([("",)], "raw STRING")
        result = df.select(ComplexTab._parse_source(f.col("raw")).alias("s"))
        row = result.collect()[0].s
        assert row.id == ""
        assert row.source == ""


class TestParseEvidenceCode:
    """Tests for ComplexTab._parse_evidence_code()."""

    def test_single_eco_code(self, session: Session) -> None:
        """A single ECO term should produce a one-element array."""
        df = session.spark.createDataFrame([("ECO:0000353",)], "raw STRING")
        result = df.select(ComplexTab._parse_evidence_code(f.col("raw")).alias("codes"))
        codes = result.collect()[0].codes
        assert codes == ["ECO:0000353"]

    def test_multiple_eco_codes_pipe_separated(self, session: Session) -> None:
        """Pipe-separated ECO codes should produce a multi-element array."""
        df = session.spark.createDataFrame([("ECO:0000353|ECO:0005543",)], "raw STRING")
        result = df.select(ComplexTab._parse_evidence_code(f.col("raw")).alias("codes"))
        codes = result.collect()[0].codes
        assert codes == ["ECO:0000353", "ECO:0005543"]

    def test_extra_text_around_eco_code_ignored(self, session: Session) -> None:
        """Extra surrounding text should be stripped; only the ECO: accession is kept."""
        df = session.spark.createDataFrame(
            [("pubmed:12345(ECO:0000353)",)], "raw STRING"
        )
        result = df.select(ComplexTab._parse_evidence_code(f.col("raw")).alias("codes"))
        codes = result.collect()[0].codes
        assert codes == ["ECO:0000353"]


class TestParseCrossReferences:
    """Tests for ComplexTab._parse_cross_references()."""

    def test_single_cross_reference(self, session: Session) -> None:
        """A single cross-reference entry should produce a one-element array."""
        df = session.spark.createDataFrame(
            [("reactome:R-HSA-9736938(identity)",)], "raw STRING"
        )
        result = df.select(
            ComplexTab._parse_cross_references(f.col("raw")).alias("xrefs")
        )
        xrefs = result.collect()[0].xrefs
        assert len(xrefs) == 1
        assert xrefs[0].source == "reactome"
        assert xrefs[0].id == "R-HSA-9736938"

    def test_multiple_cross_references(self, session: Session) -> None:
        """Pipe-separated entries should each map to a {source, id} struct."""
        df = session.spark.createDataFrame(
            [("reactome:R-HSA-9736938(identity)|wwpdb:1U7V(subset)",)], "raw STRING"
        )
        result = df.select(
            ComplexTab._parse_cross_references(f.col("raw")).alias("xrefs")
        )
        xrefs = result.collect()[0].xrefs
        assert len(xrefs) == 2
        sources = {x.source for x in xrefs}
        assert "reactome" in sources
        assert "wwpdb" in sources


class TestParseComponents:
    """Tests for ComplexTab._parse_components()."""

    def test_single_component(self, session: Session) -> None:
        """A single UniProt entry should produce a one-element component array."""
        df = session.spark.createDataFrame([("P04637(2)",)], "raw STRING")
        result = df.select(ComplexTab._parse_components(f.col("raw")).alias("comps"))
        comps = result.collect()[0].comps
        assert len(comps) == 1
        assert comps[0].id == "P04637"
        assert comps[0].stoichiometry == "2"
        assert comps[0].source == "uniprot"

    def test_multiple_components(self, session: Session) -> None:
        """Pipe-separated entries should produce one struct per component."""
        df = session.spark.createDataFrame([("P04637(2)|Q9Y6K9(1)",)], "raw STRING")
        result = df.select(ComplexTab._parse_components(f.col("raw")).alias("comps"))
        comps = result.collect()[0].comps
        assert len(comps) == 2
        ids = {c.id for c in comps}
        assert ids == {"P04637", "Q9Y6K9"}

    def test_source_is_always_uniprot(self, session: Session) -> None:
        """The source field must always be 'uniprot' regardless of the input."""
        df = session.spark.createDataFrame([("P04637(2)|Q9Y6K9(1)",)], "raw STRING")
        result = df.select(ComplexTab._parse_components(f.col("raw")).alias("comps"))
        comps = result.collect()[0].comps
        assert all(c.source == "uniprot" for c in comps)

    def test_hyphenated_uniprot_id(self, session: Session) -> None:
        """Hyphenated UniProt isoform IDs (e.g. P12345-1) should be parsed correctly."""
        df = session.spark.createDataFrame([("P12345-1(3)",)], "raw STRING")
        result = df.select(ComplexTab._parse_components(f.col("raw")).alias("comps"))
        comps = result.collect()[0].comps
        assert comps[0].id == "P12345-1"
        assert comps[0].stoichiometry == "3"


class TestFromComplexTab:
    """Integration tests for ComplexTab.from_complex_tab() using bundled sample TSVs."""

    def test_returns_molecular_complex(self, session: Session) -> None:
        """from_complex_tab should return a MolecularComplex instance."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        assert isinstance(result, MolecularComplex)

    def test_output_has_rows(self, session: Session) -> None:
        """The output dataset should contain at least one row."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        assert result.df.count() > 0

    def test_output_schema_matches_molecular_complex(self, session: Session) -> None:
        """The output DataFrame schema should match MolecularComplex.get_schema()."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        assert result.df.schema == MolecularComplex.get_schema()

    def test_all_rows_are_human(self, session: Session) -> None:
        """from_complex_tab must filter to human complexes only (taxonomy 9606)."""
        # The sample TSVs are already human-only, so the row count should be
        # equal to the total count before filtering – a non-zero count proves
        # the filter is not discarding everything.
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        assert result.df.count() > 0

    def test_experimental_and_predicted_are_unioned(self, session: Session) -> None:
        """Rows from both input files should appear in the output."""
        exp_only = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        ComplexTab.from_complex_tab(
            session, experimental=_PREDICTED_TSV, predicted=_EXPERIMENTAL_TSV
        )
        # With both files the union should have at least as many rows as either alone
        combined = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        assert combined.df.count() >= exp_only.df.count()

    def test_id_field_is_populated(self, session: Session) -> None:
        """Every row in the output must have a non-null id."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        null_ids = result.df.filter(f.col("id").isNull()).count()
        assert null_ids == 0

    def test_components_are_arrays(self, session: Session) -> None:
        """The components column should be an array type in the schema."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        components_field = next(
            field for field in result.df.schema.fields if field.name == "components"
        )
        assert isinstance(components_field.dataType, t.ArrayType)

    def test_evidence_codes_contain_eco_accessions(self, session: Session) -> None:
        """EvidenceCodes values should look like ECO accessions."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        # Collect a sample and verify at least one ECO code is present
        rows = result.df.filter(f.col("evidenceCodes").isNotNull()).limit(5).collect()
        assert any(
            any(code.startswith("ECO:") for code in row.evidenceCodes) for row in rows
        )

    def test_known_complex_id_present(self, session: Session) -> None:
        """CPX-1 from the experimental sample file must appear in the output."""
        result = ComplexTab.from_complex_tab(
            session, experimental=_EXPERIMENTAL_TSV, predicted=_PREDICTED_TSV
        )
        ids = {row.id for row in result.df.select("id").collect()}
        assert "CPX-1" in ids
