# BGzip codec tests

The files below are sample TSV files with diverse schemas, compressed using the `bgzip` and indexed with `tabix`.

```{shell}
bgzip -c tests/gentropy/data_samples/bgzip_tests/A.tsv >tests/gentropy/data_samples/bgzip_tests/A.tsv.gz
bgzip -c tests/gentropy/data_samples/bgzip_tests/B.tsv >tests/gentropy/data_samples/bgzip_tests/B.tsv.gz

tabix -s 1 -b 2 -e 2 tests/gentropy/data_samples/bgzip_tests/A.tsv.gz
tabix -s 1 -b 2 -e 2 tests/gentropy/data_samples/bgzip_tests/B.tsv.gz
```
