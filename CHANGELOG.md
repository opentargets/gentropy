# CHANGELOG



## v1.0.0 (2023-08-15)


## v1.0.0-rc.1 (2023-08-15)

### Breaking

* feat: build and submit process redesigned

- New build based on `poetry build`
- Project renamed - beware consequences with `poetry update` etc.
- Using wheels accross the board
- Package is installed in dataproc using intialisation actions
- Dependencies are installed in the remote cluster for remote work
- Minor bugs on config paths in variant variant_annotation
- BUG: variant_annotation produces an error not yet diagnosed

BREAKING CHANGE: src/etl now contains the modules instead of src/ ([`16b56fa`](https://github.com/opentargets/genetics_etl_python/commit/16b56fa2c21bb73c603a59e1d5403ecbe860affa))

* feat: modular hydra configs and other enhancements

- Dynamic configs using hydra (see README)
- ETLSession initialises spark session
- Moving to Spark logging instead of python logging
- Missing dependencies added (e.g. mypy, flake8 extensions)
- Missing dependencies added (e.g. pandas)
- Unnecesary logic removed from intervals
- Mypy added to precommit
- VScode workspace settings include lintrs

BREAKING CHANGE: ETLSession class to initialise Spark/logging ([`bcfb28e`](https://github.com/opentargets/genetics_etl_python/commit/bcfb28e5952aa78afeb4d18cb1e99b9d0c87078a))

### Build

* build: downgrade project version to 0.1.4 ([`93406d9`](https://github.com/opentargets/genetics_etl_python/commit/93406d9343466dc0b773ec5939bf3e6e823d263c))

* build: downgrade from 3.10.9 to 3.10.8 for compatibility w/ dataproc ([`d2f96bb`](https://github.com/opentargets/genetics_etl_python/commit/d2f96bb7d44a396ae459d57c6b70853667515cc0))

* build: update gitignore ([`3ed4482`](https://github.com/opentargets/genetics_etl_python/commit/3ed4482b1c5a852646f50de541f36d5e2ea8d3df))

* build: update mypy to 1.2.0 ([`0d5c6e9`](https://github.com/opentargets/genetics_etl_python/commit/0d5c6e9e9f6592f4b005e5feef43c550c3535f2d))

* build: update python to 3.8.15 ([`6c4713b`](https://github.com/opentargets/genetics_etl_python/commit/6c4713b46a4ff292e9e96efd95f2c78601e49e64))

* build: update python to 3.8.15 ([`c794692`](https://github.com/opentargets/genetics_etl_python/commit/c7946925076d57cf98464a1ff77b49fe95cd3692))

* build: new target schema with the new `canonicalTranscript` ([`80d79cb`](https://github.com/opentargets/genetics_etl_python/commit/80d79cbc325c5b2e6716b7c030b2fbfc70929f4c))

* build: pipeline run with most up to date gene index ([`f98a987`](https://github.com/opentargets/genetics_etl_python/commit/f98a9874fb409d223b48aa2b06c8989c1003e875))

* build: add pandas as a test dependency ([`9a75890`](https://github.com/opentargets/genetics_etl_python/commit/9a7589031662c50c2910b78b9023d5e69dd256e1))

* build: add pandas as a test dependency ([`5ff7ccd`](https://github.com/opentargets/genetics_etl_python/commit/5ff7ccd3608069dd07bbe0fcf35f60f781d41157))

### Chore

* chore: remove unnecessary file ([`8394c96`](https://github.com/opentargets/genetics_etl_python/commit/8394c96ca628c8d4d6553f22e950d382276c23da))

* chore: suggestions on structure (#99)

Looks good to me. ([`465306d`](https://github.com/opentargets/genetics_etl_python/commit/465306d79490057d41a3a102dec7c92f4c2b93d5))

* chore: logic cleaned up based on PR review ([`660c9ec`](https://github.com/opentargets/genetics_etl_python/commit/660c9ecff3642338458108a4f4b874c8649cb4cd))

* chore: typing error ([`d6ce0d7`](https://github.com/opentargets/genetics_etl_python/commit/d6ce0d785f1593c17fdf490ec5bf87736a18ccae))

* chore: merge do-hydra branch ([`dbc2072`](https://github.com/opentargets/genetics_etl_python/commit/dbc20726aa422ebcaf51574cc0f980b95114f112))

* chore: pull main branch ([`b1d843b`](https://github.com/opentargets/genetics_etl_python/commit/b1d843b6a7e7c6d7f4a4feb5b48f51bfd333ad3b))

* chore: wip gwas splitter ([`c5eb279`](https://github.com/opentargets/genetics_etl_python/commit/c5eb2792e34db5eaf092bc7c627127bf5b26323f))

* chore: gitignore schemas markdown ([`67accc6`](https://github.com/opentargets/genetics_etl_python/commit/67accc6c6e6babc5b59b26915114837478e7a5ae))

* chore: cleanup deprecated ([`439e847`](https://github.com/opentargets/genetics_etl_python/commit/439e8476ba5fd7415689068fb185bb1d5039427f))

* chore: update v2g schema to include distance features ([`8b7dc2c`](https://github.com/opentargets/genetics_etl_python/commit/8b7dc2c5c1c4985c2b3eeb5279919ed1450c4e62))

* chore: delete commented lines ([`3444817`](https://github.com/opentargets/genetics_etl_python/commit/34448173ca1677195530872906655a2ae12ef655))

* chore: separate script for dependency installation (#47) ([`2553be9`](https://github.com/opentargets/genetics_etl_python/commit/2553be9d49d89108a867baa5764059a977965c50))

* chore: updated config and makefile ([`8d906f1`](https://github.com/opentargets/genetics_etl_python/commit/8d906f15e141d264135718bb31dc43ada82ac01e))

* chore: adding function to calculate z-score ([`d82f270`](https://github.com/opentargets/genetics_etl_python/commit/d82f270e6d7a8531ccdc3d7d372af23cf51667ec))

* chore: exploring p-value conversion ([`890038f`](https://github.com/opentargets/genetics_etl_python/commit/890038f2974c7e0ff0ac9835eeeb3f7a47b7cd23))

### Documentation

* docs: page for window based clumping ([`bd7a23e`](https://github.com/opentargets/genetics_etl_python/commit/bd7a23e9617669d5f04e1ee81dfd1bee3a68203c))

* docs: remove the now unneeded file from utils ([`6b1f319`](https://github.com/opentargets/genetics_etl_python/commit/6b1f31917a5c437107d1e1d4c087b82147fb287d))

* docs: reorganise troubleshooting section ([`a9819e7`](https://github.com/opentargets/genetics_etl_python/commit/a9819e72c365bcf2b6070cca01b79fdc50857aa8))

* docs: update index with a separate troubleshooting section ([`79b69d9`](https://github.com/opentargets/genetics_etl_python/commit/79b69d9a28d5226a6aa4061212deccc503a2ab76))

* docs: move troubleshooting information from the contributing guide ([`ef8a7f7`](https://github.com/opentargets/genetics_etl_python/commit/ef8a7f766d91cd6ce71bb74228764ceadaa9aee6))

* docs: update documentation on building and testing ([`6f4e0c2`](https://github.com/opentargets/genetics_etl_python/commit/6f4e0c2473d2638fa699f88d26461b512c43de5a))

* docs: improve `_annotate_discovery_sample_sizes` docs ([`eff02da`](https://github.com/opentargets/genetics_etl_python/commit/eff02da6269373693398182c6cbd09a021cc6d2e))

* docs: updated java version text ([`93de7c3`](https://github.com/opentargets/genetics_etl_python/commit/93de7c3ff110f66c507c1ce8f4c254801145297f))

* docs: finngen study index dataset duplicated in docs ([`6f2aa01`](https://github.com/opentargets/genetics_etl_python/commit/6f2aa01c50a2ad61573dc3fb90138757f34fa8e1))

* docs: finngen study index dataset duplicated in docs ([`eeb85b0`](https://github.com/opentargets/genetics_etl_python/commit/eeb85b08e0a52ef4aec0963c60f3cb990b5446ea))

* docs: Update ukbiobank.py docstring

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`a6c1ccb`](https://github.com/opentargets/genetics_etl_python/commit/a6c1ccb44f31185b73e78aee0a90d3304c9e5493))

* docs: compile the new contributing section ([`42776f9`](https://github.com/opentargets/genetics_etl_python/commit/42776f9ee7fa82474ff2e479c3aa36ccf1da77e6))

* docs: add contributing information to index ([`70edea7`](https://github.com/opentargets/genetics_etl_python/commit/70edea74810bc9abbceab3daa8afae7dd8fcc9dc))

* docs: move technical information out of README ([`a11f042`](https://github.com/opentargets/genetics_etl_python/commit/a11f042fa7c67860ae170f95811693ce6b2e9638))

* docs: add need to install java version 8 ([`42325d4`](https://github.com/opentargets/genetics_etl_python/commit/42325d448b8d334c51efaf6a3b86734bde95e05d))

* docs: fix link ([`c3bee56`](https://github.com/opentargets/genetics_etl_python/commit/c3bee5691f854c8cd4a664ef69264e2fd48b4983))

* docs: fix fixes ([`0148299`](https://github.com/opentargets/genetics_etl_python/commit/0148299e90421f8dda95318e7fa5d12cfbb1e94b))

* docs: update index.md ([`3ffdd4f`](https://github.com/opentargets/genetics_etl_python/commit/3ffdd4fb2d12838e398f343044adf486c3d1a4ea))

* docs: reference renamed to components based on buniellos request ([`a6458ef`](https://github.com/opentargets/genetics_etl_python/commit/a6458ef97ab1fd250a245288e1e0092cdbd8c2a7))

* docs: correct version naming example ([`18c4e6d`](https://github.com/opentargets/genetics_etl_python/commit/18c4e6dcf9c849fb3d8d529c5f6ae35f1b56e00f))

* docs: amend documentation to make clear how versioning works ([`8b564af`](https://github.com/opentargets/genetics_etl_python/commit/8b564af0c373bc0238d64f405a1f80b4aef88edc))

* docs: missing docs for finger step added ([`e8f1f67`](https://github.com/opentargets/genetics_etl_python/commit/e8f1f6736f095a9cb6e431aba2a3eeb8fe2dad59))

* docs: rewrite README with running instructions ([`c41fd38`](https://github.com/opentargets/genetics_etl_python/commit/c41fd38bd412c05f4048fe786a43b9f03b36bcc6))

* docs: add documentation on StudyIndexFinnGen ([`9c65be4`](https://github.com/opentargets/genetics_etl_python/commit/9c65be4f137d900733207a14856e0b9498da7a5f))

* docs: add troubleshooting instructions for Pyenv/Poetry ([`2848643`](https://github.com/opentargets/genetics_etl_python/commit/2848643f3a0ca0015fd2d507b10b556e526c378c))

* docs: simplify instructions for development environment ([`597fc7a`](https://github.com/opentargets/genetics_etl_python/commit/597fc7ad98c46c691ad070321088f11444e981a4))

* docs: docs badge added

Docs badge added ([`f93bc57`](https://github.com/opentargets/genetics_etl_python/commit/f93bc5752c322e3824c2694f3ba3623b89716c8d))

* docs: fixing actions ([`a9a22ab`](https://github.com/opentargets/genetics_etl_python/commit/a9a22ab6741267871fa8e5c52af2e0d507cce3cb))

* docs: fixing actions ([`25d35a0`](https://github.com/opentargets/genetics_etl_python/commit/25d35a0ce7aebd8137a6bd2a8bc3c902b99a9327))

* docs: fixing actions ([`70f1295`](https://github.com/opentargets/genetics_etl_python/commit/70f12951f451335ef3e1c7c020bc6b3ee99bce26))

* docs: fix to prevent docs crashing ([`74b3833`](https://github.com/opentargets/genetics_etl_python/commit/74b3833bf4a40521e3973ba3557e491aefaaf9b3))

* docs: fix doc ([`61d53de`](https://github.com/opentargets/genetics_etl_python/commit/61d53deab49fc1196fba36f1fbccf70e6d77e929))

* docs: explanation improved ([`7ff8fc9`](https://github.com/opentargets/genetics_etl_python/commit/7ff8fc962ffeca267dd39a74a09fd6e6862eba83))

* docs: missing docstring ([`4343ea9`](https://github.com/opentargets/genetics_etl_python/commit/4343ea92058dc204ddac9dab36a8ea5a06337563))

* docs: gwas catalog step ([`e3fc4d5`](https://github.com/opentargets/genetics_etl_python/commit/e3fc4d54753b04d7c25939f7d89033813bd0e41f))

* docs: missing ld clumped ([`caa3453`](https://github.com/opentargets/genetics_etl_python/commit/caa3453a7505e6f3a09bab3e649f9a17143dc62c))

* docs: typo ([`52c9092`](https://github.com/opentargets/genetics_etl_python/commit/52c9092e1d61adc8e45662206021527325055ae5))

* docs: including titles in pages ([`37282e1`](https://github.com/opentargets/genetics_etl_python/commit/37282e18ea5a29f69fce8e33fb58cc5b939e3f32))

* docs: improved description ([`01a4723`](https://github.com/opentargets/genetics_etl_python/commit/01a47233ac39713f78471ea52877d26f418b2fac))

* docs: extra help to understand current configs ([`705adf7`](https://github.com/opentargets/genetics_etl_python/commit/705adf7c0a6b142351ecc426d7e2f9f1464b6b7f))

* docs: address david comment on verbose docs ([`a7210a7`](https://github.com/opentargets/genetics_etl_python/commit/a7210a79cea7d864083eba576b05aca581d97603))

* docs: correct reference to coloc modules ([`df10181`](https://github.com/opentargets/genetics_etl_python/commit/df10181b6441849435cf54ceea2d6aa9d7a967f9))

* docs: update summary of logic ([`ebbf5a4`](https://github.com/opentargets/genetics_etl_python/commit/ebbf5a44fec45ac3f9287a552eca0d62ff81248c))

* docs: added docs ([`a271ab5`](https://github.com/opentargets/genetics_etl_python/commit/a271ab5e36b6f75e345c1c6299dda4bac959fd2d))

* docs: added v2g docs page ([`6b8c983`](https://github.com/opentargets/genetics_etl_python/commit/6b8c9835f8c4546fcd9449f97ad61a1e7cd24ee7))

* docs: scafolding required for variant docs ([`a00fa0a`](https://github.com/opentargets/genetics_etl_python/commit/a00fa0a864ef0cde563357a69c306155f8177a17))

* docs: generate_variant_annotation documentation ([`84c7a3a`](https://github.com/opentargets/genetics_etl_python/commit/84c7a3a3815ff2f5888c94cc234d76373ff13c91))

* docs: removing old docsc (#36)

I don&#39;t think there is anything that hasn&#39;t been migrated yet ([`a71c367`](https://github.com/opentargets/genetics_etl_python/commit/a71c3673930393e94b848827f0d0194adcc21931))

* docs: rounding numbers for safety ([`09d09f2`](https://github.com/opentargets/genetics_etl_python/commit/09d09f2a886b4b2d3baaa2b765a1021940282580))

* docs: first pyspark function example ([`4845f6e`](https://github.com/opentargets/genetics_etl_python/commit/4845f6e7309139ad27c6ba79ba97b8f19f98866c))

* docs: non-spark function doctest ([`e750a8b`](https://github.com/opentargets/genetics_etl_python/commit/e750a8b7fcb1a61f3cf2f9f8a13824c728c870da))

* docs: doctest functionality added ([`55b5d3e`](https://github.com/opentargets/genetics_etl_python/commit/55b5d3e3da97d065f3f9401bd635aaa7a70b6399))

* docs: openblas and lapack libraries required for scipy ([`5fe9d10`](https://github.com/opentargets/genetics_etl_python/commit/5fe9d10bdd47271f50f92ecddd52167f8d26cb61))

* docs: improvements on index page and roadmap ([`a93abf5`](https://github.com/opentargets/genetics_etl_python/commit/a93abf50ce70066f676ca18c0ee4056853209142))

* docs: license added ([`c4fc195`](https://github.com/opentargets/genetics_etl_python/commit/c4fc19571bed95450060d0ced172e26cc83fff09))

* docs: better docs for mkdocs ([`d6eafae`](https://github.com/opentargets/genetics_etl_python/commit/d6eafae86f83288fd670466db927f10a46dc0066))

* docs: function docstring ([`3c1da91`](https://github.com/opentargets/genetics_etl_python/commit/3c1da915f13fd7f7977183148b32d42c0bbfefbe))

* docs: schema functions description ([`f49d913`](https://github.com/opentargets/genetics_etl_python/commit/f49d91387490823549928d072ba825d2c6c2d141))

* docs: adding badges to README ([`215e19f`](https://github.com/opentargets/genetics_etl_python/commit/215e19f3960682709c9ac180fcedc8877cea425c))

* docs: adding badges to README ([`42bb711`](https://github.com/opentargets/genetics_etl_python/commit/42bb7118122d465a64b8d9c0aacbe0662a2474fb))

* docs: adding documenation for gwas ingestion ([`6ce1d53`](https://github.com/opentargets/genetics_etl_python/commit/6ce1d53a02c29640dbb910c5bb63c520b0d93f5b))

* docs: not necessary ([`d59a96e`](https://github.com/opentargets/genetics_etl_python/commit/d59a96e269ada404d462d65daeee21ae2aa1e9b6))

* docs: adding documentation for Intervals and Variant annotation ([`645a824`](https://github.com/opentargets/genetics_etl_python/commit/645a824f37f2b6cfcda15bfb183d0688ae15bf9e))

### Feature

* feat: change r2 threshold to retrieve all variants above .2 and apply filter by .5 on r2overall ([`1789fe4`](https://github.com/opentargets/genetics_etl_python/commit/1789fe4c2e75c5896a26f359345ef80528a8667e))

* feat(_variant_coordinates_in_ldindex): replace groupby by select ([`d3a2c15`](https://github.com/opentargets/genetics_etl_python/commit/d3a2c158405517e04117c9950497c6e24f95d707))

* feat: add `get_study_locus_id` ([`6c9f98f`](https://github.com/opentargets/genetics_etl_python/commit/6c9f98f3c5d12f1b10b6c74063447043414402b5))

* feat: fix ldclumping, tests, and docs ([`68c204e`](https://github.com/opentargets/genetics_etl_python/commit/68c204e80b83891854d9dcac6ef2e4cd5dc77fc6))

* feat: function to clump a summary statistics object ([`19ece6d`](https://github.com/opentargets/genetics_etl_python/commit/19ece6d7df3490e0602c29b7fdc0f21af04df258))

* feat: adding main logic ([`338ed5c`](https://github.com/opentargets/genetics_etl_python/commit/338ed5cc162f8a9a117a58d6e4ade71589ce8060))

* feat(ld): remove unnecessary grouping - variants are no longer duplicated ([`75c7856`](https://github.com/opentargets/genetics_etl_python/commit/75c7856b03bbd65c225d2aaeec88d467cdc54d70))

* feat(ldindex): remove ambiguous variants ([`585f9e2`](https://github.com/opentargets/genetics_etl_python/commit/585f9e2a2517f9df5eb99b5116f6724804228cd3))

* feat: update ingestion fields ([`e397e8e`](https://github.com/opentargets/genetics_etl_python/commit/e397e8eefeb5bf7f0bf6b81382bbb8d731eb7cb1))

* feat: update input/output paths ([`dfe5341`](https://github.com/opentargets/genetics_etl_python/commit/dfe534151e0e714e720f628d3fea19a140cd941f))

* feat: add create-dev-cluster rule to makefile (#94) ([`88ea59f`](https://github.com/opentargets/genetics_etl_python/commit/88ea59f2e60159cbc7afc603396ccc41888b2d6d))

* feat: ukbiobank sample test data ([`a0017ae`](https://github.com/opentargets/genetics_etl_python/commit/a0017aeb728cce292e8494bdf74b07ad0395f613))

* feat: ukbiobank components ([`e7b8562`](https://github.com/opentargets/genetics_etl_python/commit/e7b85627d412c2c3bc53b9ebf85cfbaaea636856))

* feat: add UKBiobank config yaml ([`fb2a6ba`](https://github.com/opentargets/genetics_etl_python/commit/fb2a6ba8a6f3c519424a381fd6f4b85db3754a36))

* feat: add UKBiobank step ID ([`dd5f77b`](https://github.com/opentargets/genetics_etl_python/commit/dd5f77b707969a129ff2c47f0fb5acf092d8bd9f))

* feat: add UKBiobank inputs and outputs ([`1e8fc0f`](https://github.com/opentargets/genetics_etl_python/commit/1e8fc0f9e5c7d5fe94580b6a35b6caa87cecbfdb))

* feat: add UKBiobankStepConfig class ([`34e573f`](https://github.com/opentargets/genetics_etl_python/commit/34e573f9fd51e41d1364077da51497999ed4f1ef))

* feat: add StudyIndexUKBiobank class ([`80d3cd0`](https://github.com/opentargets/genetics_etl_python/commit/80d3cd015c43b06b8055cd1d3673479b36ed80c2))

* feat: ukbiobank study ingestion ([`53dab79`](https://github.com/opentargets/genetics_etl_python/commit/53dab790359adce8767fa55eab419b752d84ea2d))

* feat: replace hardcoded parameters with cutomisable ones ([`bf0c5de`](https://github.com/opentargets/genetics_etl_python/commit/bf0c5de10756e0d084372448cdd7b43f359fa9cb))

* feat: run the code from the version specific path ([`452174b`](https://github.com/opentargets/genetics_etl_python/commit/452174bdfbd8cb93692cbe09ef3af311e6696e43))

* feat: upload code to a version specific path ([`db60882`](https://github.com/opentargets/genetics_etl_python/commit/db60882c8fd656db90510441d2dc69e20dcf8550))

* feat: extract `order_array_of_structs_by_field`, handle nulls and test ([`4bd64e2`](https://github.com/opentargets/genetics_etl_python/commit/4bd64e2fc5c0bbd73b6471a44adaac4d12ace521))

* feat: order output of `_variant_coordinates_in_ldindex` and tests ([`f0d6bc2`](https://github.com/opentargets/genetics_etl_python/commit/f0d6bc2a900a0b4784da810e649c67eccbda0cff))

* feat: extract `get_ld_annotated_assocs_for_population` ([`30fe8e3`](https://github.com/opentargets/genetics_etl_python/commit/30fe8e326248948b578a7fa631a6732b3cfe6f6f))

* feat: extract methods from `variants_in_ld_in_gnomad_pop` (passes) ([`2895e24`](https://github.com/opentargets/genetics_etl_python/commit/2895e24c1aa53460f0d83a5aa496edcca76b1586))

* feat: rewrite FinnGenStep to follow the new logic ([`4b866fe`](https://github.com/opentargets/genetics_etl_python/commit/4b866fe87fd1bcd008a74e95bc4ecd67e2dda6c5))

* feat: implement the new StudyIndexFinnGen class ([`fbe2719`](https://github.com/opentargets/genetics_etl_python/commit/fbe2719e795027cac795186e64b9a52d89e20404))

* feat: add step to DAG ([`0173326`](https://github.com/opentargets/genetics_etl_python/commit/0173326cbf2c27494a2e28f9a5fd43ad5293816c))

* feat: update FinnGen ingestion code in line with the current state of the main branch ([`e6b6397`](https://github.com/opentargets/genetics_etl_python/commit/e6b6397cd853f245be2dd7bbef994ee7402dd356))

* feat: pass configuration values through FinnGenStepConfig ([`ad5e529`](https://github.com/opentargets/genetics_etl_python/commit/ad5e52937dd555388afe433290b503aaa7462e22))

* feat: pass FinnGen configuration through my_finngen.yaml ([`0f113dd`](https://github.com/opentargets/genetics_etl_python/commit/0f113ddbb43486df1964407b6f653cebcecb804e))

* feat: rewrite EFO mapping in pure PySpark ([`6db602a`](https://github.com/opentargets/genetics_etl_python/commit/6db602a67561e6cc4e748d2272c7fb922d561f17))

* feat: validate FinnGen study table against schema ([`76911d1`](https://github.com/opentargets/genetics_etl_python/commit/76911d18a2daa71efe07f1c209114a36ea200ba3))

* feat: implement EFO mapping ([`7ce6437`](https://github.com/opentargets/genetics_etl_python/commit/7ce6437a50f84acc162443935ffb7a6f036464bd))

* feat: write FinnGen study table ingestion ([`19389e8`](https://github.com/opentargets/genetics_etl_python/commit/19389e8ee66579a4d23c2654fe9f0cf197f4e8a6))

* feat: added run script for FinnGen ingestion ([`be5431c`](https://github.com/opentargets/genetics_etl_python/commit/be5431c57f63e37da6d747fe8de62924c77984de))

* feat: fix PICS fine mapping function ([`cd4dee5`](https://github.com/opentargets/genetics_etl_python/commit/cd4dee540f6207f2aa919c69dc4b3ae9cb1c7fb1))

* feat: deprecate StudyLocus._is_in_credset ([`a4bb77f`](https://github.com/opentargets/genetics_etl_python/commit/a4bb77fbfdf89a45f650404a43ff5987f08ff3ed))

* feat: improved flattening of nested structures ([`b2c9e8b`](https://github.com/opentargets/genetics_etl_python/commit/b2c9e8b42811d6eb7790e416df987b26d0f53c03))

* feat: new annotate_credible_sets function and tests ([`779eaf3`](https://github.com/opentargets/genetics_etl_python/commit/779eaf3aac221526f05ac87c9ba8e49c10c263e6))

* feat: changes on the tests sessions with the hope to speed up tests ([`0292454`](https://github.com/opentargets/genetics_etl_python/commit/02924546031fa515ac6b0632f9d6c10289299771))

* feat: update dependencies to GCP 2.1 image version ([`8070a40`](https://github.com/opentargets/genetics_etl_python/commit/8070a4040ceaed4603da754ca91c3f3247ed87e4))

* feat: gpt commit summariser ([`5f6e4e1`](https://github.com/opentargets/genetics_etl_python/commit/5f6e4e1f1c224c4a1229f36ff9eecb994212c54b))

* feat: fixes associated with study locus tests ([`630d902`](https://github.com/opentargets/genetics_etl_python/commit/630d902e2776c8383d8c4d246f84f5cde4baf578))

* feat: first slimmed variant annotation version with partial tests ([`d321d90`](https://github.com/opentargets/genetics_etl_python/commit/d321d9028f7fc690723b9b51900126b40e215646))

* feat: docs badge as link

Docs badge as link ([`7847bd7`](https://github.com/opentargets/genetics_etl_python/commit/7847bd7a12fe50d14769f28e772283a54a73522a))

* feat: README minor updates

- docs badge added
- link to documentation
- remove outdated information
- reference to workflow ([`0c966ea`](https://github.com/opentargets/genetics_etl_python/commit/0c966eaeb435f1ccc69f84db1b621950eb3c1e90))

* feat: change  not to return False when df is of size 1 ([`be5b555`](https://github.com/opentargets/genetics_etl_python/commit/be5b5553b59f7bd8531f5d542d24f62cd38c9f8f))

* feat: add check for duplicated field to `validate_schema` ([`d0b3489`](https://github.com/opentargets/genetics_etl_python/commit/d0b34891e16bfa48aaf2c85170d562d8a70fe47c))

* feat: add support and tests for nested data ([`480539b`](https://github.com/opentargets/genetics_etl_python/commit/480539b2ebd8044a4f132cb8b2644f50bed92409))

* feat: merge with remote branch ([`ad95698`](https://github.com/opentargets/genetics_etl_python/commit/ad956980f48172b11cf6f1142631653800018a63))

* feat: add support and tests for nested data ([`51ad0aa`](https://github.com/opentargets/genetics_etl_python/commit/51ad0aa25b4de641f2d683d46909086374870a40))

* feat: added flatten_schema function and test ([`5414538`](https://github.com/opentargets/genetics_etl_python/commit/5414538497f9104e678eb4b80434d00cb637190c))

* feat: add type checking to validate_schema ([`1f8d9a1`](https://github.com/opentargets/genetics_etl_python/commit/1f8d9a18fe04a7cdd5a8cbbb6249e1122216ed72))

* feat: run tests on main when push (or merge) ([`59edc6f`](https://github.com/opentargets/genetics_etl_python/commit/59edc6f98805f66249149deb6b6d5b298938bc13))

* feat: run tests on main when push (or merge) ([`bf9054f`](https://github.com/opentargets/genetics_etl_python/commit/bf9054fc36332e4e962015496580de76e70da7a2))

* feat: run tests on main when push (or merge) ([`e6806aa`](https://github.com/opentargets/genetics_etl_python/commit/e6806aac4ffdf946fe81d42235f5e36119ac79a8))

* feat: redefine validate_schema to avoid nullability issues ([`f1a3fc1`](https://github.com/opentargets/genetics_etl_python/commit/f1a3fc188426c2a8856d2a459b53d819761814ee))

* feat: redefine `validate_schema` to avoid nullability issues ([`838d4f1`](https://github.com/opentargets/genetics_etl_python/commit/838d4f13a4b5af51c51b1a8e1a6a8e1c5f27df5f))

* feat: working version of dataproc workflow ([`5826f79`](https://github.com/opentargets/genetics_etl_python/commit/5826f796a511ced418f9f85f9bd2af242b00d205))

* feat: first working version of google workflow ([`e67ff48`](https://github.com/opentargets/genetics_etl_python/commit/e67ff48dc2785d3fb9875e0b72eafe17f7d3468e))

* feat: hail session functionality restored ([`6d0a361`](https://github.com/opentargets/genetics_etl_python/commit/6d0a361694292a338690a0e8dd2f3613b38f8924))

* feat: include pyright typechecking in vscode (no precommit) ([`547633e`](https://github.com/opentargets/genetics_etl_python/commit/547633e5c8a074706a8fec1c00b9c0a8f1ce3119))

* feat: sumstats ingestion added ([`e1a9d2c`](https://github.com/opentargets/genetics_etl_python/commit/e1a9d2c2540febadeab2cd36678585d0209c18eb))

* feat: step to preprocess sumstats added ([`d78e835`](https://github.com/opentargets/genetics_etl_python/commit/d78e8357e4081bc981ca4723d7f676b70b7c80c6))

* feat: half-cooked submission using dataproc workflows ([`3cb603b`](https://github.com/opentargets/genetics_etl_python/commit/3cb603b36b59bf6ccf8281c8747d438dcb50cf13))

* feat: vscode isort on save ([`01365ed`](https://github.com/opentargets/genetics_etl_python/commit/01365ed8e8bf1f27c1c86acaef0cf932e0b872dd))

* feat: updating summary stats ingestion ([`7a48fc4`](https://github.com/opentargets/genetics_etl_python/commit/7a48fc484dced3d26575912157cb5dc8e1591d33))

* feat: yaml config for gene index ([`1617635`](https://github.com/opentargets/genetics_etl_python/commit/1617635184172ef184bfd5b74b36106f56de9754))

* feat: fix CLI to work with hydra ([`931c6cd`](https://github.com/opentargets/genetics_etl_python/commit/931c6cd3f4b334a0b7841300ed7fcfddab2c97ff))

* feat: intervals to v2g refactor and test ([`25b86f8`](https://github.com/opentargets/genetics_etl_python/commit/25b86f89dcca7b267d2597668a1598ece11b1a2a))

* feat: remove gcfs dependency ([`7ac4b78`](https://github.com/opentargets/genetics_etl_python/commit/7ac4b78a0db84582eb9539bfbfc781b1692551da))

* feat: bugfixes associated with increased study index coverage ([`54f4fcf`](https://github.com/opentargets/genetics_etl_python/commit/54f4fcfc89bf2327895d900d4c79ae969e2126ec))

* feat: adding p-value filter for summary stats ([`e19e933`](https://github.com/opentargets/genetics_etl_python/commit/e19e933b9625bd60380d83bd65ac0113d953b7ab))

* feat: adding summary stats dataset ([`4613617`](https://github.com/opentargets/genetics_etl_python/commit/46136171f986a22692114bb140d9dc2ad067344a))

* feat: several fixes linked to increased test coverage ([`445e1cc`](https://github.com/opentargets/genetics_etl_python/commit/445e1cc7eb1995f88d9be2e40fa876cd5ccf15c4))

* feat: working hydra config with optional external yaml ([`ad53e7e`](https://github.com/opentargets/genetics_etl_python/commit/ad53e7efd315c10c1f9ac2fb9ce5ec4e1c6bd89f))

* feat: precompute LD index step ([`0756150`](https://github.com/opentargets/genetics_etl_python/commit/075615029785993e4157cd2d0f6d183d26749b46))

* feat: ld_clumping ([`3aa413f`](https://github.com/opentargets/genetics_etl_python/commit/3aa413fc662fbf23db524b92d58fb0b55eda0887))

* feat: r are combined by weighted mean ([`a1977c8`](https://github.com/opentargets/genetics_etl_python/commit/a1977c8692e299ce4816920cfe4e194ae3fa8d7b))

* feat: pics refactor ([`fa71843`](https://github.com/opentargets/genetics_etl_python/commit/fa71843737dd56a64ee593b398bc44c92164182e))

* feat: ld annotation ([`0bb9529`](https://github.com/opentargets/genetics_etl_python/commit/0bb952956cc610e2cc81e53cb2aed38e6b93923a))

* feat: gwas catalog splitter ([`1418908`](https://github.com/opentargets/genetics_etl_python/commit/1418908fe56ba68c5473e368789846ec136818f6))

* feat: contributors list ([`32de81f`](https://github.com/opentargets/genetics_etl_python/commit/32de81f4235291562af7b2111b9f29671fb0fcd8))

* feat: distance to TSS v2g feature ([`a628046`](https://github.com/opentargets/genetics_etl_python/commit/a628046f4af2ce3145470d27b2945f38ed43ea52))

* feat: schemas are now displayed in documentation! ([`4681940`](https://github.com/opentargets/genetics_etl_python/commit/46819403a7e0ac78809b4c7dd5ca5cfc68b34891))

* feat: default p-value threshold ([`9b00e01`](https://github.com/opentargets/genetics_etl_python/commit/9b00e015579b5f18e5e0cebed6addcaa0c0c95ca))

* feat: minor improvements in docs ([`a5f0f02`](https://github.com/opentargets/genetics_etl_python/commit/a5f0f02954561817972d6f7b8c707454a7e5734a))

* feat: major changes on association parsing ([`c1cfee9`](https://github.com/opentargets/genetics_etl_python/commit/c1cfee9e1b42d59acbc26dfb13e12fe2c8b53223))

* feat: graph based clumping ([`31cab8d`](https://github.com/opentargets/genetics_etl_python/commit/31cab8de2d0aa206211e578aa0fb701dd5e064b2))

* feat: function to install custom jars ([`1dd6796`](https://github.com/opentargets/genetics_etl_python/commit/1dd679643779392b7275f54596be07e644090c5d))

* feat: minor changes in the parser ([`cfb9f11`](https://github.com/opentargets/genetics_etl_python/commit/cfb9f11d9b8a728c94b12b915e58eef1c2b25176))

* feat: gwascat study ingestion preliminary version ([`5a101d6`](https://github.com/opentargets/genetics_etl_python/commit/5a101d6a7556b1e4aba1367a1690948a95260749))

* feat: incorporating iteration including colocalisation steps ([`d8bc103`](https://github.com/opentargets/genetics_etl_python/commit/d8bc1030a5650089a1e49b381c818dd8adc51169))

* feat: exploring tests with dbldatagen ([`335a689`](https://github.com/opentargets/genetics_etl_python/commit/335a68928fe89e2bf86e21bc23ac7c6ea5d8afa6))

* feat: more step config less class ([`7d38ba8`](https://github.com/opentargets/genetics_etl_python/commit/7d38ba89913cfcb105bf0a925c1aad2f216c40e9))

* feat: not tested va, vi and v2g ([`094d547`](https://github.com/opentargets/genetics_etl_python/commit/094d5475fd6eb3486d30a99a749daea5644de74f))

* feat: directory name changed ([`9893398`](https://github.com/opentargets/genetics_etl_python/commit/989339899c55e7b0a56f68c10742a62fad53c194))

* feat: merging main into il-v2g-distance ([`f75bb40`](https://github.com/opentargets/genetics_etl_python/commit/f75bb40902f3b92aca8cc5794250cb3f4d70c2c5))

* feat: cli working! ([`fa3bdbb`](https://github.com/opentargets/genetics_etl_python/commit/fa3bdbb7bed7f12b675f0755c77c7cc6f7f7c80c))

* feat: first prototype of CLI using do_hydra

Currently implements the variant_annotation and variant_index steps.

Note: During pre-commit pycln introduced a bug in the code by introducing
a functionality that it&#39;s only available in python 3.10+.
To fix it we need to revert `path: str | None` to `path: Optional[str]`
in `variant_annotation.py` and `variant_index.py`. ([`744bde6`](https://github.com/opentargets/genetics_etl_python/commit/744bde62754ef84c8a36f624edd91d075989d3aa))

* feat: poetry lock updated ([`f2167e5`](https://github.com/opentargets/genetics_etl_python/commit/f2167e56d45b03a9509665a015e1ffdae71a4622))

* feat: remove `phenotype_id_gene` dependency ([`c99d2a8`](https://github.com/opentargets/genetics_etl_python/commit/c99d2a890dbdb2dbc24db8c193b7ea3fc78e8132))

* feat: adapt coloc code to newer datasets ([`3ba7a2c`](https://github.com/opentargets/genetics_etl_python/commit/3ba7a2c2675d81cdfbe93e1e7d6d7779fc1215d9))

* feat: merge ([`ace2f31`](https://github.com/opentargets/genetics_etl_python/commit/ace2f31e91821557711a13aff06a4357cda7f7d6))

* feat: reverting flake8 to 5.0.4 ([`0956965`](https://github.com/opentargets/genetics_etl_python/commit/0956965cb36ae19685256df43e3c23c6271e7714))

* feat: feat: adapt coloc to follow new credset schema ([`c5a2e3b`](https://github.com/opentargets/genetics_etl_python/commit/c5a2e3b6346d2cb0f6906c83a33386fbdbb2be01))

* feat: ignore docstrings from private functions in documentation ([`e443211`](https://github.com/opentargets/genetics_etl_python/commit/e4432115bfff0734efb0a2556cfc8400a5a0f69a))

* feat: plugin not to automatically handle required pages ([`3f4b797`](https://github.com/opentargets/genetics_etl_python/commit/3f4b797a37152395f73924a8b3f68972847e9f67))

* feat: filter v2g based on biotypes ([`c044cd4`](https://github.com/opentargets/genetics_etl_python/commit/c044cd4170bebbbaaab2b5471c2fcb7ae1308a92))

* feat: generate v2g independently and read from generated intervals ([`38c7a9a`](https://github.com/opentargets/genetics_etl_python/commit/38c7a9a9d43b88e7617c50a0157875f1136cc74c))

* feat: compute distance to tss v2g functions ([`5c28bee`](https://github.com/opentargets/genetics_etl_python/commit/5c28bee93fed64bd928178bbd3290014ab61bc8a))

* feat: v2g schema v1.0 added ([`27da971`](https://github.com/opentargets/genetics_etl_python/commit/27da9712cefc17d7203fbd59ae3a0fd683f577cd))

* feat: add polyphen, plof, and sift to v2g ([`6b408ca`](https://github.com/opentargets/genetics_etl_python/commit/6b408cab59237136ab70017bd9f7671ec943fa6d))

* feat: trialing codecov coverage threshold ([`8ecb1b1`](https://github.com/opentargets/genetics_etl_python/commit/8ecb1b1cb1bcaaea31cc380eb547dac0c21da3a1))

* feat: integrate interval parsers in v2g model ([`7bc59ef`](https://github.com/opentargets/genetics_etl_python/commit/7bc59efe0bd21d5b80489d375bec82eebfb82f12))

* feat: rearrange interval scripts into vep ([`f1fc024`](https://github.com/opentargets/genetics_etl_python/commit/f1fc024b76e2c9aca4af54703f3be5c6a114178d))

* feat: add skeleton of vep processing ([`fed7eb2`](https://github.com/opentargets/genetics_etl_python/commit/fed7eb2bba4d763fc37f6bb89691a34a641da6d4))

* feat: ipython support

- iPython is installed as development dependency
- Vscode configured to run with ipython ([`1e1717d`](https://github.com/opentargets/genetics_etl_python/commit/1e1717ddcd68daf87d1f45d8f31745c7b28f34c5))

* feat: token added ([`3fa9c76`](https://github.com/opentargets/genetics_etl_python/commit/3fa9c761c0b7f3f053b8051536cc2b1d30dddaf2))

* feat: trigger testing on push ([`76fedda`](https://github.com/opentargets/genetics_etl_python/commit/76feddac76acdd52bbf9af2cbf551d75bfeed21c))

* feat: extra config for codecov ([`e86bfaa`](https://github.com/opentargets/genetics_etl_python/commit/e86bfaaa45f3e248fc18c6fd11e354fd377255dc))

* feat: enhanced codecov options ([`7ab4345`](https://github.com/opentargets/genetics_etl_python/commit/7ab4345c48a59310691ce5bb5c8014fa4c39f333))

* feat: codecov integration ([`9c88b43`](https://github.com/opentargets/genetics_etl_python/commit/9c88b4317f9776e731ef6c8a5759438d242f7809))

* feat: spark namespace to be reused in doctests ([`d9bbe5f`](https://github.com/opentargets/genetics_etl_python/commit/d9bbe5f5b4a84f58a2b6670691b4b79348f7982b))

* feat: validate studies schema ([`9e74b64`](https://github.com/opentargets/genetics_etl_python/commit/9e74b6416a9a776937947548acbd698bcd3418c8))

* feat: update actions to handle groups ([`23a7b6b`](https://github.com/opentargets/genetics_etl_python/commit/23a7b6b1175454762c88b1e29c3daca208bee861))

* feat: license added ([`c722d30`](https://github.com/opentargets/genetics_etl_python/commit/c722d306a640494a598713268d607993972a4978))

* feat: mkdocs github action ([`dcae258`](https://github.com/opentargets/genetics_etl_python/commit/dcae2581d074768b1786b960ef571d35d7202354))

* feat: first version of mkdocs site ([`875d445`](https://github.com/opentargets/genetics_etl_python/commit/875d445d503a1779827c0984183774731bee4946))

* feat: generate study table for GWAS Catalog ([`a0e05d0`](https://github.com/opentargets/genetics_etl_python/commit/a0e05d0424c7062346c93d5d6e2c8e12667497e9))

* feat: docs support

Overall support for docstrings
    - 100% docs coverage
    - docstring linters (flake8-docstring)
    - docstring content checkers (darlint)
    - docstring pre-commit
    - interrogate aims for 95% docs coverage
    - vscode settings bug fixed ([`d7d40c8`](https://github.com/opentargets/genetics_etl_python/commit/d7d40c8c4fda9e7059046616309d8d9407f2b1e1))

* feat: non-nullable fields required in validated df ([`01362c3`](https://github.com/opentargets/genetics_etl_python/commit/01362c330786752e745e119a4e7a4e6e69c02bd0))

* feat: intervals DataFrames are now validated ([`5fd35af`](https://github.com/opentargets/genetics_etl_python/commit/5fd35af91ab0abfc6ab628b099ed57754dfcdd39))

* feat: new mirrors-mypy precommit version ([`63ea0de`](https://github.com/opentargets/genetics_etl_python/commit/63ea0def8def7bf6e21233922eca3acb8f9cc1b1))

* feat: intervals schema added ([`d1b887b`](https://github.com/opentargets/genetics_etl_python/commit/d1b887bf84dfea1cc645f93b882dc5f8e6d31d0f))

* feat: function to validate dataframe schema ([`4393739`](https://github.com/opentargets/genetics_etl_python/commit/43937391fca84fbebcfb90f8c1012a37a5209947))

* feat: handling schemas on read parquet

Incorrect schemas crash read_parquet

- wrapper around read.parquet as part of `ETLSession`
- PoC schema added to project ([`8f02fa3`](https://github.com/opentargets/genetics_etl_python/commit/8f02fa355f187c4c7b2870a482db4a900344c01d))

* feat: adding effect harmonization to gwas ingestion ([`9e6bf85`](https://github.com/opentargets/genetics_etl_python/commit/9e6bf8562e6175e4659374014e09abdc890f8923))

* feat: adding functions to effect harmonizataion ([`dd0d2e9`](https://github.com/opentargets/genetics_etl_python/commit/dd0d2e94ff2e002bea4da8626129169207c6242f))

* feat: gwas association deduplication by minor allele frequency ([`3c1138e`](https://github.com/opentargets/genetics_etl_python/commit/3c1138ed4bb526d3763083ba37444f6b052f1a5a))

* feat: debugging in vscode

- Launch.json contains debugging capabilities
- Debug overwrites the config.path to use the ./configs instead of ./
- 2 different debug configurations use 2 hydra configurations:
    - dataproc setup for developing within a dataproc cluster
    - local setup to develop in local machine (with local files) ([`46db297`](https://github.com/opentargets/genetics_etl_python/commit/46db2975e780ae9e1d41f1a8645729626fe9fae0))

* feat: ignoring __pycache__ ([`7a201ff`](https://github.com/opentargets/genetics_etl_python/commit/7a201ffe430bac65f29c8034b94ae82d5219480a))

* feat: coverage added to dev environment ([`a9d2815`](https://github.com/opentargets/genetics_etl_python/commit/a9d28153361019e32e66bf738814edb85e4c9fad))

* feat: increased functionality of the gwas catalog ingestion + tests ([`ca43ecd`](https://github.com/opentargets/genetics_etl_python/commit/ca43ecde2b138a98166d628fac02391c672b103d))

* feat: mapping GWAS Catalog variants to GnomAD3 ([`4fa9876`](https://github.com/opentargets/genetics_etl_python/commit/4fa9876d59948e8f726007c7b9bd5cf5f8a0b702))

* feat: adding GWAS Catalog association ingest script ([`dc3c70d`](https://github.com/opentargets/genetics_etl_python/commit/dc3c70d7bfcc1da53568830f1ff5725dc1a52086))

* feat: black version bumped ([`e18dd65`](https://github.com/opentargets/genetics_etl_python/commit/e18dd656c5e88975d1d6ebadbe452e9cb4ae63c6))

* feat: adding variant annotation ([`ad57c6a`](https://github.com/opentargets/genetics_etl_python/commit/ad57c6a39cae4afb34b770f8c04226a1e39c9984))

* feat: updated dependencies ([`1e00d39`](https://github.com/opentargets/genetics_etl_python/commit/1e00d39577d86b4d2bc134c3eff5b9e9809ec539))

* feat: Adding parsers for intervals dataset. ([`57ab36d`](https://github.com/opentargets/genetics_etl_python/commit/57ab36de4868711166adbbcbdb4d4667e0910989))

* feat(pre-commit): commitlint hook added ([`e570742`](https://github.com/opentargets/genetics_etl_python/commit/e570742ab587059a9b2e7f0f51d64b73a699834c))

### Fix

* fix(_variant_coordinates_in_ldindex): order variant_coordinates df just by idx and not chromosome ([`fd4cd3d`](https://github.com/opentargets/genetics_etl_python/commit/fd4cd3d5bab29fb3ca3738466267e7c3ae629486))

* fix: start index at 0 in _variant_coordinates_in_ldindex ([`1e2ec4a`](https://github.com/opentargets/genetics_etl_python/commit/1e2ec4a5248676445d2139a8e04dea61be1feb1d))

* fix: updating tests ([`5b91bbd`](https://github.com/opentargets/genetics_etl_python/commit/5b91bbdbc624947d88e5450432442b785cd428c9))

* fix: update expectation in `test__finemap` ([`350a91a`](https://github.com/opentargets/genetics_etl_python/commit/350a91a8dc9774159f84d8cc2085f650337eef10))

* fix: comply with studyLocus requirements ([`17a7014`](https://github.com/opentargets/genetics_etl_python/commit/17a70147d148b17579ba43af5992321e9bb5da00))

* fix: avoid empty credible set (#3016) ([`684bb58`](https://github.com/opentargets/genetics_etl_python/commit/684bb58b2436cce3ef9fee8f7fa6744a0108291a))

* fix: pvalueMantissa as float in summary stats ([`9c4154a`](https://github.com/opentargets/genetics_etl_python/commit/9c4154a3d66c1678c6b08b034eb71701355d2dc5))

* fix: spark does not like functions in group bys ([`877671d`](https://github.com/opentargets/genetics_etl_python/commit/877671df4e2d1bf19928dd830947b7deeeee3b68))

* fix: stacking avatars in docs ([`917847e`](https://github.com/opentargets/genetics_etl_python/commit/917847ef658fef11f8bfb47ff6e619199ac77765))

* fix: typo in docstring ([`2266f58`](https://github.com/opentargets/genetics_etl_python/commit/2266f5863b46faf05c8232f1c9bf95e333bc45d2))

* fix: implement suggestions from code review ([`bb3ec04`](https://github.com/opentargets/genetics_etl_python/commit/bb3ec04fa49763faf32d6daafb539b86a1ae527e))

* fix: revert accidentally removed link ([`696b549`](https://github.com/opentargets/genetics_etl_python/commit/696b5495a1add55aeb972e5c713a3845d69066b6))

* fix: set pvalue text to null if no mapping is available ([`94e1a82`](https://github.com/opentargets/genetics_etl_python/commit/94e1a82396e9c90cc27525420eea7fdce3975a7f))

* fix: revert removing subStudyDescription from the schema ([`8a1da7e`](https://github.com/opentargets/genetics_etl_python/commit/8a1da7e7862ea3cf10d4cbb2a9f985627e2cc4d9))

* fix: use studyId instead of projectId as primary key ([`10a2e6d`](https://github.com/opentargets/genetics_etl_python/commit/10a2e6dde398a10d62cba78c017ce712133fca1a))

* fix: change substudy separator to &#34;/&#34; to fix efo parsing ([`8a44135`](https://github.com/opentargets/genetics_etl_python/commit/8a44135c0c1f9ddca769ae1b7e14dc5606d28e56))

* fix: resolve trait accounting for null p value texts ([`34d44aa`](https://github.com/opentargets/genetics_etl_python/commit/34d44aabe020a2de742df69c4f6d1605fd7cb8d0))

* fix: updated StudyIndexUKBiobank spark efficiency ([`3693739`](https://github.com/opentargets/genetics_etl_python/commit/36937392fc3438b162c4c7c9e2b2448fe849069f))

* fix: update gwascat projectid to gcst ([`d6ffc5b`](https://github.com/opentargets/genetics_etl_python/commit/d6ffc5b047d364ae8750e12e6b6d9dc8dcb1186c))

* fix: updated StudyIndexUKBiobank ([`20a5c8f`](https://github.com/opentargets/genetics_etl_python/commit/20a5c8fdc43af27b482885792c751417b28932e8))

* fix: incorrect parsing of study description ([`2b6a2ae`](https://github.com/opentargets/genetics_etl_python/commit/2b6a2ae0ecb31eb31802e282359da76c3837a78a))

* fix: right image in the workflow ([`1127fcb`](https://github.com/opentargets/genetics_etl_python/commit/1127fcb2d931e324fc2ededab39f350b6a122203))

* fix: update conftest.py read.csv

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`85b6f9d`](https://github.com/opentargets/genetics_etl_python/commit/85b6f9da3992fe386f34c18e11203e7cbe443e0d))

* fix: update gcp.yaml spacing

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`8527be8`](https://github.com/opentargets/genetics_etl_python/commit/8527be8611e247c25c92143465dc56adff6f17c0))

* fix: update gcp.yaml spacing

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`b7479f3`](https://github.com/opentargets/genetics_etl_python/commit/b7479f39535ae872404fe4a17fadb66b714b8d3d))

* fix: correct sumstats url ([`8b2ac60`](https://github.com/opentargets/genetics_etl_python/commit/8b2ac60d5ebe8523816d7c1d019b12254253b5b3))

* fix: decrease partitions by just coalescing ([`eebca6d`](https://github.com/opentargets/genetics_etl_python/commit/eebca6d2b34745d9723eae5561548fea734c6522))

* fix: variable interpolation in workflow_template.py ([`befab8f`](https://github.com/opentargets/genetics_etl_python/commit/befab8fdfd9a70970c789dbc9cca10d0364e9ef9))

* fix: reorganise workflow_template.py to make pytest and argparse work together ([`7f0b0c8`](https://github.com/opentargets/genetics_etl_python/commit/7f0b0c8a7c4cfbd81df82374cbc2586b281984f0))

* fix: typos ([`09e3521`](https://github.com/opentargets/genetics_etl_python/commit/09e352174793ed191dc6b3f007347a20ff8d1c1c))

* fix: revert accidental DAG changes ([`a90b7a2`](https://github.com/opentargets/genetics_etl_python/commit/a90b7a2398692dd5345d9d18418d5b1374d057f5))

* fix: reduce num partitions to fix job failure ([`e92dff2`](https://github.com/opentargets/genetics_etl_python/commit/e92dff27a0fc46494f373d7d11eec00dc59d5885))

* fix: handle empty credibleSet in `annotate_credible_sets` + test ([`d5c3a56`](https://github.com/opentargets/genetics_etl_python/commit/d5c3a56c16ceeaa468dd69f15a228652c3fc1ead))

* fix: control none and add test for _finemap + refactor ([`4a7be65`](https://github.com/opentargets/genetics_etl_python/commit/4a7be65c475a8e753019a1da3bb60617d1628867))

* fix: do not control credibleSet size for applying finemap udf ([`3ae745d`](https://github.com/opentargets/genetics_etl_python/commit/3ae745d994b09be5ed001e82accb1676f5b8707e))

* fix: remove use of aliases to ensure study index is updated ([`3f1a9ef`](https://github.com/opentargets/genetics_etl_python/commit/3f1a9efa4664bbe8a821200a8eb64c6e8e13a554))

* fix: adapt `finemap` to handle nulls + test ([`61c4f95`](https://github.com/opentargets/genetics_etl_python/commit/61c4f957a67b554062acdc628623de12df17e758))

* fix: remove null elements in credibleSet ([`d1452d7`](https://github.com/opentargets/genetics_etl_python/commit/d1452d76833ae83cc6c802b170969de89e2a4a7b))

* fix: handle null credibleSet as empty arrays ([`c15ac92`](https://github.com/opentargets/genetics_etl_python/commit/c15ac92c5dc0a4d341fabf81b981afb2ec215fa9))

* fix: define finemap udf schema from studylocus ([`4c52b99`](https://github.com/opentargets/genetics_etl_python/commit/4c52b996e20a1fb184858aeb8025b71e6c1f4e00))

* fix: add missing columns to `_variant_coordinates_in_ldindex` + new test ([`a39e74c`](https://github.com/opentargets/genetics_etl_python/commit/a39e74caa7682ffd51a2bae43b4b918a0463b153))

* fix: update indices column names in `get_ld_annotated_assocs_for_population` ([`7ed8b10`](https://github.com/opentargets/genetics_etl_python/commit/7ed8b10832098568d82f13bd779ab027e227f13d))

* fix: populate projectId and studyType constant value columns ([`21f5f81`](https://github.com/opentargets/genetics_etl_python/commit/21f5f81295e42fdc15ec6d150f17681f6a10c26d))

* fix: configuration target for FinnGenStepConfig ([`8bc8238`](https://github.com/opentargets/genetics_etl_python/commit/8bc8238eaf1966275715f29a1efe4b50113e3a86))

* fix: update lit column init to agree with `validate_df_schema` behaviour ([`ae77fa0`](https://github.com/opentargets/genetics_etl_python/commit/ae77fa04b9a8a83d992a1d29909133563da20bc9))

* fix: rewrite JSON ingestion with RDD and remove unnecessary dependencies ([`c0add46`](https://github.com/opentargets/genetics_etl_python/commit/c0add4681a7ba3c29d53d44c8e80b114211651dc))

* fix: configuration variable name ([`ae88fd1`](https://github.com/opentargets/genetics_etl_python/commit/ae88fd14a839e20b2ebae032130670c67ac2d68f))

* fix: several errors found during debugging ([`3a1182c`](https://github.com/opentargets/genetics_etl_python/commit/3a1182cf4d2ebe0c5cc81e81db4db5a10adcfee1))

* fix: resolve problems with Pyenv/Poetry installation ([`56aedaf`](https://github.com/opentargets/genetics_etl_python/commit/56aedafe684ad9b85aa13b83928ebaeca45b5a2f))

* fix: activate Poetry shell when setting up the dev environment ([`237c696`](https://github.com/opentargets/genetics_etl_python/commit/237c696e61f21709b92b635f7d32b706ce83b98f))

* fix: update `ld_index_template` according to new location ([`3c33b3f`](https://github.com/opentargets/genetics_etl_python/commit/3c33b3f75acafbf9dea8bd0f239d4c88007379c0))

* fix: revert changes in `.vscode` ([`1224643`](https://github.com/opentargets/genetics_etl_python/commit/1224643779e3f1fab535d67b8c0c68609129d289))

* fix: calculate r2 before applying weighting function ([`fed6ea9`](https://github.com/opentargets/genetics_etl_python/commit/fed6ea9b440b12f4d1b7de826381280a458b0a22))

* fix: restore .gitignore ([`69ec6cb`](https://github.com/opentargets/genetics_etl_python/commit/69ec6cb6bd1c89a23de55f12ddc06fe2a26f299e))

* fix: flatten_schema result test to pyspark 3.1 schema convention ([`c59ad3c`](https://github.com/opentargets/genetics_etl_python/commit/c59ad3c37651a420663a80bc25098f78be4e3db1))

* fix: revert to life pre-gpt ([`d4e5bf3`](https://github.com/opentargets/genetics_etl_python/commit/d4e5bf3e2b5d8baa7b0dc2d7eb6fb7c07fa11c5c))

* fix: altering order of columns to pass validation (provisional hack) ([`3535912`](https://github.com/opentargets/genetics_etl_python/commit/35359122f8015ce917cd1bd414f6ff929476fe8d))

* fix: cascading effects in other schemas ([`852dcac`](https://github.com/opentargets/genetics_etl_python/commit/852dcac32fe7fe76e087bd2c9ceac2660eb08267))

* fix: remove nested filter inside the window in ([`58c5d52`](https://github.com/opentargets/genetics_etl_python/commit/58c5d5250faff8dc2c6616595b3214e55d483116))

* fix: handle duplicated chrom in v2g generation ([`26d9aea`](https://github.com/opentargets/genetics_etl_python/commit/26d9aeae06a9be474b731d284839d64c5ed59c90))

* fix: `_annotate_discovery_sample_sizes` drop default fields before join ([`fe93cf5`](https://github.com/opentargets/genetics_etl_python/commit/fe93cf57091feba709fd4dbf54c2675952cad43a))

* fix: `_annotate_ancestries` drop default fields before join ([`dce0e76`](https://github.com/opentargets/genetics_etl_python/commit/dce0e76986a828dafbeea0c42ab88962149801a2))

* fix: `_annotate_sumstats_info` drop duplicated columns before join and coalesce ([`dec2cf4`](https://github.com/opentargets/genetics_etl_python/commit/dec2cf4dfd1c34f90946d92f7168cd1fe8d03b8f))

* fix: order ld index by idx and unpersist data ([`5a8e03a`](https://github.com/opentargets/genetics_etl_python/commit/5a8e03a7030c88b6b7c39f2ed20123598ff76dea))

* fix: correct attribute names for ld indices ([`b1d56c4`](https://github.com/opentargets/genetics_etl_python/commit/b1d56c48cac25ca11c587527c32df5ddbeddc004))

* fix: join `_variant_coordinates_in_ldindex` on `variantId`

variant_df does not have split coordinates ([`829ef6b`](https://github.com/opentargets/genetics_etl_python/commit/829ef6b982cc7c83c9fa95928140d23987cd67d3))

* fix: move rsId and concordance check outside the filter function

windows cannot be applied inside where clauses ([`c57919d`](https://github.com/opentargets/genetics_etl_python/commit/c57919d2d28e0cea8f4eb67f2b2fb2e30d273945))

* fix: update configure and gitignore ([`6cc9af1`](https://github.com/opentargets/genetics_etl_python/commit/6cc9af18a6061083abbd403b35034f56233dd226))

* fix: type issue ([`0450163`](https://github.com/opentargets/genetics_etl_python/commit/0450163d6ea9f67d524820b6df44f1aa8e4d7cd2))

* fix: typing issue ([`64e5591`](https://github.com/opentargets/genetics_etl_python/commit/64e5591e790750610b59e4adf0789a3011ddd6c0))

* fix: typing issue fixed ([`ddec460`](https://github.com/opentargets/genetics_etl_python/commit/ddec4609be27a39421da7744db59c3e9baa28a56))

* fix: typing and tests ([`7373b13`](https://github.com/opentargets/genetics_etl_python/commit/7373b13f55036b17e2998b3cf01304f0d94997d9))

* fix: right populations in config ([`fd7ceb8`](https://github.com/opentargets/genetics_etl_python/commit/fd7ceb8ba57e38d1a460026e531c651610b162b9))

* fix: gnomad LD populations updated ([`47f5873`](https://github.com/opentargets/genetics_etl_python/commit/47f58733a93ce659dd8c2421e95414e11d73a0d5))

* fix: blocking issues in variant_annotation ([`29a1f0e`](https://github.com/opentargets/genetics_etl_python/commit/29a1f0e5bf92cca7912d1792e6d2f167b8026a27))

* fix: blocking issues in ld_index ([`3fc30ed`](https://github.com/opentargets/genetics_etl_python/commit/3fc30edb9887d2b80fba9513f18faa90f6f4c9ce))

* fix: typing issue ([`f012ffd`](https://github.com/opentargets/genetics_etl_python/commit/f012ffd044c3530044bc06c76809b96f28e29b02))

* fix: tests working again ([`6bec918`](https://github.com/opentargets/genetics_etl_python/commit/6bec91882c7c8ea5ff4e8a69b0a7c4fb987f1e35))

* fix: missing f.lit ([`8e4cfcb`](https://github.com/opentargets/genetics_etl_python/commit/8e4cfcbc8840c1da1deaeafb2257bf4d80c6c058))

* fix: extensive fixes accross all codebase ([`2b48906`](https://github.com/opentargets/genetics_etl_python/commit/2b48906b9d3b89a911198e493a9f58a4d98b9ff7))

* fix: missing chain added ([`2d8815f`](https://github.com/opentargets/genetics_etl_python/commit/2d8815fdf8b56e4b68c8f5d4993d9d329b39d806))

* fix: typo in config ([`8ebde89`](https://github.com/opentargets/genetics_etl_python/commit/8ebde894563aed66c69e29dbdc1baa1d0074d4e8))

* fix: fixing sumstats column nullabitlity ([`62c20a9`](https://github.com/opentargets/genetics_etl_python/commit/62c20a960777acf467700e92be45765dee9f7910))

* fix: clearing up validation ([`2a727ee`](https://github.com/opentargets/genetics_etl_python/commit/2a727ee7c238ec4cfb63560e4feecef4f6398bd7))

* fix: steps as dataclasses ([`19dac76`](https://github.com/opentargets/genetics_etl_python/commit/19dac76a8cc1735b37aae0df3d751839236a4164))

* fix: incorrect config file ([`0f3495c`](https://github.com/opentargets/genetics_etl_python/commit/0f3495c00fd5dbc954f4a0ac4f4d02dbf83c7fc6))

* fix: fixing test allowing nullable column ([`45cd4fd`](https://github.com/opentargets/genetics_etl_python/commit/45cd4fdfc794ba72a01b73fb06ce7a8cd214f80e))

* fix: rename json directory to prevent conflicts ([`c941aa4`](https://github.com/opentargets/genetics_etl_python/commit/c941aa4cdc6ac7c14f8aff5bcff94ba3db7b3db5))

* fix: fixing doctest ([`2faefb4`](https://github.com/opentargets/genetics_etl_python/commit/2faefb469ab6f215ee57988bb98c884e252f9d36))

* fix: merging with do_hydra ([`87415c0`](https://github.com/opentargets/genetics_etl_python/commit/87415c0514210a2a7be61dd9b5434d5912c1f5e3))

* fix: addressing various comments ([`a866f3b`](https://github.com/opentargets/genetics_etl_python/commit/a866f3b1eed798364238da017d628aae97511289))

* fix: missing dependency for testing ([`735609c`](https://github.com/opentargets/genetics_etl_python/commit/735609c233b76198a4579e25f72650b4297414ea))

* fix: missed dbldatagen dependency ([`8cd76dd`](https://github.com/opentargets/genetics_etl_python/commit/8cd76dd3ec7e7f323d4bf4d1e0b64bed4115cf48))

* fix: pyupgrade to stop messing with typecheck ([`239f3db`](https://github.com/opentargets/genetics_etl_python/commit/239f3dbe75812e6c5547788ebfc60800a8c21dca))

* fix: operative pytest again ([`54255c3`](https://github.com/opentargets/genetics_etl_python/commit/54255c3f6b6dd24a5a03b084703f0e3849b4eb0b))

* fix: wrong import ([`bb76019`](https://github.com/opentargets/genetics_etl_python/commit/bb76019f768bff677d2d4bbe6517f9b143a690f3))

* fix: df not considered ([`608e371`](https://github.com/opentargets/genetics_etl_python/commit/608e371c5609eb70407121fec7a9b03af5efd746))

* fix: consolidate path changes ([`39d5276`](https://github.com/opentargets/genetics_etl_python/commit/39d5276eb9a48e9d3b6d0204e72d5523e96c1979))

* fix: using normalise column ([`9c1a294`](https://github.com/opentargets/genetics_etl_python/commit/9c1a294cc23375387edfdb038934ca495c83ddfb))

* fix: bug with config field ([`55a4f61`](https://github.com/opentargets/genetics_etl_python/commit/55a4f612c83bf718c55a540571a93222a48098ec))

* fix: exclude unneccessary content ([`0bb26a7`](https://github.com/opentargets/genetics_etl_python/commit/0bb26a7df554d4ee2ce4518a6dc14f333a92051a))

* fix: typo in the schema ([`4e112cc`](https://github.com/opentargets/genetics_etl_python/commit/4e112cc39e5e81fa39426c40ca9cd1df34daaf42))

* fix: import fix ([`0ddd969`](https://github.com/opentargets/genetics_etl_python/commit/0ddd969094b8f14cec85f5a42eb5e53f3039c39b))

* fix: finalizing claping logic ([`f27fd07`](https://github.com/opentargets/genetics_etl_python/commit/f27fd07fde21cedc1dfeee150c688e9e66915c44))

* fix: type fixes ([`edd67eb`](https://github.com/opentargets/genetics_etl_python/commit/edd67eb6aeddb3ce3918cfcbd8d08c5c41c4b215))

* fix: issues about clumping ([`b981941`](https://github.com/opentargets/genetics_etl_python/commit/b9819414ea15f20bae224b797cb8dc0a79117120))

* fix: merge conflicts ([`24174c0`](https://github.com/opentargets/genetics_etl_python/commit/24174c02fbd4d9cdd51d904280baf91380553873))

* fix: merge conflicts resolved ([`f34a016`](https://github.com/opentargets/genetics_etl_python/commit/f34a01667ce096d2ceb915b03d446d63fee36e2e))

* fix: incorrect super statement ([`f038772`](https://github.com/opentargets/genetics_etl_python/commit/f03877235f8214a7fa30afb5be74ebd2bf814a3c))

* fix: wrong paths ([`8753644`](https://github.com/opentargets/genetics_etl_python/commit/875364417b3949852f2650a6ac84dde035b52722))

* fix: fix normalisation of the inverse distance values ([`e1ba2b2`](https://github.com/opentargets/genetics_etl_python/commit/e1ba2b227ca98738232049d0e45076f74e84e459))

* fix: minor bugfixes ([`91586a4`](https://github.com/opentargets/genetics_etl_python/commit/91586a4a4d24d3027f6998f77a197807cc366209))

* fix: schema fix ([`874f1b3`](https://github.com/opentargets/genetics_etl_python/commit/874f1b338c2ce4fdc43820ebd09304548b9b46a8))

* fix: dropping pyupgrade due to problems with hydra compatibility in Union type ([`373a5ec`](https://github.com/opentargets/genetics_etl_python/commit/373a5eccdf69c900f447d93d55dafd65517d0af0))

* fix: comment out filter on chrom 22 ([`52a4ee2`](https://github.com/opentargets/genetics_etl_python/commit/52a4ee26aca968d34f5c12e615c040d4ac6afe94))

* fix: update tests ([`086452c`](https://github.com/opentargets/genetics_etl_python/commit/086452c5704b22607ce433407ee40a77461d73fc))

* fix: remove repartition step in intevals df ([`5ced127`](https://github.com/opentargets/genetics_etl_python/commit/5ced127813f77f0918d0a2a601872cc8f8259d45))

* fix: fix tests data definition ([`a382ffa`](https://github.com/opentargets/genetics_etl_python/commit/a382ffaf3876feae152e39cfd010f4062cc61d87))

* fix: revert unwanted change ([`d726ba2`](https://github.com/opentargets/genetics_etl_python/commit/d726ba260f9ecfd6b3a1a9517e299cb044917c55))

* fix: correct for nullable fields in `studies.json ([`e0cb874`](https://github.com/opentargets/genetics_etl_python/commit/e0cb874b4cd1cb2812703dd46072052d1d635276))

* fix: write gwas studies in overwrite mode ([`8b0031f`](https://github.com/opentargets/genetics_etl_python/commit/8b0031f29d35600c5c6eec947d578e754abb75d2))

* fix: correct column names of `mock_rsid_filter` ([`8c41c76`](https://github.com/opentargets/genetics_etl_python/commit/8c41c76aa007580d932f1e0e3e8c4fed796bea5b))

* fix: typo in mock data in mock_allele_columns ([`bca99a1`](https://github.com/opentargets/genetics_etl_python/commit/bca99a1889104a18dabb57d7b8b2c70b0ec1f470))

* fix: camel case associations dataset ([`ae26935`](https://github.com/opentargets/genetics_etl_python/commit/ae269359974be49c35e850817a9711ff380c74ed))

* fix: structure dev dependencies ([`782f27a`](https://github.com/opentargets/genetics_etl_python/commit/782f27a97ce68879f8d78fb19363cebf8e4dd737))

* fix: fetch-depth=0 ([`6fde7ee`](https://github.com/opentargets/genetics_etl_python/commit/6fde7eea6cd53d050db5ccbbe82d87ae53f90e44))

* fix: depth=0 as suggested by warning ([`43aa631`](https://github.com/opentargets/genetics_etl_python/commit/43aa631460733e5cf03712fc13155b87c7fb1a6b))

* fix: module needs to be installed for mkdocstring ([`377597a`](https://github.com/opentargets/genetics_etl_python/commit/377597a5a02628435c7de2633bad54f9208bb816))

* fix: docs gh action refactor ([`913a6f6`](https://github.com/opentargets/genetics_etl_python/commit/913a6f6b402028732fd7dd27e35cffa015140f29))

* fix: variable name changed ([`7773fe4`](https://github.com/opentargets/genetics_etl_python/commit/7773fe4063423f029bfb562fb0dcee7715c4cbc6))

* fix(pr comments): abstracting some of the hardcoded values, changing z-score calculation ([`589bbd9`](https://github.com/opentargets/genetics_etl_python/commit/589bbd9882c52aafeb04d61022b3721da5cbc4f6))

* fix: clash between vscode and pretty-format-json ([`4c5af7e`](https://github.com/opentargets/genetics_etl_python/commit/4c5af7e3c2875f8c741ee9b604b2140187dc03f3))

* fix: clash between black and pretty format json

Defaulting to 2 indent spaces (like black) ([`6c001c7`](https://github.com/opentargets/genetics_etl_python/commit/6c001c7c58f00019270da2bd080233033ad168c1))

* fix(config): parametrizing target index ([`18752d4`](https://github.com/opentargets/genetics_etl_python/commit/18752d41526d903f92160ef00ed8eaa7569c0653))

* fix(schema): camel casing column names ([`7dc16ea`](https://github.com/opentargets/genetics_etl_python/commit/7dc16ea38fa5e4a88cc44bbfa3fa6379c2442824))

* fix: updates for new gene index ([`b368f08`](https://github.com/opentargets/genetics_etl_python/commit/b368f0856498b8045274100ca7f4b60eb8aeb9a4))

* fix: flake FS002 issue resolved ([`305415e`](https://github.com/opentargets/genetics_etl_python/commit/305415e0b6e57ce921caf20d436cf7d1714af4e6))

* fix: copy target index into the release playground bucket ([`2058318`](https://github.com/opentargets/genetics_etl_python/commit/2058318762fe4433677c283173eef311cc8d5205))

* fix: fixed imports in intervals ([`e061f22`](https://github.com/opentargets/genetics_etl_python/commit/e061f22e47d5ba3cc9701a03283ba3b1bd3326bc))

* fix: might be required ([`51ea89e`](https://github.com/opentargets/genetics_etl_python/commit/51ea89e6a318f7f87863b333e4e27d434cb64f7e))

* fix: pytest gh actions now adds coverage ([`9f4b478`](https://github.com/opentargets/genetics_etl_python/commit/9f4b47801305f7d92e3f7dde8311b8d63fab85c9))

* fix: more fixes to gh action ([`debcf80`](https://github.com/opentargets/genetics_etl_python/commit/debcf8077bcd5d57ea76f750472bf8c04188e1e6))

* fix: pytest gh action ([`be491cc`](https://github.com/opentargets/genetics_etl_python/commit/be491ccfed125b10d1f1424cf72d8557f63d4b85))

* fix: gwas ingestion adapted to new structure

- makefile works for step
- avoids to pass config object inside the module ([`c791da3`](https://github.com/opentargets/genetics_etl_python/commit/c791da3eab27a6c841e26817f433c1bc09990e03))

* fix: testing strategy moved to github actions ([`5fab4f7`](https://github.com/opentargets/genetics_etl_python/commit/5fab4f719cdd4d37ec5ba24b48097b00bc71e9a9))

* fix: pytest hook fixed ([`9b59a7c`](https://github.com/opentargets/genetics_etl_python/commit/9b59a7cdf2ea2fbbf009086f3b31fb62757e72e3))

* fix: python version and dependencies

- Documentation more clear about how to handle python version
- Windows requirements ignored on build ([`3886693`](https://github.com/opentargets/genetics_etl_python/commit/38866933963a4e8674fd047bbd895a4e122d922f))

* fix: adding hail to dependency + moving gcsfs to production with the same version as on pyspark ([`b6d7d07`](https://github.com/opentargets/genetics_etl_python/commit/b6d7d07e4586ae372e3fcfd0d42c4bb8fcbec741))

* fix: fixing python and spark version

Adjust to dataproc versions ([`b5be97d`](https://github.com/opentargets/genetics_etl_python/commit/b5be97dbca460033bd0dcf35e7103bbbde48f403))

* fix: Missing interval parser added. ([`34914c4`](https://github.com/opentargets/genetics_etl_python/commit/34914c4d818b99da77c59af6133e88049dd66d6a))

* fix: commitlint hook was not working ([`b6f13b5`](https://github.com/opentargets/genetics_etl_python/commit/b6f13b50e35e9d2829300f2079f5d98ef76cf623))

### Performance

* perf: do not enforce nullability status ([`7b07147`](https://github.com/opentargets/genetics_etl_python/commit/7b071475f6d564b362eaa3b585ed9696c36cb69d))

* perf: replace withColumn calls with a select ([`5d82d00`](https://github.com/opentargets/genetics_etl_python/commit/5d82d008379859fcf19e8862ce67d0d1e8b33e6b))

### Refactor

* refactor: the logic in calculate_confidence_interval_for_summary_statistics is moved ([`c73275a`](https://github.com/opentargets/genetics_etl_python/commit/c73275ab01234692e1ec92f452413ae2a7ac5b7b))

* refactor(ld_annotation_by_locus_ancestry): use select instead of drop for intelligibility ([`dc2132d`](https://github.com/opentargets/genetics_etl_python/commit/dc2132de5fdefde08c5aa9923e3c5c19e0750b26))

* refactor: create credibleSet according to full schema ([`f085065`](https://github.com/opentargets/genetics_etl_python/commit/f0850653e519ab8e5a81ff0e3499c9a88b416df2))

* refactor(ld_annotation_by_locus_ancestry): get unique pops ([`3f842c9`](https://github.com/opentargets/genetics_etl_python/commit/3f842c9aa9a5636a58191fe9f7b8e79d6c8ad22c))

* refactor: some minor issues sorted out around summary statistics ([`5db1731`](https://github.com/opentargets/genetics_etl_python/commit/5db1731ae8ca92570c8a5c3c7449b31cfb1b8d41))

* refactor(ldindex): remove unnecessary nullability check ([`2df071c`](https://github.com/opentargets/genetics_etl_python/commit/2df071c7003a6948b8d815085de6b9c3a9a13a92))

* refactor: remoce subStudyDescription from associations dataset ([`0ce6a45`](https://github.com/opentargets/genetics_etl_python/commit/0ce6a45e4739cfce2253654ea8c0a0ea9b019c8a))

* refactor: deprecate _is_in_credset ([`2f2a3eb`](https://github.com/opentargets/genetics_etl_python/commit/2f2a3eb07c27feea28113d0173b02c14dfaa3c19))

* refactor: remove chromosome from is_in_credset function ([`c317c55`](https://github.com/opentargets/genetics_etl_python/commit/c317c552c728f1ac20d190d6357db3993cf20a2d))

* refactor: drop redundants in tests

redundancy only happens in the testing contest ([`b22feb3`](https://github.com/opentargets/genetics_etl_python/commit/b22feb3e820c19e0ef83c1a3a9f1666ef5358a2d))

* refactor: exploring custom jar integration ([`19ee061`](https://github.com/opentargets/genetics_etl_python/commit/19ee0612348a89c074b94abf724e75014fb2ef2a))

* refactor: review biotype filter ([`aa8148b`](https://github.com/opentargets/genetics_etl_python/commit/aa8148b7467aa48eef76ef8be78a78f05468c13f))

* refactor: rename to `geneFromPhenotypeId` ([`9528b72`](https://github.com/opentargets/genetics_etl_python/commit/9528b72c9362ee082180c7b480706c1e25e4915a))

* refactor: rename `coloc_metadata` to `utils` to add `extract_credible_sets` fun ([`9d60925`](https://github.com/opentargets/genetics_etl_python/commit/9d609257cdc58871e2d7f033dd901c972552b63e))

* refactor: state private methods and rename `coloc_utils` folder ([`f627500`](https://github.com/opentargets/genetics_etl_python/commit/f62750067dbc16e93ce4b90b22a227bb2a86ebef))

* refactor: simplify logic in run_coloc ([`cfbc1b7`](https://github.com/opentargets/genetics_etl_python/commit/cfbc1b763876b6d7bbbc357107e92313f88c09f6))

* refactor: find_all_vs_all_overlapping_signals processes a df ([`55d3f8e`](https://github.com/opentargets/genetics_etl_python/commit/55d3f8e1b2c6acedbe8d2ca4d90856b22dcb69c1))

* refactor: export indepdently, part by chrom and ordered by pos ([`3fdb2eb`](https://github.com/opentargets/genetics_etl_python/commit/3fdb2ebd161e19d47fe8bdcff1de8ba9e4449aca))

* refactor: extract the logic to read data from the main function to reuse the dfs ([`4535077`](https://github.com/opentargets/genetics_etl_python/commit/4535077b8b1733bff87600b011ba827108a912d0))

* refactor: convert parameters to dataframes, not paths ([`126ffeb`](https://github.com/opentargets/genetics_etl_python/commit/126ffeb6ba214d8eeafa7dc1907f6be152b048dc))

* refactor: bigger machine, added score to jung, optimisations ([`655c07b`](https://github.com/opentargets/genetics_etl_python/commit/655c07b03b8dc25e8b64879516ad032c81aa788d))

* refactor: adapt config and remove run_intervals.py ([`19f78c9`](https://github.com/opentargets/genetics_etl_python/commit/19f78c93e85dd1076ad1648cb2940722c806d513))

* refactor: default maximum window for liftover set to 0 ([`2855145`](https://github.com/opentargets/genetics_etl_python/commit/2855145b1c38303d1dbb74732ac5290c99620246))

* refactor: minor improvements ([`5c20160`](https://github.com/opentargets/genetics_etl_python/commit/5c2016080c1be4b6802c49b496daafca14a78071))

* refactor: apply review suggestions ([`07ab14c`](https://github.com/opentargets/genetics_etl_python/commit/07ab14ca7d48f18cf4c0827d681513198b3cab57))

* refactor: optimise `test_gwas_process_assoc` ([`bcd908d`](https://github.com/opentargets/genetics_etl_python/commit/bcd908d4390b38c7db7927aac661f2dd34ee53f1))

* refactor: reorder `process_associations` ([`7460eda`](https://github.com/opentargets/genetics_etl_python/commit/7460eda5c11c8f06883bbe3ab7be37ed965a5e5b))

* refactor: adapt `map_variants` to variant schema ([`425a4bb`](https://github.com/opentargets/genetics_etl_python/commit/425a4bbbc809104537bc2131556edc24c354983e))

* refactor: reorder `ingest_gwas_catalog_studies` ([`8f4efee`](https://github.com/opentargets/genetics_etl_python/commit/8f4efee7423a7309411deb9bfd929b977fb62ac3))

* refactor: slim `extract_discovery_sample_sizes` ([`3e2e40f`](https://github.com/opentargets/genetics_etl_python/commit/3e2e40fc1f8a15703298b53ce07c0d019bf727ec))

* refactor: optimise study sumstats annotation ([`63be3c3`](https://github.com/opentargets/genetics_etl_python/commit/63be3c3af06cf4121ed8a8c920480ccda685f076))

* refactor: optimise `parse_efos` ([`695b4f0`](https://github.com/opentargets/genetics_etl_python/commit/695b4f004c13c4dc28b10f2db9fb4e7a28e4cc48))

* refactor: use select statements in `get_sumstats_location` ([`886bdca`](https://github.com/opentargets/genetics_etl_python/commit/886bdca5b3c0d0ee78ff5967283917ea0f141ff8))

* refactor: merging main branch ([`e2ca92d`](https://github.com/opentargets/genetics_etl_python/commit/e2ca92d829f77337f21a0b43e0cdaac1ecf77717))

### Style

* style: import row ([`de85023`](https://github.com/opentargets/genetics_etl_python/commit/de85023fd8ab19e09f9b1ea583d52bf115c2f1ba))

* style: reorganise individual function calls into a concatenated chain of calls ([`7a4a8d6`](https://github.com/opentargets/genetics_etl_python/commit/7a4a8d6a948208bfd8def728e9661ecdfa87f5a9))

* style: uniform comments in gcp.yaml ([`6978170`](https://github.com/opentargets/genetics_etl_python/commit/69781709584043c5485dd74dec9570ba35b7760e))

* style: reorganise configuration in line with the new structure ([`eed3d9e`](https://github.com/opentargets/genetics_etl_python/commit/eed3d9e095a660ebe078f41096e81fa7ce086318))

* style: rename ld indices location to directory and no extension ([`512c842`](https://github.com/opentargets/genetics_etl_python/commit/512c842e7e5ac744820b017fc5640b8bf009ef24))

* style: using fstring for concatenation ([`2855069`](https://github.com/opentargets/genetics_etl_python/commit/28550697affe1151a28073549d61a0e30f273d06))

* style: validate variant inputs ([`ebe6717`](https://github.com/opentargets/genetics_etl_python/commit/ebe6717b1207d1629685a71421b4cbce3c4b590b))

### Test

* test: add `calculate_confidence_interval` doctest ([`8ab0e88`](https://github.com/opentargets/genetics_etl_python/commit/8ab0e88f07507981df13adafe75209bb6d137264))

* test: add `test__finemap` ([`7e7fb27`](https://github.com/opentargets/genetics_etl_python/commit/7e7fb27a50c127da0f50f963ddb138f2fa9abe67))

* test: update failing test ([`0343621`](https://github.com/opentargets/genetics_etl_python/commit/0343621a985cac92933f849b1010d13ea6863d4c))

* test: more clarity in _collect_clump test ([`31363aa`](https://github.com/opentargets/genetics_etl_python/commit/31363aaa933a2a3fadd06b8f8d79826ec682d751))

* test: _filter_leads functions doctest ([`e6f0343`](https://github.com/opentargets/genetics_etl_python/commit/e6f0343061b2c2762129a301ce2215dc296ce3d3))

* test: testing for window based clumping function ([`aedecb9`](https://github.com/opentargets/genetics_etl_python/commit/aedecb981922993ef5da6a756ba0b5811bcc4c27))

* test: update test sample data ([`e5ca634`](https://github.com/opentargets/genetics_etl_python/commit/e5ca63496c3b488c186fe144f10a5a3ce9c28bda))

* test: update tests ([`b28a642`](https://github.com/opentargets/genetics_etl_python/commit/b28a6422255b5a0aeb3409522b318278530e7fd0))

* test: add UKBiobank study index test ([`3f79d82`](https://github.com/opentargets/genetics_etl_python/commit/3f79d82a97b88be5181807e9431393a99ca19952))

* test: add UKBiobank study configuration test ([`ebe8a63`](https://github.com/opentargets/genetics_etl_python/commit/ebe8a63d9f01f27af319de3378c26214087224f5))

* test: add `test_finemap_pipeline` ([`56fc5a8`](https://github.com/opentargets/genetics_etl_python/commit/56fc5a82a852da32a65f6ea1427a597c55cbf36a))

* test: create non zero p values ([`97bcdc3`](https://github.com/opentargets/genetics_etl_python/commit/97bcdc3f369bf19539b500197999a9bef90cba4f))

* test: add `test_finemap_null_r2` (fails) ([`56c782a`](https://github.com/opentargets/genetics_etl_python/commit/56c782ae58e25efbf21b385cab6cd2a8d5027711))

* test: improve ld tests ([`7d2eb68`](https://github.com/opentargets/genetics_etl_python/commit/7d2eb6878251681216dbb9661c3f35b2ec729433))

* test: fix FinnGen study test ([`bc3ec92`](https://github.com/opentargets/genetics_etl_python/commit/bc3ec92c971677a2df6994f0b7ed4f85fda592e1))

* test: test ingestion from source for StudyIndexFinnGen ([`08cb9f9`](https://github.com/opentargets/genetics_etl_python/commit/08cb9f96b95772129b4031a4bef0b27dd7d8b8a8))

* test: implement sample FinnGen study data fixture ([`d4b64bc`](https://github.com/opentargets/genetics_etl_python/commit/d4b64bccf365edc2c08671a47f3d0bfd69a9364e))

* test: prepare sample FinnGen study data ([`c1a69ac`](https://github.com/opentargets/genetics_etl_python/commit/c1a69acbca4e8ef2a557e3dc5123a3f530f63134))

* test: test schema compliance for StudyIndexFinnGen ([`e9a6a2e`](https://github.com/opentargets/genetics_etl_python/commit/e9a6a2e8ab0b34f8628a68a068ed48d9974e7499))

* test: implement mock study index fixture for FinnGen ([`1125d64`](https://github.com/opentargets/genetics_etl_python/commit/1125d645e437b1b2bdf48d76fcb7bd43981449ec))

* test: add `test_variants_in_ld_in_gnomad_pop` (hail misconfiguration error) ([`8cabb5f`](https://github.com/opentargets/genetics_etl_python/commit/8cabb5f61e1f8698d25e1aa483dac605c8ed3907))

* test: add test_variant_coordinates_in_ldindex (passes) ([`840dcc7`](https://github.com/opentargets/genetics_etl_python/commit/840dcc7987cdd0168dadfb3ae8960fbd9a12ffc6))

* test: doctest dataframe ordering unchanged ([`0494730`](https://github.com/opentargets/genetics_etl_python/commit/0494730881bafa790a17966f51abe2ef15f9af75))

* test: map_to_variant_annotation_variants ([`b99b58d`](https://github.com/opentargets/genetics_etl_python/commit/b99b58da903e1c7394439c0af48de2cc1f05aa83))

* test: study locus gwas catalog from source ([`a27b51f`](https://github.com/opentargets/genetics_etl_python/commit/a27b51ffa2fa357bb3ff7e5108da60bbb059b82d))

* test: added ([`577c50a`](https://github.com/opentargets/genetics_etl_python/commit/577c50aa4117f47c31c33c9b1543effad238b7fa))

* test: is in credset function ([`65bc2aa`](https://github.com/opentargets/genetics_etl_python/commit/65bc2aace3bc76f735f57a792f1e9ac1b47a3d47))

* test: added `test_validate_schema_different_datatype` ([`f76e62f`](https://github.com/opentargets/genetics_etl_python/commit/f76e62f4d562e497839ea4f66b00e194a3498757))

* test: add `TestValidateSchema` suite ([`b997962`](https://github.com/opentargets/genetics_etl_python/commit/b9979622cda602dc806a5b17db76eeccc8313b0f))

* test: convert_odds_ratio_to_beta doctest ([`483822c`](https://github.com/opentargets/genetics_etl_python/commit/483822c8f83a787617b2993e0975e59942811f44))

* test: adding more tests to summary statistics ([`c4d7a78`](https://github.com/opentargets/genetics_etl_python/commit/c4d7a78e4a17ec9c97d106d755be373844cc344c))

* test: gene_index step added ([`db081ec`](https://github.com/opentargets/genetics_etl_python/commit/db081ece3cd58b03052f98021a06c041a57e4400))

* test: concatenate substudy description ([`4d598ef`](https://github.com/opentargets/genetics_etl_python/commit/4d598ef84671ac88dbd0867c9b270c07f33a03ea))

* test: qc_all in study-locus ([`517ac48`](https://github.com/opentargets/genetics_etl_python/commit/517ac4832bda032e69219f568ca95a8ea06cd774))

* test: more qc tests ([`a7ab2a5`](https://github.com/opentargets/genetics_etl_python/commit/a7ab2a523a06ae88927e2d390143b9df004c058a))

* test: qc incomplete mapping ([`4c9d46a`](https://github.com/opentargets/genetics_etl_python/commit/4c9d46a4667d2a420433d7c86ee6a76deac11699))

* test: qc unmapped variants ([`2db11ab`](https://github.com/opentargets/genetics_etl_python/commit/2db11ab9c9cb477f6a3b11dec97894ddb9cb66dd))

* test: gnomad position to ensembl ([`23bab11`](https://github.com/opentargets/genetics_etl_python/commit/23bab11743f9f961a05a8dce7c6fa36550d65b1c))

* test: helpers ([`49d5586`](https://github.com/opentargets/genetics_etl_python/commit/49d558614587524be784aa1b1df42451859f021c))

* test: session ([`628eca9`](https://github.com/opentargets/genetics_etl_python/commit/628eca9bed663b348abd4e7e6b0e6a753168227e))

* test: splitter ([`d6669bb`](https://github.com/opentargets/genetics_etl_python/commit/d6669bbfbe190ff682097234dd19aa99b6e4f487))

* test: additional coverage in ld_index and gene_index datasets ([`733394f`](https://github.com/opentargets/genetics_etl_python/commit/733394ffc480dcdcc7af433a9b1fb18e8f470f1d))

* test: additional tests ([`4e50e45`](https://github.com/opentargets/genetics_etl_python/commit/4e50e45fb4d5c8dfd609a1917a3020d85edc8755))

* test: pvalue normalisation ([`57711d4`](https://github.com/opentargets/genetics_etl_python/commit/57711d4b2a89fef68991c3f81dc6a9e7578d4557))

* test: failing test ([`1d038c7`](https://github.com/opentargets/genetics_etl_python/commit/1d038c7bfed02426dcc1f1a85117cdb46009f5f8))

* test: pvalue parser ([`bbe9f7d`](https://github.com/opentargets/genetics_etl_python/commit/bbe9f7db4b3403425da9e77d5351cc8227a64fd5))

* test: unnecessary tests ([`9f896a5`](https://github.com/opentargets/genetics_etl_python/commit/9f896a53dc820d68aa3d8f3a58779bde170102e9))

* test: palindromic alleles ([`dafb66c`](https://github.com/opentargets/genetics_etl_python/commit/dafb66c503ef9934334eb7c63868ea9c63327720))

* test: adding tests for summary stats ingestion ([`64194d7`](https://github.com/opentargets/genetics_etl_python/commit/64194d7230793722f90ba67e913fcc43db3d8a54))

* test: pytest syntax complies with flake ([`c29c29d`](https://github.com/opentargets/genetics_etl_python/commit/c29c29d783c6f6b969148c02ae0c4262bf37780d))

* test: ld clumping test ([`13cf805`](https://github.com/opentargets/genetics_etl_python/commit/13cf80579f0b2d6a89b0cd9527fbbb65d7e4e31d))

* test: archive deprecated tests and ignore them ([`aae3978`](https://github.com/opentargets/genetics_etl_python/commit/aae3978284aa8baed647a4928464338c648f59d0))

* test: added tests for distance features ([`680184e`](https://github.com/opentargets/genetics_etl_python/commit/680184e3f0e4195c8638bc0f57fd69c6d47fb5e4))

* test: add validation step ([`e89e48c`](https://github.com/opentargets/genetics_etl_python/commit/e89e48c26863713dda4982530ca2cf7289138d84))

* test: refactor of testing suite ([`9aa4014`](https://github.com/opentargets/genetics_etl_python/commit/9aa401448b7cbfc568d3554e0bc2007ddf11b44d))

* test: added test suite for v2g evidence ([`9745304`](https://github.com/opentargets/genetics_etl_python/commit/974530459614aab1d7ca2c0c9dce60ca1bf64971))

* test: test only running on PRs again ([`53873ee`](https://github.com/opentargets/genetics_etl_python/commit/53873ee20aa638b5ce6ab8404bd8f0c68d12f270))

* test: revert change ([`6aaacc2`](https://github.com/opentargets/genetics_etl_python/commit/6aaacc29ffc97a962e4b0daa709691be88b1eea3))

* test: adapted to camelcase variables ([`ab3d4da`](https://github.com/opentargets/genetics_etl_python/commit/ab3d4daa3e2e8384ea69a3bef2965df427a51515))

* test: checking all schemas are valid ([`57712bb`](https://github.com/opentargets/genetics_etl_python/commit/57712bb57f0cfb802f40673d6c712ffedf17d938))

* test: pytest added to vscode configuration ([`9048fd8`](https://github.com/opentargets/genetics_etl_python/commit/9048fd8680038c3a6da511eb31b3e37251edabb4))

* test: nicer outputs with pytest-sugar ([`49928a5`](https://github.com/opentargets/genetics_etl_python/commit/49928a545a117a7ad2611d3a14f7cab24d630a85))

* test: adding more tests on v2d ([`0b586ab`](https://github.com/opentargets/genetics_etl_python/commit/0b586ab391c74edaad595309042ba9113106d8ff))

### Unknown

* Merge pull request #116 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`9396369`](https://github.com/opentargets/genetics_etl_python/commit/9396369208661b88611f250473dcaa366b35e4d4))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.2.0 → v2.2.1](https://github.com/hadialqattan/pycln/compare/v2.2.0...v2.2.1) ([`2a1c6c6`](https://github.com/opentargets/genetics_etl_python/commit/2a1c6c63515f1a2693df3d520eef6ea2c95c99a6))

* Merge pull request #114 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`8335528`](https://github.com/opentargets/genetics_etl_python/commit/8335528fdf7268ca858d8f6f2266c1a0aab8ae52))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.6 → v2.2.0](https://github.com/hadialqattan/pycln/compare/v2.1.6...v2.2.0)
- [github.com/pycqa/flake8: 6.0.0 → 6.1.0](https://github.com/pycqa/flake8/compare/6.0.0...6.1.0) ([`9b89b4e`](https://github.com/opentargets/genetics_etl_python/commit/9b89b4e14852cb991c6ee1bf306dd3998e3a45c4))

* Merge pull request #108 from opentargets/il-fix-ld-annotation

Fixes to the `annotate_ld` function ([`536a9d1`](https://github.com/opentargets/genetics_etl_python/commit/536a9d1b9735d36dfe3fb96373a3d7d6b2db5600))

* Merge pull request #111 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`c8079c7`](https://github.com/opentargets/genetics_etl_python/commit/c8079c707e7db7e26e99fa6990fc73e829fad3c8))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.5 → v2.1.6](https://github.com/hadialqattan/pycln/compare/v2.1.5...v2.1.6) ([`01c1280`](https://github.com/opentargets/genetics_etl_python/commit/01c1280576b3ff7551cc0b121c3ca2621e5efb04))

* Merge pull request #106 from opentargets/ds_3017_update_sumstats

refactor: some minor issues sorted out around summary statistics ([`0d976ba`](https://github.com/opentargets/genetics_etl_python/commit/0d976ba74ab5e442652518a5db34970e31190037))

* Merge pull request #107 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`883347e`](https://github.com/opentargets/genetics_etl_python/commit/883347e3ba8e66041e79336e1b988f34fd470b2e))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/psf/black: 23.3.0 → 23.7.0](https://github.com/psf/black/compare/23.3.0...23.7.0) ([`3b0cd7b`](https://github.com/opentargets/genetics_etl_python/commit/3b0cd7be1707e5efdd79a6230d22910a5914fb01))

* Merge pull request #105 from opentargets:il-ldindex-duplicates

Remove ambiguous variants from LDIndex ([`0b8b9b7`](https://github.com/opentargets/genetics_etl_python/commit/0b8b9b7ee29637c65bcfb1bf74063e785605da8e))

* Merge pull request #97 from opentargets:ds_sumstats_to_locus

Finding loci via a distance based clumping ([`630d8ef`](https://github.com/opentargets/genetics_etl_python/commit/630d8ef8ec1f5411482f4d48f539e821abd42818))

* Merge pull request #104 from opentargets:il-fix-pics

Add `test__finemap` ([`c2a7513`](https://github.com/opentargets/genetics_etl_python/commit/c2a75136ce9f1c194c84d6a25b6438288c4260d6))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_sumstats_to_locus ([`ec2b229`](https://github.com/opentargets/genetics_etl_python/commit/ec2b22950b5f5b232364185443aeb916aebbd119))

* Merge pull request #103 from opentargets/il-studylocusid-func

feat: add `get_study_locus_id` ([`69e5abf`](https://github.com/opentargets/genetics_etl_python/commit/69e5abf5528e479ff8f908eba9f2911f58046e68))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_sumstats_to_locus ([`c842213`](https://github.com/opentargets/genetics_etl_python/commit/c842213ebf498c281f7beb91ad0d4e0e3a4eeb00))

* Merge pull request #102 from opentargets/il-fix-pics

fix: avoid empty credible set (#3016) ([`8fd06d7`](https://github.com/opentargets/genetics_etl_python/commit/8fd06d734fcb245e69e6197fcea0d6c4994583cf))

* Merge pull request #100 from opentargets/do_mantissa_float

fix: pValueMantissa as float in summary stats ([`dcac9b9`](https://github.com/opentargets/genetics_etl_python/commit/dcac9b9f501d0657c5adb5e5b278716724e3897c))

* Merge pull request #101 from opentargets/il-ld-clumping

feat: fix ldclumping, tests, and docs ([`c9d0bba`](https://github.com/opentargets/genetics_etl_python/commit/c9d0bbaf7d56313cf974bf2df3fccba8a8e606bc))

* Update src/otg/method/window_based_clumping.py ([`cb56d99`](https://github.com/opentargets/genetics_etl_python/commit/cb56d990d8172168a7c6d063c56a9447c9ffdbbc))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`5c5e0cd`](https://github.com/opentargets/genetics_etl_python/commit/5c5e0cd922e8977ed35b660da717d926575e0b2e))

* Update src/otg/method/window_based_clumping.py ([`1e50b57`](https://github.com/opentargets/genetics_etl_python/commit/1e50b57ae382143fdacc63df8d8214afc100ce01))

* Merge pull request #98 from opentargets/do_fixavatars

fix: stacking avatars in docs ([`877194b`](https://github.com/opentargets/genetics_etl_python/commit/877194b63f6054635fccdc3a89f5694eeadeff57))

* Merge pull request #95 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`8840ad5`](https://github.com/opentargets/genetics_etl_python/commit/8840ad5274123348d16412797ed039d15b046299))

* Merge pull request #96 from opentargets/tskir-node-troubleshooting

Thanks, @tskir. This will hopefully save some time for someone in the future. ([`b900e8d`](https://github.com/opentargets/genetics_etl_python/commit/b900e8df7533b6fd96db204ac9ef806c2b5e477c))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/mirrors-mypy: v1.3.0 → v1.4.1](https://github.com/pre-commit/mirrors-mypy/compare/v1.3.0...v1.4.1) ([`65cbe68`](https://github.com/opentargets/genetics_etl_python/commit/65cbe6821133856307405bbfc062e587ca103ac3))

* Merge pull request #88 from opentargets/2955-tskir-finngen-r9

Migrate to FinnGen R9 ([`d3f2e1b`](https://github.com/opentargets/genetics_etl_python/commit/d3f2e1b492ed8dcf402afbb0ead11a36c8cbadfa))

* Merge pull request #92 from opentargets/il-gwas-index-fix

Fix GWASCatalog data issues ([`594f738`](https://github.com/opentargets/genetics_etl_python/commit/594f7380be588488cbb3f29c1a187c27fc81f1dc))

* Merge pull request #86 from opentargets/hn-ukbb-ingest

UKBiobank ingestion ([`ef5afb0`](https://github.com/opentargets/genetics_etl_python/commit/ef5afb085b94b0894a8dcddc6dc33324b1fd9d40))

* Merge pull request #93 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`f816ece`](https://github.com/opentargets/genetics_etl_python/commit/f816ece41c55f96eb9781b1bcdb70b9e7586297a))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/yesqa: v1.4.0 → v1.5.0](https://github.com/asottile/yesqa/compare/v1.4.0...v1.5.0) ([`e37d247`](https://github.com/opentargets/genetics_etl_python/commit/e37d247124f1104aa54741ea9afce1126a432ad6))

* Merge pull request #91 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`f55fec9`](https://github.com/opentargets/genetics_etl_python/commit/f55fec936a02010bf2739761a806cec20c38d669))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.3 → v2.1.5](https://github.com/hadialqattan/pycln/compare/v2.1.3...v2.1.5)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/pre-commit/mirrors-mypy: v1.2.0 → v1.3.0](https://github.com/pre-commit/mirrors-mypy/compare/v1.2.0...v1.3.0) ([`c83c992`](https://github.com/opentargets/genetics_etl_python/commit/c83c9924c5ee5bcfdf4411aa378db20ce47d0911))

* Merge pull request #90 from opentargets/buniello-patch-4

Update roadmap.md ([`3afcc35`](https://github.com/opentargets/genetics_etl_python/commit/3afcc35a23c6b40774e49a29de906d80f2ff0831))

* Merge pull request #89 from opentargets/buniello-patch-3

Update roadmap.md ([`66ba355`](https://github.com/opentargets/genetics_etl_python/commit/66ba355bb31510a4e1b4d7c54a4e420ccafda19c))

* Update roadmap.md ([`6e3d6ad`](https://github.com/opentargets/genetics_etl_python/commit/6e3d6ad48156dabdb867d5a65910ae665ff6b22b))

* Update roadmap.md ([`29d3132`](https://github.com/opentargets/genetics_etl_python/commit/29d31328abf55bdb9284c4054550b783d16cdeb3))

* Merge pull request #87 from opentargets/tskir-contributing

Create a contributing checklist for the genetics repository ([`117945b`](https://github.com/opentargets/genetics_etl_python/commit/117945bf83502570217ef444e7bc998a1154e55b))

* Merge pull request #79 from opentargets/do_gcp_image_2_1

This PR&#39;s main feature is upgrading the reference [GCP images to 2.1](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.1). As a consequence, Spark is updated to 3.3.0 and Python to 3.10.

 *BREAKING CHANGE*: This will require updating everyone&#39;s poetry environments. You might need to run `make setup-dev` again on your local machine. ([`579e991`](https://github.com/opentargets/genetics_etl_python/commit/579e991fdd15690bb9f1fec1f10a76fe4ee79798))

* Merge pull request #83 from opentargets/il-study-locus

Debugging GWAS Catalog pipeline ([`c09c1f4`](https://github.com/opentargets/genetics_etl_python/commit/c09c1f4cd5e38f4ec5ef2cdd732913824cb28d7f))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`427535a`](https://github.com/opentargets/genetics_etl_python/commit/427535afb6fb6d86595a14a12afea56531c6410a))

* Merge pull request #85 from opentargets/buniello-patch-2

Update roadmap.md ([`ea7efcb`](https://github.com/opentargets/genetics_etl_python/commit/ea7efcb3aab96799826362253c97c110ce55a8f8))

* Merge pull request #84 from opentargets/buniello-patch-1

docs: update documentation ([`30000e7`](https://github.com/opentargets/genetics_etl_python/commit/30000e755de491b6027c4dde98a8aedf7e6168b4))

* Update roadmap.md ([`c89f768`](https://github.com/opentargets/genetics_etl_python/commit/c89f768d9b9c154b272aa5e9083fcd7f444b4e10))

* Merge branch &#39;do_gcp_image_2_1&#39; into il-study-locus ([`b4020b8`](https://github.com/opentargets/genetics_etl_python/commit/b4020b83b4e763411bffea691f997468f3802019))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`ef18bd1`](https://github.com/opentargets/genetics_etl_python/commit/ef18bd1f7f07a5e73e9062ae44b8e6e050f11ef6))

* Merge pull request #82 from opentargets/2946-tskir-versioning

Implement version aware code deployment ([`f67387e`](https://github.com/opentargets/genetics_etl_python/commit/f67387e614a3a9f527815f6fe2be343d786595c8))

* Fix numbering in README.md ([`5c7bbbc`](https://github.com/opentargets/genetics_etl_python/commit/5c7bbbc31c3c026853223f27249fec57397c1233))

* Merge pull request #80 from opentargets/do-finngen-step-docfix

docs: missing docs for finngen step added ([`7cfb0ca`](https://github.com/opentargets/genetics_etl_python/commit/7cfb0ca90ce9d00448fc263ab29cb2ca45c458d7))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`85aaeb0`](https://github.com/opentargets/genetics_etl_python/commit/85aaeb0ffb43fe242a1869b4bb486ea888fbd185))

* Merge pull request #59 from opentargets/2771-tskir-finngen

FinnGen ingestion ([`9afe9ee`](https://github.com/opentargets/genetics_etl_python/commit/9afe9ee656db51b1c82822e175ec2cac8cecbf39))

* Merge pull request #77 from opentargets/il-credset

Redefinition of credible set annotation ([`d5a51ab`](https://github.com/opentargets/genetics_etl_python/commit/d5a51ab2e45c1f8415d9ac98e2cdb7fc83395a8b))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-credset ([`ccf6ecf`](https://github.com/opentargets/genetics_etl_python/commit/ccf6ecf8f21deca1dd43d25703e36d8461907627))

* Merge pull request #76 from opentargets/il-nested-validation

Improved validation of nested structures ([`bff5c0d`](https://github.com/opentargets/genetics_etl_python/commit/bff5c0d0c2124e7f89ad132d13f375fb891c5bea))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`a0bfd8c`](https://github.com/opentargets/genetics_etl_python/commit/a0bfd8c0338808742d43e4bb83427bf06906115e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`9ab1c58`](https://github.com/opentargets/genetics_etl_python/commit/9ab1c58078212348f7973457ca8b13d12e4622eb))

* Merge pull request #73 from opentargets/do_va_slim

Do va slim ([`8e095fe`](https://github.com/opentargets/genetics_etl_python/commit/8e095fe1e8cd53146322fcc763ddf1a67ed90235))

* Merge pull request #75 from opentargets/do_test_session_streamline

Test session streamline ([`2a7e1bc`](https://github.com/opentargets/genetics_etl_python/commit/2a7e1bca9553315f63281248c0247634ec92dae6))

* Merge pull request #72 from opentargets/do_test_credset

test: is in credset function ([`b81327a`](https://github.com/opentargets/genetics_etl_python/commit/b81327a3371f8161caaea2f2e9eebdc49fd3d022))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`5bc4b38`](https://github.com/opentargets/genetics_etl_python/commit/5bc4b38673748abd6b9f3e18e274c30e355efa13))

* Merge pull request #71 from opentargets/il-schemas

Validate schemas overlooking the nullability fields ([`660e5d8`](https://github.com/opentargets/genetics_etl_python/commit/660e5d8a352d36b5576b0a877cc053bfcf343ed9))

* Merge branch &#39;il-schemas&#39; of https://github.com/opentargets/genetics_etl_python into il-schemas ([`57310d7`](https://github.com/opentargets/genetics_etl_python/commit/57310d754a32ebda2ed5a37bdab18ffaab76676a))

* Revert &#34;build: update mypy to 1.2.0&#34;

This reverts commit 0d5c6e9e9f6592f4b005e5feef43c550c3535f2d. ([`34f1024`](https://github.com/opentargets/genetics_etl_python/commit/34f1024a4739038e715878070c43453cead03c3d))

* Revert &#34;fix: order ld index by idx and unpersist data&#34;

This reverts commit 5a8e03a7030c88b6b7c39f2ed20123598ff76dea. ([`aba6bd8`](https://github.com/opentargets/genetics_etl_python/commit/aba6bd8e82f8a2f0469b69a37698540b96bdee42))

* Revert &#34;fix: correct attribute names for ld indices&#34;

This reverts commit b1d56c48cac25ca11c587527c32df5ddbeddc004. ([`1f6e389`](https://github.com/opentargets/genetics_etl_python/commit/1f6e389e626c741fce47520bfa9be3bf4482c950))

* Revert &#34;style: rename ld indices location to directory and no extension&#34;

This reverts commit 512c842e7e5ac744820b017fc5640b8bf009ef24. ([`eedf763`](https://github.com/opentargets/genetics_etl_python/commit/eedf76309d45230d82e1e614a99e37504aa32c87))

* Revert &#34;fix: join `_variant_coordinates_in_ldindex` on `variantId`&#34;

This reverts commit 829ef6b982cc7c83c9fa95928140d23987cd67d3. ([`5f9c7c1`](https://github.com/opentargets/genetics_etl_python/commit/5f9c7c1b47cb5c77c2edde75e211e33f24212b12))

* Revert &#34;fix: move rsId and concordance check outside the filter function&#34;

This reverts commit c57919d2d28e0cea8f4eb67f2b2fb2e30d273945. ([`5dda33f`](https://github.com/opentargets/genetics_etl_python/commit/5dda33f80213e3ed15a3c7a9038422c5345d4ab6))

* Revert &#34;fix: update configure and gitignore&#34;

This reverts commit 6cc9af18a6061083abbd403b35034f56233dd226. ([`4051d0d`](https://github.com/opentargets/genetics_etl_python/commit/4051d0dbe733ea0a0d759e469474cea4dc2e5f82))

* Merge branch &#39;il-schemas&#39; of https://github.com/opentargets/genetics_etl_python into il-schemas ([`079ee76`](https://github.com/opentargets/genetics_etl_python/commit/079ee76589d7c02318fd9a1789b8085dd42ecbfd))

* Merge branch &#39;main&#39; into il-schemas ([`27eeb03`](https://github.com/opentargets/genetics_etl_python/commit/27eeb03513401a24e04b5e3f6f9902e6bbbd5597))

* Merge pull request #66 from opentargets/do_hydra

Do hydra ([`52152ff`](https://github.com/opentargets/genetics_etl_python/commit/52152fff02e572672595c68016932bf37036c032))

* Merge pull request #68 from opentargets/ds_summary_stats_ingest

Summary statistics dataset ([`499a780`](https://github.com/opentargets/genetics_etl_python/commit/499a780e83dc07eacd3bb48ba8e5d6c68d8f4b96))

* Update gcp.yaml

Wrong interpolation ([`bb377d5`](https://github.com/opentargets/genetics_etl_python/commit/bb377d5b2133f7f1033a73aa1cf17759c76f307c))

* Merge branch &#39;do_hydra&#39; into ds_summary_stats_ingest ([`b5fbcb0`](https://github.com/opentargets/genetics_etl_python/commit/b5fbcb08aa8acc6d9ffb28c00a94e369ca7e6422))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into ds_summary_stats_ingest ([`172fa25`](https://github.com/opentargets/genetics_etl_python/commit/172fa2563e58892da1a847521fffbd83cd084185))

* Merge pull request #64 from opentargets/ds_clumping_refactor

Clumping refactor ([`2500f7f`](https://github.com/opentargets/genetics_etl_python/commit/2500f7fa84ce27418418d281a7b40ea00b4000c7))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`d232e4e`](https://github.com/opentargets/genetics_etl_python/commit/d232e4ebdc63bfed6249bc4759ede7c2649537a2))

* Update src/etl/gwas_ingest/pics.py ([`7cf8f42`](https://github.com/opentargets/genetics_etl_python/commit/7cf8f4261aa78ca723b3d7032a1e3fe649b0d27d))

* Update src/etl/gwas_ingest/clumping.py ([`2772831`](https://github.com/opentargets/genetics_etl_python/commit/277283103f798c02a7660d10f432505aa53344bd))

* Merge pull request #46 from opentargets/il-v2g-distance

Extract V2G evidence from a variant distance to a gene TSS ([`ce4fa35`](https://github.com/opentargets/genetics_etl_python/commit/ce4fa3598a9902e0c8883afae311407fee0458d6))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-v2g-distance ([`10b1d7c`](https://github.com/opentargets/genetics_etl_python/commit/10b1d7cf17652321e3187c76e90381a3c43819f4))

* [pre-commit.ci] pre-commit autoupdate (#63)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pycqa/isort: 5.11.4 → 5.12.0](https://github.com/pycqa/isort/compare/5.11.4...5.12.0)
- [github.com/psf/black: 22.12.0 → 23.1.0](https://github.com/psf/black/compare/22.12.0...23.1.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/pycqa/pydocstyle: 6.2.3 → 6.3.0](https://github.com/pycqa/pydocstyle/compare/6.2.3...6.3.0)

* fix: reverting flake8 version

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

---------

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`b8b5bb1`](https://github.com/opentargets/genetics_etl_python/commit/b8b5bb1fb97c4b6cfeddd2fe06e3147845dca20a))

* [pre-commit.ci] pre-commit autoupdate (#56)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pygrep-hooks: v1.9.0 → v1.10.0](https://github.com/pre-commit/pygrep-hooks/compare/v1.9.0...v1.10.0)
- [github.com/asottile/pyupgrade: v3.3.0 → v3.3.1](https://github.com/asottile/pyupgrade/compare/v3.3.0...v3.3.1)
- [github.com/hadialqattan/pycln: v2.1.2 → v2.1.3](https://github.com/hadialqattan/pycln/compare/v2.1.2...v2.1.3)
- [github.com/pycqa/isort: 5.10.1 → 5.11.4](https://github.com/pycqa/isort/compare/5.10.1...5.11.4)
- [github.com/psf/black: 22.10.0 → 22.12.0](https://github.com/psf/black/compare/22.10.0...22.12.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/alessandrojcm/commitlint-pre-commit-hook: v9.3.0 → v9.4.0](https://github.com/alessandrojcm/commitlint-pre-commit-hook/compare/v9.3.0...v9.4.0)
- [github.com/pycqa/pydocstyle: 6.1.1 → 6.2.3](https://github.com/pycqa/pydocstyle/compare/6.1.1...6.2.3)

* regressing flake8

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`e0f02ea`](https://github.com/opentargets/genetics_etl_python/commit/e0f02ea7237d1f047ae1d7b019cf854d19889825))

* Do pics (#57)

* feat: experimental LD using hail

* feat: handling of the lower triangle

* docs: more comments explaining what\&#39;s going on

* feat: non-working implementation of ld information

* feat: ld information based on precomputed index

* fix: no longer neccesary

* feat: missing ld.py added

* docs: intervals functions examples

* fix: typo

* refactor: larger scale update in the GWAS Catalog data ingestion

* feat: precommit updated

* feat: first pics implementation derived from gnomAD LD information

* feat: modularise iterating over populations

* feat: finalizing GWAS Catalog ingestion steps

* fix: test fixed, due to changes in logic

* feat: ignoring .coverage file

* feat: modularised pics method

* feat: integrating pics

* chore: smoothing out bits

* feat: cleanup of the pics logic

No select statements, more concise functions and carrying over all required information

* fix: slight updates

* feat: map gnomad positions to ensembl positions LD

* fix: use cumsum instead of postprob

* feat: update studies schemas

* feat: working on integrating ingestion with pics

* feat: support for hail doctests

* test: _query_block_matrix example

* feat: ignore hail logs for testing

* feat: ingore TYPE_CHECKING blocks from testing

* feat: pics benchmark notebook (co-authored by Irene)

* feat: new finding on r &gt; 1

* docs: Explanation about the coordinate shift

* test: several pics doctests

* test: fixed test for log neg pval

* style: remove variables only used for return

* feat: parametrise liftover chain

* refactor: consolidating some code

* feat: finishing pics

* chore: adding tests to effect harmonization

* chore: adding more tests

* feat: benchmarking new dataset

* fix: resolving minor bugs around pics

* Apply suggestions from code review

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;

* fix: applying review comments

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

* fix: addressing some more review comments

* fix: update GnomAD join

* fix: bug sorted out in pics filterin

* feat: abstracting QC flagging

* feat: solving clumping

* fix: clumping is now complete

Co-authored-by: David &lt;ochoa@ebi.ac.uk&gt;
Co-authored-by: David Ochoa &lt;dogcaesar@gmail.com&gt;
Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;
Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt; ([`7e42f94`](https://github.com/opentargets/genetics_etl_python/commit/7e42f947fbbd1c030f166459d082ae7fa70c1592))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`e94adfd`](https://github.com/opentargets/genetics_etl_python/commit/e94adfd9a306580f85e80c6cea0357383393f9f3))

* Merge branch &#39;main&#39; into il-v2g-distance ([`363288e`](https://github.com/opentargets/genetics_etl_python/commit/363288edfec067d5137b61d5bc9500b465340a74))

* Integrate eCAVIAR colocalization (#58)

* refactor: extract method to find overlapping peaks to `find_gwas_vs_all_overlapping_peaks`

* test: add tests for `find_gwas_vs_all_overlapping_peaks`

* fix: add gene_id to the metadata col

* feat: implement working ecaviar

* test: add test for `ecaviar_colocalisation`

* test: added test for _extract_credible_sets

* fix: filter only variants in the 95 credible set

* docs: update coloc docs

* feat: add coloc schema and validation step ([`8c5c532`](https://github.com/opentargets/genetics_etl_python/commit/8c5c532cfc4afb33e536a5acfc663a7a630d6985))

* Delete .DS_Store ([`010447b`](https://github.com/opentargets/genetics_etl_python/commit/010447b9f82c1e390af3d16b1050cd9380dfc5ab))

* Merge pull request #54 from opentargets/il-update-coloc

Update the coloc pipeline to the newer credible sets and study datasets ([`dc0f566`](https://github.com/opentargets/genetics_etl_python/commit/dc0f56682e1b1b658f5ca2b22c0fbc6198646186))

* Merge pull request #55 from opentargets/il-python-update

Update package to Python 3.8.15 ([`ff6cdc3`](https://github.com/opentargets/genetics_etl_python/commit/ff6cdc3032823788b603d18fce60fdef4877daee))

* Merge pull request #53 from opentargets/il-refactor-coloc

Reorganisation of the coloc logic ([`2b7a338`](https://github.com/opentargets/genetics_etl_python/commit/2b7a338f3143c691b4a136c631b78f21a5cbeea1))

* Merge pull request #52 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`d1079a5`](https://github.com/opentargets/genetics_etl_python/commit/d1079a5abe250d378f8e73dbd489697bb9b617b6))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pre-commit-hooks: v4.3.0 → v4.4.0](https://github.com/pre-commit/pre-commit-hooks/compare/v4.3.0...v4.4.0)
- [github.com/asottile/pyupgrade: v3.2.2 → v3.3.0](https://github.com/asottile/pyupgrade/compare/v3.2.2...v3.3.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0) ([`e1dc17a`](https://github.com/opentargets/genetics_etl_python/commit/e1dc17aad1c403533b77a14ba4236c01a8f4a2f8))

* Merge pull request #41 from opentargets/il-vep

Extract V2G evidence from functional predictions ([`57b0adf`](https://github.com/opentargets/genetics_etl_python/commit/57b0adf9e68e10f4256826ea00f3b7c6de7dc915))

* Merge pull request #51 from opentargets/do_awesome_pages

feat: ignore docstrings from private functions in documentation ([`16adafc`](https://github.com/opentargets/genetics_etl_python/commit/16adafccfca301d250216aa296f47573ddfb7371))

* Merge branch &#39;main&#39; into il-vep ([`21104f3`](https://github.com/opentargets/genetics_etl_python/commit/21104f386597bad1a011166f12ea93a9c1ba16f2))

* Merge branch &#39;main&#39; into il-vep ([`827e244`](https://github.com/opentargets/genetics_etl_python/commit/827e2448a7fc2482d3cf772d6604d097d65c5fb4))

* Merge pull request #50 from opentargets/do_awesome_pages

feat: plugin not to automatically handle required pages ([`7ebe630`](https://github.com/opentargets/genetics_etl_python/commit/7ebe6303cc2de4ce0327a167aab452114feefad1))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pre-commit-hooks: v4.3.0 → v4.4.0](https://github.com/pre-commit/pre-commit-hooks/compare/v4.3.0...v4.4.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0) ([`1a4082e`](https://github.com/opentargets/genetics_etl_python/commit/1a4082e52621cdcb9acab11af61efacc2d0450b9))

* Add `gnomad3VariantId` to variant annotation schema (#48)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

* feat: include gnomad3VariantId in the variant schema

* feat: include gnomad3VariantId in the variant schema

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`621dfe2`](https://github.com/opentargets/genetics_etl_python/commit/621dfe28da7bf8e8beb8adf0e0f3675eef0f22d7))

* [pre-commit.ci] pre-commit autoupdate (#42)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v3.2.0 → v3.2.2](https://github.com/asottile/pyupgrade/compare/v3.2.0...v3.2.2)
- [github.com/hadialqattan/pycln: v2.1.1 → v2.1.2](https://github.com/hadialqattan/pycln/compare/v2.1.1...v2.1.2)
- [github.com/pre-commit/mirrors-mypy: v0.982 → v0.991](https://github.com/pre-commit/mirrors-mypy/compare/v0.982...v0.991)

* chore: separate script for dependency installation (#47)

* Minor improvements to the variant datasets generation (#45)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt;

The script runs and generated dataset is agreement with expectation.

* fix: adding __init__.py

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt;
Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`7fad89c`](https://github.com/opentargets/genetics_etl_python/commit/7fad89cd3eb326f48943b3fa076db214a8cd8a79))

* Minor improvements to the variant datasets generation (#45)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt;

The script runs and generated dataset is agreement with expectation. ([`95b5a88`](https://github.com/opentargets/genetics_etl_python/commit/95b5a88862235729174fa3ce62ee4c83f2993ac6))

* Merge branch &#39;il-vep&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`b029e3b`](https://github.com/opentargets/genetics_etl_python/commit/b029e3bf6d3f5b86e0292efd62b5686e6d784aef))

* revert commit to ebe6717 state ([`5c92546`](https://github.com/opentargets/genetics_etl_python/commit/5c92546ce3615a959c73cf7d1c1f23e933184c1f))

* Revert &#34;fix: fix tests data definition&#34;

This reverts commit a382ffaf3876feae152e39cfd010f4062cc61d87. ([`a4a390b`](https://github.com/opentargets/genetics_etl_python/commit/a4a390bda6712573bb04b3dc1504580b27b7f062))

* Merge pull request #40 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`86578da`](https://github.com/opentargets/genetics_etl_python/commit/86578da17fdd58c43d47594fd506fdccdf1eaf4a))

* Merge pull request #39 from opentargets/do_codecov

codecov coverage target ([`17b429c`](https://github.com/opentargets/genetics_etl_python/commit/17b429cc99a7551d55445c48d94ca2c56827725c))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v3.1.0 → v3.2.0](https://github.com/asottile/pyupgrade/compare/v3.1.0...v3.2.0) ([`e7a3840`](https://github.com/opentargets/genetics_etl_python/commit/e7a38404a71e760b2397f3d724493804797dbab0))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`194347c`](https://github.com/opentargets/genetics_etl_python/commit/194347c798422762d72fda2ab404c8e224b222e6))

* Add variant index generation module (#33)

* feat: coalesce variant_idx
* feat: remove utils to parse vep and annotate closest genes
* feat: add output validation
* feat: extract `mostSevereConsequence` out of vep


* test: added tests for spark helpers

* docs: remove reference to closest gene
* docs: added missing docs

Co-authored-by: David Ochoa &lt;dogcaesar@gmail.com&gt; ([`bc42bb8`](https://github.com/opentargets/genetics_etl_python/commit/bc42bb817a5a1bb36263e44aa89cc9443138f53a))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`b58f1dc`](https://github.com/opentargets/genetics_etl_python/commit/b58f1dc05c55c85fc3c7173e6adea5dc00d4a785))

* Merge pull request #38 from opentargets/DSuveges-patch-1

Adding variant pipeline docs ([`23e2228`](https://github.com/opentargets/genetics_etl_python/commit/23e2228386c21e482190028b44357dd20805a049))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`ff494bb`](https://github.com/opentargets/genetics_etl_python/commit/ff494bb0fc8dbc62fcd3663313d54732e39a411a))

* Adding variant pipeline docs

the __init__ script was missing! ([`fc8e6aa`](https://github.com/opentargets/genetics_etl_python/commit/fc8e6aa2eb0dbb0e25cc6e8d5727fc97c93f8ea9))

* pvalue to zscore refactor + refactored function to operate with column ([`31a1556`](https://github.com/opentargets/genetics_etl_python/commit/31a1556ec18f95f30bc6129385d388845ef5a0b0))

* Do_removepandas (#37)

remove pandas dependency + removing old docs ([`5edb5df`](https://github.com/opentargets/genetics_etl_python/commit/5edb5dff0903b5817d110113131e562273fc11a3))

* Modification of the variant annotation schema (#24) ([`6edcdea`](https://github.com/opentargets/genetics_etl_python/commit/6edcdea0a7a84e2f2ae4ba55118747e2e9994bb0))

* Fixing bug in pvalue -&gt; z-score calculation (#34)

* Fixing bug in pvalue -&gt; z-score calculation
* Returning to scipy.stats functions
* Updating examples ([`8c77cd1`](https://github.com/opentargets/genetics_etl_python/commit/8c77cd1617854d493031cfc1997fcdfceb53bc22))

* Merge pull request #32 from opentargets/do_ipython_support

feat: ipython support ([`17c61eb`](https://github.com/opentargets/genetics_etl_python/commit/17c61eb43f59d57ad9742c3365649aa9dabd1027))

* Merge pull request #31 from opentargets/do_docs

feat: token added to codecov ([`bef4e86`](https://github.com/opentargets/genetics_etl_python/commit/bef4e86a3cdddbc02ce73677a3a72fd26d95830a))

* Merge pull request #30 from opentargets/do_docs

feat: codecov integration ([`d169917`](https://github.com/opentargets/genetics_etl_python/commit/d16991767c6c12b6e4f06b7853ac042d6071b0c4))

* Merge pull request #29 from opentargets/do_docs

Support for doctests implemented ([`f9d42d2`](https://github.com/opentargets/genetics_etl_python/commit/f9d42d2b1b81afa5e2f6f420371383504937abb4))

* Merge branch &#39;main&#39; into do_docs ([`0fe439c`](https://github.com/opentargets/genetics_etl_python/commit/0fe439ca8fcd1366db3cb0920ef33bf7f43e7c7d))

* Merge pull request #28 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`fd995f8`](https://github.com/opentargets/genetics_etl_python/commit/fd995f87a7043ab866ab8172f8bc59886fd6de20))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.38.2 → v3.1.0](https://github.com/asottile/pyupgrade/compare/v2.38.2...v3.1.0)
- [github.com/psf/black: 22.8.0 → 22.10.0](https://github.com/psf/black/compare/22.8.0...22.10.0)
- [github.com/pre-commit/mirrors-mypy: v0.981 → v0.982](https://github.com/pre-commit/mirrors-mypy/compare/v0.981...v0.982) ([`56103b7`](https://github.com/opentargets/genetics_etl_python/commit/56103b75f5980421aa0f983352be8ef61d7145eb))

* Merge pull request #26 from opentargets/gwas_ingest_improve

Ingesting GWAS Catalog study table ([`4919545`](https://github.com/opentargets/genetics_etl_python/commit/4919545cf6fde1b52b8c57d3b28e3debcb2616dc))

* Merge remote-tracking branch &#39;refs/remotes/origin/gwas_ingest_improve&#39; into gwas_ingest_improve ([`5101a6d`](https://github.com/opentargets/genetics_etl_python/commit/5101a6d7d0017816900ec5d01bfd9b882c87e77a))

* Merge branch &#39;main&#39; into gwas_ingest_improve ([`40d4a03`](https://github.com/opentargets/genetics_etl_python/commit/40d4a033235e032087b17556a1bc68b9219b6fac))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`ac9b7a8`](https://github.com/opentargets/genetics_etl_python/commit/ac9b7a8914925d3c9ec8fc6d3707215ab1e37952))

* Merge pull request #27 from opentargets/do_docs

force merging to checkout if gh action works ([`4f970bb`](https://github.com/opentargets/genetics_etl_python/commit/4f970bb6c2af74216c5f8f9d80532cd4385620f6))

* Merge pull request #25 from opentargets/do_docs

feat: docs support ([`c16f27b`](https://github.com/opentargets/genetics_etl_python/commit/c16f27b4dbbc454592d774833355e786fdfdb7c8))

* Merge branch &#39;main&#39; into do_docs ([`aa614a7`](https://github.com/opentargets/genetics_etl_python/commit/aa614a77726d6920337bf08e37d68a6d616095a9))

* Merge pull request #23 from opentargets/do_schemadoc

docs: schema functions description ([`4d881ab`](https://github.com/opentargets/genetics_etl_python/commit/4d881ab8c478215c20b399b6cda399d1616f066c))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`decf535`](https://github.com/opentargets/genetics_etl_python/commit/decf535a4e6088639c3ff92dbbf4ad7fbca5e18f))

* Merge pull request #15 from opentargets/gwas_ingest_improve

Adding more stuff to GWAS Catalog ingest ([`9cb71cc`](https://github.com/opentargets/genetics_etl_python/commit/9cb71ccfb50d1970635b918218bf2d5d119cc44b))

* Merge branch &#39;main&#39; into gwas_ingest_improve ([`4d0f089`](https://github.com/opentargets/genetics_etl_python/commit/4d0f0896191b12d31dbc477cfb846d7abd4ba9d1))

* Update documentation/GWAS_ingestion.md

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`8b46024`](https://github.com/opentargets/genetics_etl_python/commit/8b460240b3b237fd20a7868471534fb7ebe2b07d))

* Merge pull request #22 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`1716782`](https://github.com/opentargets/genetics_etl_python/commit/171678294d9dcceb198e4821742b0808b03b9256))

* Merge pull request #21 from opentargets/do_readschema

Merging to prevent distractions. We can fix things as we go ([`654a702`](https://github.com/opentargets/genetics_etl_python/commit/654a702d19b6f9209ae378c07f95ee184ad4bd08))

* Apply suggestions from code review

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`fa863ab`](https://github.com/opentargets/genetics_etl_python/commit/fa863ab80a78f9cba51b94c440a975ff0f70b891))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.38.0 → v2.38.2](https://github.com/asottile/pyupgrade/compare/v2.38.0...v2.38.2) ([`66e504a`](https://github.com/opentargets/genetics_etl_python/commit/66e504aeb29d6e5dee8a236b7575cb11464f4527))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`0f39ed6`](https://github.com/opentargets/genetics_etl_python/commit/0f39ed697d6eed2eb813da6696709a4b9cb3abea))

* Merge branch &#39;main&#39; into do_readschema ([`b4ed33d`](https://github.com/opentargets/genetics_etl_python/commit/b4ed33dea6ca1485fd94d41064c37e6bc7419308))

* Merge pull request #20 from opentargets/do_intervals_fromtarget

Intervals from target ([`cd18a05`](https://github.com/opentargets/genetics_etl_python/commit/cd18a05c5ae40a9856e89a51c58f0fc5130a12b4))

* Merge branch &#39;do_intervals_fromtarget&#39; of https://github.com/opentargets/genetics_etl_python into do_intervals_fromtarget ([`a569fa5`](https://github.com/opentargets/genetics_etl_python/commit/a569fa5cf564a54ae4233967d4db510712f75821))

* Merge pull request #18 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`991f78e`](https://github.com/opentargets/genetics_etl_python/commit/991f78e473ae9cac84283d15116f83f67062e9b8))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`f684c9f`](https://github.com/opentargets/genetics_etl_python/commit/f684c9ffe1c9e7d4b6c94bc657fd576f5059ac81))

* Update launch.json
Removing comment lines from the `launch.json` file. ([`01d7fee`](https://github.com/opentargets/genetics_etl_python/commit/01d7fee9903930418e5aaf9519da8020fa213763))

* Update launch.json

Removing comment lines from the `launch.json` file. ([`7fbd510`](https://github.com/opentargets/genetics_etl_python/commit/7fbd510591480ded149a3e25384525f7e08d7f55))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.37.3 → v2.38.0](https://github.com/asottile/pyupgrade/compare/v2.37.3...v2.38.0) ([`3e60d53`](https://github.com/opentargets/genetics_etl_python/commit/3e60d532cf29b90f0561c4877e4e062818ce04d8))

* Merge pull request #17 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`625df4d`](https://github.com/opentargets/genetics_etl_python/commit/625df4dac3884657f1360fb37ee3042f632a7ea1))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`ec08107`](https://github.com/opentargets/genetics_etl_python/commit/ec081074c5df3bc5b9493842e270845e5746f07a))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/alessandrojcm/commitlint-pre-commit-hook: v9.0.0 → v9.1.0](https://github.com/alessandrojcm/commitlint-pre-commit-hook/compare/v9.0.0...v9.1.0) ([`2fa250b`](https://github.com/opentargets/genetics_etl_python/commit/2fa250bd0b4673e21c225df3e91bdf907cac5f75))

* Merge pull request #14 from opentargets/ds_gwas_ingest ([`468ad1c`](https://github.com/opentargets/genetics_etl_python/commit/468ad1c3bce3b50a9a88a83bba6d64b4265ba5c4))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`3783db8`](https://github.com/opentargets/genetics_etl_python/commit/3783db81986ebe64fc4a58936ce09179b3ac80ee))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`e9702a7`](https://github.com/opentargets/genetics_etl_python/commit/e9702a73864bf8226583b085cb9732fa20853e3a))

* Merge pull request #13 from opentargets/variant_annot

Variant annotation included with new build system ([`4132dcb`](https://github.com/opentargets/genetics_etl_python/commit/4132dcbe308ef676a9a1d34e655afd4923088fc4))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into variant_annot ([`01e4ae4`](https://github.com/opentargets/genetics_etl_python/commit/01e4ae43e763ae3b963e5e097bfd7e699a28be7b))

* Merge pull request #12 from opentargets/do_config

feat: modular hydra configs and other enhancements ([`764efa0`](https://github.com/opentargets/genetics_etl_python/commit/764efa0d5dc0ac84ecb425408045b44d1a05d96c))

* Merge pull request #10 from opentargets/ds_intervals

The main interval script is added to the repo ([`ba08567`](https://github.com/opentargets/genetics_etl_python/commit/ba08567a6500f55ecc47cb36b86636b690d09aab))

* Merge pull request #9 from opentargets/ds_intervals

feat: Adding parsers for intervals dataset. ([`483edf7`](https://github.com/opentargets/genetics_etl_python/commit/483edf72c2bc6f20ef76ec0088a91e30b45cfee6))

* Echo removed. ([`972e6bc`](https://github.com/opentargets/genetics_etl_python/commit/972e6bc3feef0793df995d2e2a92e71e1f643ceb))

* Black sç ([`e30e036`](https://github.com/opentargets/genetics_etl_python/commit/e30e03692290d28dcbc61293a8ccc4383ffabb96))

* Merge pull request #8 from opentargets/do_setupenvironment

Do setupenvironment ([`8f59dc8`](https://github.com/opentargets/genetics_etl_python/commit/8f59dc813338666a44a1dfbdfbbc532a5bd572d8))

* Instructions to setup dev environment ([`804ae79`](https://github.com/opentargets/genetics_etl_python/commit/804ae79bf924acbafe344d01cc9010a37ab3d77c))

* black was missing from dependencies ([`1a2a800`](https://github.com/opentargets/genetics_etl_python/commit/1a2a80019bee2343c32b918e884bcf7b03ad9bbd))

* Merge pull request #7 from opentargets/do_project_rename

generalising project ([`d54414b`](https://github.com/opentargets/genetics_etl_python/commit/d54414b0ebbebda330d53856599cdc130ed8355c))

* generalising project ([`e765611`](https://github.com/opentargets/genetics_etl_python/commit/e7656111b67051b203b7ef905b651c56d2322a3e))

* Merge pull request #4 from opentargets/do_poetry

Managing build and dependencies using poetry + makefile ([`3835fc8`](https://github.com/opentargets/genetics_etl_python/commit/3835fc8710bf5753babba70078f81fbd85e64aea))

* Action no longer runs ([`44f9c6d`](https://github.com/opentargets/genetics_etl_python/commit/44f9c6d134a3c14e26b056428803fb806ac22df9))

* Merge pull request #6 from opentargets/do_all_lints

pre-commit replaces GitHub actions for all code quality checks ([`2074a0c`](https://github.com/opentargets/genetics_etl_python/commit/2074a0c934e998714bf30dbe9363f9b11b0509a6))

* pre-commit including and code adjusted to new changes ([`80b4aa2`](https://github.com/opentargets/genetics_etl_python/commit/80b4aa212554b4b9029a13cc21ed191be1f091fb))

* Trying single action ([`d44ca2f`](https://github.com/opentargets/genetics_etl_python/commit/d44ca2f58f97634b9b11b3da16bec1d7fd90132e))

* poetry version added for caching purposes ([`991234f`](https://github.com/opentargets/genetics_etl_python/commit/991234fd6b1916f15287346bf36cbeb0ed717d59))

* more indentation problems ([`36e4560`](https://github.com/opentargets/genetics_etl_python/commit/36e4560d76c8ac95347cea67698ca7b28741e0a6))

* indentation problem ([`3f9fd30`](https://github.com/opentargets/genetics_etl_python/commit/3f9fd30544210e761c694580582f9057062a7fba))

* pylint workflow updated ([`45adf32`](https://github.com/opentargets/genetics_etl_python/commit/45adf325ca106bdbf8531237a40d1f6b57ae7796))

* unnecessary code ([`72d4dc4`](https://github.com/opentargets/genetics_etl_python/commit/72d4dc4dc19f1a4fcbe497c115286a39bc5ca755))

* changes on dependencies ([`74d6947`](https://github.com/opentargets/genetics_etl_python/commit/74d694717d41068df0a73b9d5140ca6898a91f25))

* spaces over tabs ([`f1d36c0`](https://github.com/opentargets/genetics_etl_python/commit/f1d36c0194cccc3903510b36b806505fe5d76714))

* Problem renaming variables ([`138508a`](https://github.com/opentargets/genetics_etl_python/commit/138508a8dd86769ccfa45c27d067294b0a35dcc4))

* Minimum python version downgraded ([`a5477b4`](https://github.com/opentargets/genetics_etl_python/commit/a5477b4078372191d6ae37969eef17839959786e))

* add init ([`035f45d`](https://github.com/opentargets/genetics_etl_python/commit/035f45d6d919d19c4d1e1a2a4f67d785c842c555))

* updating pylint test to work with poetry ([`6f17ea3`](https://github.com/opentargets/genetics_etl_python/commit/6f17ea30ffd843fe9aa25a0ef0222613baf2fe6c))

* More meaningful help ([`5c8e23e`](https://github.com/opentargets/genetics_etl_python/commit/5c8e23e9646151dcc01a049531db49bc679d99e7))

* more isort changes ([`fe46a77`](https://github.com/opentargets/genetics_etl_python/commit/fe46a7738aed135dacd3d4adb7a32361b16f0158))

* Changes to comply with isort ([`1155e96`](https://github.com/opentargets/genetics_etl_python/commit/1155e96373013643d14ddc8ec37e1fab73044032))

* Unnecessary comments ([`c94fcef`](https://github.com/opentargets/genetics_etl_python/commit/c94fceff13c67ea750e17e8f89bdde62abaa0eb4))

* unused import ([`53632c3`](https://github.com/opentargets/genetics_etl_python/commit/53632c3065f5803de40757daaff7749541899b99))

* build version works ([`be23652`](https://github.com/opentargets/genetics_etl_python/commit/be236526e19328ddb8700831eac5a3e684ba6981))

* unnecessary ([`3943ba8`](https://github.com/opentargets/genetics_etl_python/commit/3943ba88ccded27a20ec2af7a85734b5557d90b0))

* renamed config file ([`24f0167`](https://github.com/opentargets/genetics_etl_python/commit/24f01677125a5226f073d4ee3db787f4c2a37398))

* right project dependencies structure ([`eb63079`](https://github.com/opentargets/genetics_etl_python/commit/eb63079784f723c1be2735b94554013f879ec61b))

* updates to gitignore ([`f36b993`](https://github.com/opentargets/genetics_etl_python/commit/f36b9930e4632f684310c3fc04232a9d08c3e682))

* changes to gitignore ([`83e2854`](https://github.com/opentargets/genetics_etl_python/commit/83e2854145bcee3d6d98d3cfe5bd4ff9e1d7d215))

* Adding gitignore ([`8fddac8`](https://github.com/opentargets/genetics_etl_python/commit/8fddac8e291b6db4ddec9ed62dee2d927ce01992))

* pyspark as development dependency ([`e3cbe1d`](https://github.com/opentargets/genetics_etl_python/commit/e3cbe1d9be15d70e934ab4228418f06efc380e2f))

* No longer required with Poetry ([`895f3df`](https://github.com/opentargets/genetics_etl_python/commit/895f3df3d32c3c88c13feb294f159a42465475c8))

* Fixing imports based on new strucure ([`d3e21d3`](https://github.com/opentargets/genetics_etl_python/commit/d3e21d3b87571a36f9395b014d7031f97546a54e))

* Update pylint action to use poetry ([`4300678`](https://github.com/opentargets/genetics_etl_python/commit/43006780a114402bc29ad9b1eb41d05dacf707d9))

* Update dev dependencies ([`7404401`](https://github.com/opentargets/genetics_etl_python/commit/74044014b992cc820503e7f83b2685312d56f75d))

* New structure and poetry lock ([`89aaf46`](https://github.com/opentargets/genetics_etl_python/commit/89aaf46702e7bd08e8a6bf0e8ef51313ea58bf76))

* No longer necessary ([`2d318f0`](https://github.com/opentargets/genetics_etl_python/commit/2d318f03eb7d5773c28fc5142cb09f5377a53628))

* simplify name ([`94deb08`](https://github.com/opentargets/genetics_etl_python/commit/94deb0840c4532ebfb38200c367ab76e56bcf662))

* simplify name ([`ab8f63b`](https://github.com/opentargets/genetics_etl_python/commit/ab8f63b36b1489e4750a6d05ee42546b6e64a3ae))

* Too many badges ([`df0fdb2`](https://github.com/opentargets/genetics_etl_python/commit/df0fdb2e45d422b5144f8893f0b2df270d5e9bd2))

* Merge pull request #2 from opentargets/do_black_action

Do black action ([`fcd611a`](https://github.com/opentargets/genetics_etl_python/commit/fcd611aca54a296442cae54247c2f80b7e3cbe28))

* pylinting and badges ([`9cb1966`](https://github.com/opentargets/genetics_etl_python/commit/9cb196611474508ff763c2dd3b98b60768ee3251))

* missing docstrings ([`7132519`](https://github.com/opentargets/genetics_etl_python/commit/7132519b9f432e5cc679da31aa80934a4a703865))

* Improving dependencies ([`ab879a5`](https://github.com/opentargets/genetics_etl_python/commit/ab879a58e8156fb6b329279b4d50cb6bbd01356b))

* add requirements install to pylint ([`ab28c94`](https://github.com/opentargets/genetics_etl_python/commit/ab28c9466f702ccfed0ec6a618238e1832c935b6))

* Merge pull request #3 from opentargets:do_lintchanges

Do_lintchanges ([`556b0bb`](https://github.com/opentargets/genetics_etl_python/commit/556b0bbc9abf44e32f49750149c8a4bef5bbf1bd))

* lint compliance ([`1f0e584`](https://github.com/opentargets/genetics_etl_python/commit/1f0e5840a37ab24785debbe7703aaa6936d03362))

* adjusting lint ([`1b123f6`](https://github.com/opentargets/genetics_etl_python/commit/1b123f682e3d63eeacf6ac825354558d04888257))

* Black and pylint added ([`43f47ae`](https://github.com/opentargets/genetics_etl_python/commit/43f47aecf1494821a8b6d0f99216fe1ae8459ae3))

* pylint action ([`9478db8`](https://github.com/opentargets/genetics_etl_python/commit/9478db813e2f4aef06dfda54048a0e75bceea820))

* Black fixed ([`dfc4cc1`](https://github.com/opentargets/genetics_etl_python/commit/dfc4cc19dd6c0f6d7aad625883f840bfe491854a))

* Checking gh action works III ([`ed0edc2`](https://github.com/opentargets/genetics_etl_python/commit/ed0edc2087ae785c68a12e63b3258c7c01fd3e7f))

* Checking gh action works II ([`e84f4df`](https://github.com/opentargets/genetics_etl_python/commit/e84f4df077e3a57b3deee037746a9a712f3667a7))

* Merge branch &#39;do_black_action&#39; of https://github.com/opentargets/genetics_spark_coloc into do_black_action ([`b2b85d2`](https://github.com/opentargets/genetics_etl_python/commit/b2b85d2734a80fd1495265a0fc2cfb56f952bb5e))

* Checking gh action works ([`e057b23`](https://github.com/opentargets/genetics_etl_python/commit/e057b23c4bf7d42a18ce37c44ee0acc52d842955))

* Remove options ([`6790860`](https://github.com/opentargets/genetics_etl_python/commit/67908609e7586dd0cc3c9f3496fb6d40bcae42a5))

* Trying black integration ([`6776309`](https://github.com/opentargets/genetics_etl_python/commit/677630916a93c6e85a050e4add333bf7ecf96af2))

* unnecessary code removed ([`1d2d971`](https://github.com/opentargets/genetics_etl_python/commit/1d2d971c6dd3b98dfc126a5260e38aa221ad0d5e))

* working version scaled to 64 cores ([`7072332`](https://github.com/opentargets/genetics_etl_python/commit/7072332f7b34c878f0af0f6beeadea8fd1feff67))

* Rename function ([`00ee91a`](https://github.com/opentargets/genetics_etl_python/commit/00ee91a19f52a3c8485ef3e4f12bd838cd8fef96))

* TODO resolved ([`27f0f73`](https://github.com/opentargets/genetics_etl_python/commit/27f0f7315b8de6224543ff4f090de1a565a97354))

* Merge pull request #1 from mkarmona:mkarmona-patch-1

improve join conditions and better hash function ([`e30e811`](https://github.com/opentargets/genetics_etl_python/commit/e30e8115d1f51a74b8a873b640663b556c300f85))

* including chromosome in join ([`b9a4589`](https://github.com/opentargets/genetics_etl_python/commit/b9a45890aed8c2f335e52b7fa2ffbdb57fa9c4f3))

* Comment out filtering by chromosome ([`7c1d2e2`](https://github.com/opentargets/genetics_etl_python/commit/7c1d2e28abdc8094c96173011aba31b6c1f413ff))

* improve join conditions and better hash function ([`3088c37`](https://github.com/opentargets/genetics_etl_python/commit/3088c370081c8583d95806682a7e2b58e9afce45))

* duplicated column ([`477239c`](https://github.com/opentargets/genetics_etl_python/commit/477239c7af835c9e38027b03dca0bba590ac20ca))

* simpler execution ([`d7f27a1`](https://github.com/opentargets/genetics_etl_python/commit/d7f27a1026580938e6922989153163ee166803bd))

* Merge branch &#39;main&#39; of https://github.com/d0choa/genetics_spark_coloc ([`2c1de88`](https://github.com/opentargets/genetics_etl_python/commit/2c1de88fc8803e103020c82f535ef5722e56c63c))

* Feedback from mk ([`10f09b8`](https://github.com/opentargets/genetics_etl_python/commit/10f09b8c8e999f6df8848f09556e1a31c3c8c79b))

* duplicated row ([`693e32d`](https://github.com/opentargets/genetics_etl_python/commit/693e32dfcd3c60dc69c958e99b3d63b0b13e7c43))

* Bug fixing ([`5b6217c`](https://github.com/opentargets/genetics_etl_python/commit/5b6217c6d978490227fae6ad958eeb0ff4f72c05))

* ammend comment ([`ea7adf5`](https://github.com/opentargets/genetics_etl_python/commit/ea7adf567a7ae73a274298985d86ac5986613f9c))

* Clean up (not tested) ([`e99ba28`](https://github.com/opentargets/genetics_etl_python/commit/e99ba28dde19e41fd9603ff2fb4001e51ec42e22))

* Working version of self-join ([`8f41180`](https://github.com/opentargets/genetics_etl_python/commit/8f41180332ee9bace346a1ffd5eadeb3deb8316e))

* indentation fixed ([`f420aa1`](https://github.com/opentargets/genetics_etl_python/commit/f420aa10785a0addb6f666b4b539bc3f8a216716))

* add expected results ([`751c4a9`](https://github.com/opentargets/genetics_etl_python/commit/751c4a98795beef51877858f5a04bce7914d4010))

* reverting changes to work with vectors ([`b4eb54e`](https://github.com/opentargets/genetics_etl_python/commit/b4eb54e74b71180cb87c97811010b9059523e4ee))

* coloc running version ([`9042b69`](https://github.com/opentargets/genetics_etl_python/commit/9042b69e92bfbdb07a86cc31c498fa597808885e))

* Not tested version ([`f620947`](https://github.com/opentargets/genetics_etl_python/commit/f620947ad268371f215bdce10414b9107360766c))

* unnecessary on clause removed ([`36a5798`](https://github.com/opentargets/genetics_etl_python/commit/36a5798b2953f6ce1bebcd3840b0a2d88ca6e595))

* New approach: self-join, using the complex “on” clause, creating the nested object with all the tag variants ([`13580ca`](https://github.com/opentargets/genetics_etl_python/commit/13580ca00c9fdfaae8221714d5ad9f9665ea3fc2))

* Coloc using windows instead of groupby ([`955e30c`](https://github.com/opentargets/genetics_etl_python/commit/955e30c56a77075358ad45141272a930b11af628))

* distinct is not necessary ([`fd6a639`](https://github.com/opentargets/genetics_etl_python/commit/fd6a63951485830b039b58d871f1687290e4700d))

* config based on chr22 partitiiooned dataset ([`31f2d24`](https://github.com/opentargets/genetics_etl_python/commit/31f2d245af0658849f77a78bd81327a7cc5c440e))

* More clear to select than to drop ([`dbb88ba`](https://github.com/opentargets/genetics_etl_python/commit/dbb88bace89987caf8283d9546906c75d669b92c))

* separation between py files and yaml ([`220c613`](https://github.com/opentargets/genetics_etl_python/commit/220c613f2ae501d26a36d860ff89e3befbc41855))

* more leeway before destroying machine ([`02f59cb`](https://github.com/opentargets/genetics_etl_python/commit/02f59cbfd2ea55e90a74ca6689ccac1711b677c2))

* to allow spark debugging interfaces ([`0c51c7f`](https://github.com/opentargets/genetics_etl_python/commit/0c51c7f6bc4fe3ab8f432a6c74e87350e56e1a61))

* partial improvements ([`9484c45`](https://github.com/opentargets/genetics_etl_python/commit/9484c45026b8e4ffff4218e3ed360e58700bf082))

* incomplete on in join ([`8834a44`](https://github.com/opentargets/genetics_etl_python/commit/8834a44079205c03380d4dd06555acd701914ff7))

* improvements on carrying over info from credset ([`909a17f`](https://github.com/opentargets/genetics_etl_python/commit/909a17fe6563bf6a0c45926bb3ae854ec9e63287))

* unstable: playing with structure ([`90eb673`](https://github.com/opentargets/genetics_etl_python/commit/90eb6730023637606a86f159990edf554148eef8))

* Trying to carry variant/study metadata ([`fc9197c`](https://github.com/opentargets/genetics_etl_python/commit/fc9197c7f9db06157720e1204f11b595871a390e))

* comment ([`a77e808`](https://github.com/opentargets/genetics_etl_python/commit/a77e808ef98bed167a9785cc029b22baca431f51))

* more readable ([`1610438`](https://github.com/opentargets/genetics_etl_python/commit/1610438407b869f2b38dfcb50399785de19da3cd))

* Restructuring the project ([`77cf939`](https://github.com/opentargets/genetics_etl_python/commit/77cf9395756a84e42ecfcc0804a15d1041ace049))

* typo fixed ([`9c0e3f6`](https://github.com/opentargets/genetics_etl_python/commit/9c0e3f61130f54029bcd6aad84b49715068b6a53))

* 32 cores is more realistic at the moment ([`0b70026`](https://github.com/opentargets/genetics_etl_python/commit/0b70026c77e24cbfe4a12031c0894e0b03276b18))

* unnecessary nesting removed ([`b8e63f7`](https://github.com/opentargets/genetics_etl_python/commit/b8e63f7c9bcfc9ccb027be19507fcd4fbd15efaf))

* avoiding crossJoin to best manage partitions ([`440f11d`](https://github.com/opentargets/genetics_etl_python/commit/440f11d927a3e34ec983a631f938e500adb89419))

* explain ([`f03e2be`](https://github.com/opentargets/genetics_etl_python/commit/f03e2beb648056c03c738255cbf01d1096c6da68))

* removing unnecesary persist ([`6310944`](https://github.com/opentargets/genetics_etl_python/commit/6310944dded491d4359825542592366a37f35019))

* machine definition and submission now work ([`f3f96da`](https://github.com/opentargets/genetics_etl_python/commit/f3f96da1d426990bccc35e45c358c36e4c4bfccf))

* Bug writing file ([`10840dc`](https://github.com/opentargets/genetics_etl_python/commit/10840dc53a3c3cf5f20267a2a1ababb9f560a160))

* Slimmer version to check performance ([`5ed86b2`](https://github.com/opentargets/genetics_etl_python/commit/5ed86b2e9fd946043775d56d16d2151dcc68ff47))

* trying to make hydra fly ([`bc14e71`](https://github.com/opentargets/genetics_etl_python/commit/bc14e71bf1750b52a3245636949bb7ba670d248e))

* Still not working version ([`b312857`](https://github.com/opentargets/genetics_etl_python/commit/b3128576969811f51b3906cbabd57fea6e37d31c))

* Initial commit ([`8a29b09`](https://github.com/opentargets/genetics_etl_python/commit/8a29b09a42e32a7a64b6eb09f6a5b5a4dbe19298))
