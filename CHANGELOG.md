# CHANGELOG

## [1.6.65](https://github.com/deepmodeling/dflow/compare/v1.6.64...v1.6.65) (2023-03-01)


### Bug Fixes

* add group to dynamic resource ([4b6ce4a](https://github.com/deepmodeling/dflow/commit/4b6ce4a492813156fb51ed06c21d108d71febafe))

## [1.6.64](https://github.com/deepmodeling/dflow/compare/v1.6.63...v1.6.64) (2023-02-28)


### Bug Fixes

* move debug tests to the directory tests ([c25f984](https://github.com/deepmodeling/dflow/commit/c25f984427bcb7372837cecadbde713e41f44f28))
* set node to phase running after replay a step ([c25f984](https://github.com/deepmodeling/dflow/commit/c25f984427bcb7372837cecadbde713e41f44f28))
* step.workflow is None in debug mode ([c25f984](https://github.com/deepmodeling/dflow/commit/c25f984427bcb7372837cecadbde713e41f44f28))
* use if_expression explicitly to ensure the same behavior in both default and debug modes ([c25f984](https://github.com/deepmodeling/dflow/commit/c25f984427bcb7372837cecadbde713e41f44f28))

## [1.6.63](https://github.com/deepmodeling/dflow/compare/v1.6.62...v1.6.63) (2023-02-28)


### Bug Fixes

* big parameter passed in debug mode ([c7a03a7](https://github.com/deepmodeling/dflow/commit/c7a03a79fb574d3d772d742df50b92f379428e19))

## [1.6.62](https://github.com/deepmodeling/dflow/compare/v1.6.61...v1.6.62) (2023-02-27)


### Bug Fixes

* remove workflow name from global outputs ([93487db](https://github.com/deepmodeling/dflow/commit/93487db1b07232b24f40503a386393da9be65ef9))
* replay of step ([93487db](https://github.com/deepmodeling/dflow/commit/93487db1b07232b24f40503a386393da9be65ef9))

## [1.6.61](https://github.com/deepmodeling/dflow/compare/v1.6.60...v1.6.61) (2023-02-24)


### Bug Fixes

* wrong value of output parameter of step ([bbbc6d4](https://github.com/deepmodeling/dflow/commit/bbbc6d4ac9a78d844736997d16e2a5e9a707f980))

## [1.6.60](https://github.com/deepmodeling/dflow/compare/v1.6.59...v1.6.60) (2023-02-24)


### Bug Fixes

* support for expressions in values of parameters ([902f4ed](https://github.com/deepmodeling/dflow/commit/902f4ed7185acd6db891cc0f80aba1721fad21ad))

## [1.6.59](https://github.com/deepmodeling/dflow/compare/v1.6.58...v1.6.59) (2023-02-24)


### Bug Fixes

* do not copy reused artifact for default ([2fa94ed](https://github.com/deepmodeling/dflow/commit/2fa94edbc6bb231d7c15c61970461be04ed42436))

## [1.6.58](https://github.com/deepmodeling/dflow/compare/v1.6.57...v1.6.58) (2023-02-23)


### Bug Fixes

* allow None in List[Path] when checking types of artifact ([b5b6033](https://github.com/deepmodeling/dflow/commit/b5b603349a80c62dfa7774f12460a369ae28b053))
* support for login with phone number for bohrium ([b5b6033](https://github.com/deepmodeling/dflow/commit/b5b603349a80c62dfa7774f12460a369ae28b053))

## [1.6.57](https://github.com/deepmodeling/dflow/compare/v1.6.56...v1.6.57) (2023-02-22)


### Bug Fixes

* add pre_script and post_script ([fa0319d](https://github.com/deepmodeling/dflow/commit/fa0319db9853c9a6e6d259275177cb595a0e810c))
* dflow_success_tag cannot stat in dispatcher executor ([fa0319d](https://github.com/deepmodeling/dflow/commit/fa0319db9853c9a6e6d259275177cb595a0e810c))

## [1.6.56](https://github.com/deepmodeling/dflow/compare/v1.6.55...v1.6.56) (2023-02-21)


### Bug Fixes

* create secert with http headers ([5f384cd](https://github.com/deepmodeling/dflow/commit/5f384cd4a549c277ce5352cb89699bca12ed2056))

## [1.6.55](https://github.com/deepmodeling/dflow/compare/v1.6.54...v1.6.55) (2023-02-21)


### Bug Fixes

* pass env vars in dispatcher executor ([806c0dd](https://github.com/deepmodeling/dflow/commit/806c0ddb8199a928c28641135c36be2d058eef0a))

## [1.6.54](https://github.com/deepmodeling/dflow/compare/v1.6.53...v1.6.54) (2023-02-21)


### Bug Fixes

* support for artifact with nested dict of path ([da78171](https://github.com/deepmodeling/dflow/commit/da78171e5eddfa855336a8d03fc59d1fd5675c18))

## [1.6.53](https://github.com/deepmodeling/dflow/compare/v1.6.52...v1.6.53) (2023-02-16)


### Bug Fixes

* bug of merge_sliced_step in debug mode ([b364277](https://github.com/deepmodeling/dflow/commit/b364277e17486d2920880689290108247f2e5edd))

## [1.6.52](https://github.com/deepmodeling/dflow/compare/v1.6.51...v1.6.52) (2023-02-16)


### Bug Fixes

* support for handling multi slices in one dispatcher job ([5de1157](https://github.com/deepmodeling/dflow/commit/5de1157b5c15fbc11281889f42024a3543c498d9))

## [1.6.51](https://github.com/deepmodeling/dflow/compare/v1.6.50...v1.6.51) (2023-02-16)


### Bug Fixes

* upgrade ([4ddab4c](https://github.com/deepmodeling/dflow/commit/4ddab4cfe393117c9b84f5b707fac3eefba21e87))

## [1.6.50](https://github.com/deepmodeling/dflow/compare/v1.6.49...v1.6.50) (2023-02-16)


### Bug Fixes

* big parameter string null bug ([f9925c2](https://github.com/deepmodeling/dflow/commit/f9925c2c6b8de435e0314b6011a8a24335e492cb))

## [1.6.50]

### Bug Fixes

* Support BigParameter default value on local machine

## [1.6.49](https://github.com/deepmodeling/dflow/compare/v1.6.48...v1.6.49) (2023-02-15)


### Bug Fixes

* deprecate lebesgue executor ([0a68cb9](https://github.com/deepmodeling/dflow/commit/0a68cb9663d320744c6d2f8ed435bae8e6a020b3))
* one task not failing the whole group when pool_size used ([0a68cb9](https://github.com/deepmodeling/dflow/commit/0a68cb9663d320744c6d2f8ed435bae8e6a020b3))

## [1.6.48](https://github.com/deepmodeling/dflow/compare/v1.6.47...v1.6.48) (2023-02-14)


### Bug Fixes

* direct jsonpickle.dumps for big parameter ([7e1a063](https://github.com/deepmodeling/dflow/commit/7e1a063fbcfdd7e4ddbcaff3e0b9f761f1fc720f))
* support for argo_len of big parameter ([7e1a063](https://github.com/deepmodeling/dflow/commit/7e1a063fbcfdd7e4ddbcaff3e0b9f761f1fc720f))

## [1.6.47](https://github.com/deepmodeling/dflow/compare/v1.6.46...v1.6.47) (2023-02-13)


### Bug Fixes

* add API for creating bohrium job group ID ([772ad4d](https://github.com/deepmodeling/dflow/commit/772ad4ddb0f89e2dde6c0a0a231fa7c944788e6e))
* process cannot pass dflow configs ([77febb9](https://github.com/deepmodeling/dflow/commit/77febb9c7d618bfd62cac8f8c504e8d707909196))
* support for expression of loop item ([12c58c9](https://github.com/deepmodeling/dflow/commit/12c58c9e6cc6caff593a3e524f4eb882d492561d))
* support for slicing big parameter ([772ad4d](https://github.com/deepmodeling/dflow/commit/772ad4ddb0f89e2dde6c0a0a231fa7c944788e6e))

## [1.6.46](https://github.com/deepmodeling/dflow/compare/v1.6.45...v1.6.46) (2023-02-10)


### Bug Fixes

* jsonpickle.loads failed in bohrium executor ([24a603e](https://github.com/deepmodeling/dflow/commit/24a603e9ac8b28e15dee27e2e767c3973de8c829))

## [1.6.45](https://github.com/deepmodeling/dflow/compare/v1.6.44...v1.6.45) (2023-02-10)


### Bug Fixes

* save key-id mapping for super OP ([fd18d0e](https://github.com/deepmodeling/dflow/commit/fd18d0efaa0679f9602515247ce4b6ce2173dbab))

## [1.6.44](https://github.com/deepmodeling/dflow/compare/v1.6.43...v1.6.44) (2023-02-09)


### Bug Fixes

* fix dispatcher bohrium cannot sync log ([119e91c](https://github.com/deepmodeling/dflow/commit/119e91c4ad629cf81457a60ca8fa8424f5301cd5))

## [1.6.43](https://github.com/deepmodeling/dflow/compare/v1.6.42...v1.6.43) (2023-02-09)


### Bug Fixes

* add comments to query functions ([fa92dce](https://github.com/deepmodeling/dflow/commit/fa92dce6d0b46f34601210de173400bed87a97c5))
* different behaviors of OSS client ([fa92dce](https://github.com/deepmodeling/dflow/commit/fa92dce6d0b46f34601210de173400bed87a97c5))

## [1.6.42](https://github.com/deepmodeling/dflow/compare/v1.6.41...v1.6.42) (2023-02-07)


### Bug Fixes

* let argument override default value of big parameter ([22db61b](https://github.com/deepmodeling/dflow/commit/22db61bf4e6cfa45603044f5f74749916836ddde))

## [1.6.41](https://github.com/deepmodeling/dflow/compare/v1.6.40...v1.6.41) (2023-02-06)


### Bug Fixes

* add API query_global_outputs ([b96922d](https://github.com/deepmodeling/dflow/commit/b96922dfe4d0d3b77067d35a43e3e739ad8687e7))
* prepend dflow_bigpar_ to global name of parameter saved as artifact ([b96922d](https://github.com/deepmodeling/dflow/commit/b96922dfe4d0d3b77067d35a43e3e739ad8687e7))

## [1.6.40](https://github.com/deepmodeling/dflow/compare/v1.6.39...v1.6.40) (2023-02-06)


### Bug Fixes

* downgrade for query_step_by_key ([8485582](https://github.com/deepmodeling/dflow/commit/8485582756e754af9496bc48362930f467de3db0))
* save key2id map in global outputs, add query_step_by_key ([af64ea0](https://github.com/deepmodeling/dflow/commit/af64ea06f190de3aa45de4c4483931f42b637622))
* support default value of big parameter ([af64ea0](https://github.com/deepmodeling/dflow/commit/af64ea06f190de3aa45de4c4483931f42b637622))

## [1.6.39](https://github.com/deepmodeling/dflow/compare/v1.6.38...v1.6.39) (2023-02-03)


### Bug Fixes

* support group_size for debug mode ([5adc44c](https://github.com/deepmodeling/dflow/commit/5adc44ce3411951699ae80b4225ec1af5bd3d32d))

## [1.6.38](https://github.com/deepmodeling/dflow/compare/v1.6.37...v1.6.38) (2023-02-03)


### Bug Fixes

* add workflow_annotations to global config ([594781f](https://github.com/deepmodeling/dflow/commit/594781fb640d36367914717e4c41408669ab4541))
* restrict jsonpickle==2.2.0 ([594781f](https://github.com/deepmodeling/dflow/commit/594781fb640d36367914717e4c41408669ab4541))

## [1.6.37](https://github.com/deepmodeling/dflow/compare/v1.6.36...v1.6.37) (2023-02-02)


### Bug Fixes

* set default mode to None for input artifacts ([477f9d6](https://github.com/deepmodeling/dflow/commit/477f9d63868a5822c40a44a954f0aa180b363305))

## [1.6.36](https://github.com/deepmodeling/dflow/compare/v1.6.35...v1.6.36) (2023-02-02)


### Bug Fixes

* set default mode to 755 for input artifacts ([275b03d](https://github.com/deepmodeling/dflow/commit/275b03dda2806fc2bc117eee1f6340351d9fb1f0))

## [1.6.35](https://github.com/deepmodeling/dflow/compare/v1.6.34...v1.6.35) (2023-02-01)


### Bug Fixes

* add group_size feature for slices ([374e244](https://github.com/deepmodeling/dflow/commit/374e244e1c099d7dac1d6e12301ac5ea08a0b4d9))

## [1.6.34](https://github.com/deepmodeling/dflow/compare/v1.6.33...v1.6.34) (2023-01-31)


### Bug Fixes

* add oss storage client ([70c8950](https://github.com/deepmodeling/dflow/commit/70c8950173ae1b519930c3941ca6c93c8f53e126))
* add warns for handling argo objects ([70c8950](https://github.com/deepmodeling/dflow/commit/70c8950173ae1b519930c3941ca6c93c8f53e126))
* support dict of path for artifact ([70c8950](https://github.com/deepmodeling/dflow/commit/70c8950173ae1b519930c3941ca6c93c8f53e126))

## [1.6.33](https://github.com/deepmodeling/dflow/compare/v1.6.32...v1.6.33) (2023-01-29)


### Bug Fixes

* add argument interactive to run_command ([f7bd903](https://github.com/deepmodeling/dflow/commit/f7bd903f4912f78a503798b4d4f6141bc3f88c85))

## [1.6.32](https://github.com/deepmodeling/dflow/compare/v1.6.31...v1.6.32) (2023-01-20)


### Bug Fixes

* sys.stdin.encoding -&gt; sys.stdout.encoding ([f531786](https://github.com/deepmodeling/dflow/commit/f531786ce0ebdabd7ceebf2766821587d0e932c1))
* workaround for unavailable exit code of Bohrium job; ([f531786](https://github.com/deepmodeling/dflow/commit/f531786ce0ebdabd7ceebf2766821587d0e932c1))

## [1.6.31](https://github.com/deepmodeling/dflow/compare/v1.6.30...v1.6.31) (2023-01-18)


### Bug Fixes

* use bash -ic instead of bash -lc ([280b7b1](https://github.com/deepmodeling/dflow/commit/280b7b13b024f8c0104fc185d6038d555bcfeac6))

## [1.6.30](https://github.com/deepmodeling/dflow/compare/v1.6.29...v1.6.30) (2023-01-17)


### Bug Fixes

* continue on number/ratio skip errors ([4b18efa](https://github.com/deepmodeling/dflow/commit/4b18efa1d42fdb7a78c3ff7e29aeebc0295bd998))

## [1.6.29](https://github.com/deepmodeling/dflow/compare/v1.6.28...v1.6.29) (2023-01-17)


### Bug Fixes

* add try_bash to run_command ([ae1b77c](https://github.com/deepmodeling/dflow/commit/ae1b77c2c7c6ef622a3661c8f95e4fbde8190ca1))

## [1.6.28](https://github.com/deepmodeling/dflow/compare/v1.6.27...v1.6.28) (2023-01-07)


### Bug Fixes

* add log to Bohrium dispatcher executor ([14e5c58](https://github.com/deepmodeling/dflow/commit/14e5c5850a76f9ef22ba96b1b440107eea588b8b))
* add retry to dispatcher executor ([14e5c58](https://github.com/deepmodeling/dflow/commit/14e5c5850a76f9ef22ba96b1b440107eea588b8b))

## [1.6.27](https://github.com/deepmodeling/dflow/compare/v1.6.26...v1.6.27) (2023-01-04)


### Bug Fixes

* continue_on_success_ratio used with executor ([e1f61ae](https://github.com/deepmodeling/dflow/commit/e1f61ae754fceb62ee3c284bcdf861dfb76f9a4b))

## [1.6.26](https://github.com/deepmodeling/dflow/compare/v1.6.25...v1.6.26) (2023-01-03)


### Bug Fixes

* duplicate template name when multiple continue_on_success_ratio used ([3187ac6](https://github.com/deepmodeling/dflow/commit/3187ac6a20a09ffe6b142867be278d55c037b2a8))
* enhance performance of querying and reusing ([3187ac6](https://github.com/deepmodeling/dflow/commit/3187ac6a20a09ffe6b142867be278d55c037b2a8))

## [1.6.25](https://github.com/deepmodeling/dflow/compare/v1.6.24...v1.6.25) (2022-12-31)


### Bug Fixes

* no share path for non-dp user ([ad71a77](https://github.com/deepmodeling/dflow/commit/ad71a7764156f4211b0d7640768caafdaef9e294))

## [1.6.24](https://github.com/deepmodeling/dflow/compare/v1.6.23...v1.6.24) (2022-12-30)


### Bug Fixes

* unclosed file ResourceWarning ([2e61ef1](https://github.com/deepmodeling/dflow/commit/2e61ef167a824785ee65572e1e5b67b4e89686c5))

## [1.6.23](https://github.com/deepmodeling/dflow/compare/v1.6.22...v1.6.23) (2022-12-28)


### Bug Fixes

* ArgoStep deepcopy input dict ([47ca63d](https://github.com/deepmodeling/dflow/commit/47ca63dfc7cceb75a206892fa158fd7391e99370))

## [1.6.22](https://github.com/deepmodeling/dflow/compare/v1.6.21...v1.6.22) (2022-12-28)


### Bug Fixes

* ArgoStep not idempotent ([ac255bb](https://github.com/deepmodeling/dflow/commit/ac255bb317c935bbc8cf4838841db784cd16fa76))

## [1.6.21](https://github.com/deepmodeling/dflow/compare/v1.6.20...v1.6.21) (2022-12-28)


### Bug Fixes

* ArgoStep not idempotent ([21e4628](https://github.com/deepmodeling/dflow/commit/21e462866bacdc0c5446a1f8ae7093bbd1dc336a))

## [1.6.20](https://github.com/deepmodeling/dflow/compare/v1.6.19...v1.6.20) (2022-12-27)


### Bug Fixes

* enhance query_step efficiency ([3c5bd34](https://github.com/deepmodeling/dflow/commit/3c5bd348f5e51544701e69f3a1743f4704767985))

## [1.6.19](https://github.com/deepmodeling/dflow/compare/v1.6.18...v1.6.19) (2022-12-27)


### Bug Fixes

* refresh token in tiefblue client ([ca81d25](https://github.com/deepmodeling/dflow/commit/ca81d2598d57cf152e11f1d2610e0748e9d03237))
* remove empty step group ([ca81d25](https://github.com/deepmodeling/dflow/commit/ca81d2598d57cf152e11f1d2610e0748e9d03237))

## [1.6.18](https://github.com/deepmodeling/dflow/compare/v1.6.17...v1.6.18) (2022-12-20)


### Bug Fixes

* output parameter has no attribute path ([95920d3](https://github.com/deepmodeling/dflow/commit/95920d3b6c33921548affa3f308e4189eb4bf89e))

## [1.6.17](https://github.com/deepmodeling/dflow/compare/v1.6.16...v1.6.17) (2022-12-15)


### Bug Fixes

* dispatcher bohrium use bohrium.config ([3f3062b](https://github.com/deepmodeling/dflow/commit/3f3062b038715fe026e0cf67d168ce6e02f4bf30))
* executor used for context ([3f3062b](https://github.com/deepmodeling/dflow/commit/3f3062b038715fe026e0cf67d168ce6e02f4bf30))

## [1.6.16](https://github.com/deepmodeling/dflow/compare/v1.6.15...v1.6.16) (2022-12-15)


### Bug Fixes

* support for config from env var ([1fff9f2](https://github.com/deepmodeling/dflow/commit/1fff9f2b5f238f3d4161ce0676960b40a820596e))

## [1.6.15](https://github.com/deepmodeling/dflow/compare/v1.6.14...v1.6.15) (2022-12-14)


### Bug Fixes

* **path on windows:** use raw string to make the cross-platform path behaviors right. ([0b28521](https://github.com/deepmodeling/dflow/commit/0b2852181567e787b2d0b5fe197e521b9485c988))

## [1.6.14](https://github.com/deepmodeling/dflow/compare/v1.6.13...v1.6.14) (2022-12-09)


### Bug Fixes

* remove unecessary **kwargs ([35f1dee](https://github.com/deepmodeling/dflow/commit/35f1dee34ef7c58e45bd464e5b5f5a51532e5165))
* support for registering first slice only in lineage ([35f1dee](https://github.com/deepmodeling/dflow/commit/35f1dee34ef7c58e45bd464e5b5f5a51532e5165))
* support storage to argo gateway ([4ba49f2](https://github.com/deepmodeling/dflow/commit/4ba49f2b2a2324644d1ade256a539ff8508142ee))

## [1.6.13](https://github.com/deepmodeling/dflow/compare/v1.6.12...v1.6.13) (2022-12-08)


### Bug Fixes

* add HTTP headers to config ([a2573ff](https://github.com/deepmodeling/dflow/commit/a2573ffb962bee56c43cc64902668fdf13245c41))
* create secret for env var ([547dcaa](https://github.com/deepmodeling/dflow/commit/547dcaaa3625111ecbdc7b1bc3c0db879bc69fb5))
* support for env from a source ([12cd852](https://github.com/deepmodeling/dflow/commit/12cd8526592c680d267ccfcf501351b35f910faa))
* support for treating input artifact as non-archived file ([a2573ff](https://github.com/deepmodeling/dflow/commit/a2573ffb962bee56c43cc64902668fdf13245c41))

## [1.6.12](https://github.com/deepmodeling/dflow/compare/v1.6.11...v1.6.12) (2022-12-04)


### Bug Fixes

* support Steps template for continue_on_num_success and continue_on_success_ratio ([d1ce485](https://github.com/deepmodeling/dflow/commit/d1ce48529dc9cb6439610347542b2ee49f33d394))

## [1.6.11](https://github.com/deepmodeling/dflow/compare/v1.6.10...v1.6.11) (2022-12-02)


### Bug Fixes

* add extra prefixes ignored by S3Artifact auto-prefixing ([2e3ce02](https://github.com/deepmodeling/dflow/commit/2e3ce023044da8c11bbe1cf7bc045a0cb17f0c38))
* replace github link with gitee link in install-*.sh ([2e3ce02](https://github.com/deepmodeling/dflow/commit/2e3ce023044da8c11bbe1cf7bc045a0cb17f0c38))
* support for pass list of artifacts to input artifact ([b12c62e](https://github.com/deepmodeling/dflow/commit/b12c62e6c5ad16c0b9beec4c3d668e5e2a8d1eb5))

## [1.6.10](https://github.com/deepmodeling/dflow/compare/v1.6.9...v1.6.10) (2022-11-30)


### Bug Fixes

* add debug info for workflow manifest ([5ca6fa7](https://github.com/deepmodeling/dflow/commit/5ca6fa7230d286b596685ce99bf859c3c18f60af))
* add if __name__ == '__main__': to Python OP ([5ca6fa7](https://github.com/deepmodeling/dflow/commit/5ca6fa7230d286b596685ce99bf859c3c18f60af))
* add to_dict, to_json, to_yaml methods to workflow ([56ccf13](https://github.com/deepmodeling/dflow/commit/56ccf137b3029138efc5f09545f28ec714039e4a))
* find function OPs ([56ccf13](https://github.com/deepmodeling/dflow/commit/56ccf137b3029138efc5f09545f28ec714039e4a))

## [1.6.9](https://github.com/deepmodeling/dflow/compare/v1.6.8...v1.6.9) (2022-11-21)


### Bug Fixes

* use render_script method to map /tmp to /Users/x.liu/workflows/dflow/tmp for PythonOPTemplate ([77a50a6](https://github.com/deepmodeling/dflow/commit/77a50a6c0ac2bdec1faba73ec3a6f663e737a27e))

## [1.6.8](https://github.com/deepmodeling/dflow/compare/v1.6.7...v1.6.8) (2022-11-21)


### Bug Fixes

* bohrium in dispatcher executor ([2c19211](https://github.com/deepmodeling/dflow/commit/2c192114e475c580897a46df19cf3407caf1ba9b))

## [1.6.7](https://github.com/deepmodeling/dflow/compare/v1.6.6...v1.6.7) (2022-11-18)


### Bug Fixes

* add host addr for lebesgue executor ([d57b425](https://github.com/deepmodeling/dflow/commit/d57b4250f2d4a14fb76ad361320087911c870d18))
* make OP function callable ([d57b425](https://github.com/deepmodeling/dflow/commit/d57b4250f2d4a14fb76ad361320087911c870d18))

## [1.6.6](https://github.com/deepmodeling/dflow/compare/v1.6.5...v1.6.6) (2022-11-18)


### Bug Fixes

* add argo_sum and argo_concat ([487349f](https://github.com/deepmodeling/dflow/commit/487349f6d696a4dbf3b7efce4440b66aaa7338dc))
* add register_output_artifacts for step ([e047cf4](https://github.com/deepmodeling/dflow/commit/e047cf41dba7b919f7b42343f69f1344a83c8775))
* create secret for private registry ([e047cf4](https://github.com/deepmodeling/dflow/commit/e047cf41dba7b919f7b42343f69f1344a83c8775))
* debug mode: mimic argo's behavior on output parameter of parallel steps ([35f7f99](https://github.com/deepmodeling/dflow/commit/35f7f997c8e95f1ac3d95b8e2ba87215617b24d3))
* enhance error message of bohrium client ([615f4c0](https://github.com/deepmodeling/dflow/commit/615f4c03cbb3f876bd6091f5e22e1eb3b2ad9410))
* enhance function OP ([6c92799](https://github.com/deepmodeling/dflow/commit/6c9279938cde2904c4b8ccf27902728dc9ec8676))
* no prefix when user use S3Artifact() ([7d17af3](https://github.com/deepmodeling/dflow/commit/7d17af3ba0a039b3c8b5cd8cca0e9dd3f016c82a))
* support for always passing root path of artifact ([e9257fa](https://github.com/deepmodeling/dflow/commit/e9257faae5449f9821f7b8f04d8e1a39bf6d39da))

## [1.6.5](https://github.com/deepmodeling/dflow/compare/v1.6.4...v1.6.5) (2022-11-11)


### Bug Fixes

* add download method to S3Artifact ([6c2150a](https://github.com/deepmodeling/dflow/commit/6c2150a7f3f2cfbb6f792ab36d472798ef0882f7))
* add to_dict and from_dict methods to S3Artifact ([e999917](https://github.com/deepmodeling/dflow/commit/e999917add6047898457d384f6846038a78ae3ab))
* bug in collecting output parameters of sliced step ([a84f840](https://github.com/deepmodeling/dflow/commit/a84f8401697b5200e55a6494cb5cb1e97214448a))
* download S3Artifact ([adfd046](https://github.com/deepmodeling/dflow/commit/adfd046dc35497986282ecf5c3f818ab73060928))
* support for setting bohrium url for bohrium executor ([a84f840](https://github.com/deepmodeling/dflow/commit/a84f8401697b5200e55a6494cb5cb1e97214448a))

## [1.6.4](https://github.com/deepmodeling/dflow/compare/v1.6.3...v1.6.4) (2022-11-04)


### Bug Fixes

* get k8s client when used ([c1b3470](https://github.com/deepmodeling/dflow/commit/c1b3470a09694430b47a42e9d535b273221887db))

## [1.6.3](https://github.com/deepmodeling/dflow/compare/v1.6.2...v1.6.3) (2022-11-04)


### Bug Fixes

* future len of input parameter ([7fd6f13](https://github.com/deepmodeling/dflow/commit/7fd6f13371e9e83add4228fd9f149bc6f9ad6318))

## [1.6.2](https://github.com/deepmodeling/dflow/compare/v1.6.1...v1.6.2) (2022-11-02)


### Bug Fixes

* parse repo info from configmap ([f1aab93](https://github.com/deepmodeling/dflow/commit/f1aab932550d3fcfb53706117a0c95e49d843a37))

## [1.6.1](https://github.com/deepmodeling/dflow/compare/v1.6.0...v1.6.1) (2022-11-01)


### Bug Fixes

* a bug in sliced super op ([e1bf3a0](https://github.com/deepmodeling/dflow/commit/e1bf3a044e33273031170bf873227ccd3de945f4))
* add bohrium storage client ([8c7092c](https://github.com/deepmodeling/dflow/commit/8c7092c65b5f4ca3260fc46c0624f8f3b45c924a))
* support for sliced python op in sliced super op ([e1bf3a0](https://github.com/deepmodeling/dflow/commit/e1bf3a044e33273031170bf873227ccd3de945f4))
* support for storage plugins ([8c7092c](https://github.com/deepmodeling/dflow/commit/8c7092c65b5f4ca3260fc46c0624f8f3b45c924a))

## [1.6.0](https://github.com/deepmodeling/dflow/compare/v1.5.14...v1.6.0) (2022-10-26)


### Features

* introduce bohrium plugin which provide realtime log and adapt bohrium platform ([9296a3f](https://github.com/deepmodeling/dflow/commit/9296a3fecb8d5eb7ad30b573e7f58e57520aaa4a))


### Bug Fixes

* add artifact_repo_key ([0818d12](https://github.com/deepmodeling/dflow/commit/0818d1299fb3cc8f48a43102f268e8747bf3f918))
* slices for Steps/DAG ([0818d12](https://github.com/deepmodeling/dflow/commit/0818d1299fb3cc8f48a43102f268e8747bf3f918))

## [1.5.14](https://github.com/deepmodeling/dflow/compare/v1.5.13...v1.5.14) (2022-10-19)


### Bug Fixes

* add readme for debug mode ([151130c](https://github.com/deepmodeling/dflow/commit/151130c8d27ddc4928bc829ee963a3a013c91262))
* enhance error message ([f009607](https://github.com/deepmodeling/dflow/commit/f0096074d97343ad75646931aec2c3a1f28fb1ab))
* port-forward --address 0.0.0.0 in readme ([f009607](https://github.com/deepmodeling/dflow/commit/f0096074d97343ad75646931aec2c3a1f28fb1ab))
* slice by group in debug mode ([f009607](https://github.com/deepmodeling/dflow/commit/f0096074d97343ad75646931aec2c3a1f28fb1ab))

## [1.5.13](https://github.com/deepmodeling/dflow/compare/v1.5.12...v1.5.13) (2022-10-13)


### Bug Fixes

* support for python3.7 in debug mode ([6d7d971](https://github.com/deepmodeling/dflow/commit/6d7d9714bfec35e694ea401b940f00b555571583))

## [1.5.12](https://github.com/deepmodeling/dflow/compare/v1.5.11...v1.5.12) (2022-10-13)


### Bug Fixes

* optional input artifact in debug mode ([2eb90e8](https://github.com/deepmodeling/dflow/commit/2eb90e879f35f2a3181c8a2add503cc2005405b5))
* use subprocess.Popen in debug mode ([2eb90e8](https://github.com/deepmodeling/dflow/commit/2eb90e879f35f2a3181c8a2add503cc2005405b5))

## [1.5.11](https://github.com/deepmodeling/dflow/compare/v1.5.10...v1.5.11) (2022-10-13)


### Bug Fixes

* a typo ([c93235f](https://github.com/deepmodeling/dflow/commit/c93235f339edebe175897d545e17da1c72ed3840))
* add comments for config and s3_config ([358d4f1](https://github.com/deepmodeling/dflow/commit/358d4f1e262ea422eef34cbe2da3ce0122f199fe))
* add pool_size for slices ([7fe9d8c](https://github.com/deepmodeling/dflow/commit/7fe9d8c1d9d4809dccc18e885f0bbce6af156ccc))
* add utils for find OPs in a package ([358d4f1](https://github.com/deepmodeling/dflow/commit/358d4f1e262ea422eef34cbe2da3ce0122f199fe))
* consistent APIs for default mode and debug mode ([2e8f5cf](https://github.com/deepmodeling/dflow/commit/2e8f5cf74e0f797e4031bdc4deca3e87102415b4))
* format error some lines are too long ([06f4dd5](https://github.com/deepmodeling/dflow/commit/06f4dd56d527390a103816af4542d488ed86780e))
* omit queue_name if it is None in dispatcher executor ([2438c6c](https://github.com/deepmodeling/dflow/commit/2438c6c0a0f416365ed429131dd69125d05696ff))
* pass n_total explicitly in continue_on_success_ratio ([2438c6c](https://github.com/deepmodeling/dflow/commit/2438c6c0a0f416365ed429131dd69125d05696ff))
* raise Exception when a parameter passed to a big parameter, vice versa ([2e8f5cf](https://github.com/deepmodeling/dflow/commit/2e8f5cf74e0f797e4031bdc4deca3e87102415b4))
* save info of Steps/DAG in debug mode ([93784c2](https://github.com/deepmodeling/dflow/commit/93784c23f7d102ea7424d43d6870697645136dd9))
* support for download slices and modify slices in debug mode ([fb1da44](https://github.com/deepmodeling/dflow/commit/fb1da441a131503bcdd0fb3474e409ffebb93a96))

## [1.5.10](https://github.com/deepmodeling/dflow/compare/v1.5.9...v1.5.10) (2022-10-03)


### Bug Fixes

* add reused keys to global outputs ([5ea6b53](https://github.com/deepmodeling/dflow/commit/5ea6b536e8e8326d3b1e7f726e71671602e2c9bf))
* print uid when submitting ([5ea6b53](https://github.com/deepmodeling/dflow/commit/5ea6b536e8e8326d3b1e7f726e71671602e2c9bf))

## [1.5.9](https://github.com/deepmodeling/dflow/compare/v1.5.8...v1.5.9) (2022-10-03)


### Bug Fixes

* add requests to dependencies ([ac2c7f4](https://github.com/deepmodeling/dflow/commit/ac2c7f4a945381f1bc62f2acf555a134aa10503c))
* bug of parallelism for sliced step with sequence ([0729d1e](https://github.com/deepmodeling/dflow/commit/0729d1e398baa9d82dd741b026f5c23e431a292d))

## [1.5.8](https://github.com/deepmodeling/dflow/compare/v1.5.7...v1.5.8) (2022-09-28)


### Bug Fixes

* raise FileNotFoundError when downloading an artifact without key ([615a934](https://github.com/deepmodeling/dflow/commit/615a934b6c40ee5400aa8cc810c30b72dbcde85e))
* replace python command to python3 ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))
* save uid after submission ([c696f1d](https://github.com/deepmodeling/dflow/commit/c696f1dadd52691242c2ac8668c573bbb16358c4))
* some bugs of dispatcher executor ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))
* support for local mode of dispatcher ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))

## [1.5.7](https://github.com/deepmodeling/dflow/compare/v1.5.6...v1.5.7) (2022-09-24)


### Bug Fixes

* add image pull secrets for workflow ([df69681](https://github.com/deepmodeling/dflow/commit/df696817c9df3063a7a1740eefe20dcdde9a2bf4))
* refact FutureLen, add FutureRange ([99730e2](https://github.com/deepmodeling/dflow/commit/99730e2865236c9b6b46682fe629b10834e8d8ae))
* support for skipping same files while downloading ([df69681](https://github.com/deepmodeling/dflow/commit/df696817c9df3063a7a1740eefe20dcdde9a2bf4))

## [1.5.6](https://github.com/deepmodeling/dflow/compare/v1.5.5...v1.5.6) (2022-09-22)


### Bug Fixes

* add methods for OP to get s3 key/link of input/output artifacts ([8a12911](https://github.com/deepmodeling/dflow/commit/8a12911bbc4f451ef0d70345346acbe09430abf2))
* add slurm docker cluster for autotests ([ef60d79](https://github.com/deepmodeling/dflow/commit/ef60d79690db125a94f7c852686b8acb35ae4c0d))

## [1.5.5](https://github.com/deepmodeling/dflow/compare/v1.5.4...v1.5.5) (2022-09-20)


### Bug Fixes

* pass steps/tasks in the initialization of Steps/DAG ([9ef5fd7](https://github.com/deepmodeling/dflow/commit/9ef5fd7a1d1c2c4fd52cc6952d5655020d7760e1))
* prepare_step is None ([9ef5fd7](https://github.com/deepmodeling/dflow/commit/9ef5fd7a1d1c2c4fd52cc6952d5655020d7760e1))

## [1.5.4](https://github.com/deepmodeling/dflow/compare/v1.5.3...v1.5.4) (2022-09-19)


### Bug Fixes

* support parallelism for a parallel step other than sliced step ([d62de45](https://github.com/deepmodeling/dflow/commit/d62de45cc8d21fbd0143fc156fad187ac0bcdf2e))

## [1.5.3](https://github.com/deepmodeling/dflow/compare/v1.5.2...v1.5.3) (2022-09-19)


### Bug Fixes

* support for any private key file name ([ded49c9](https://github.com/deepmodeling/dflow/commit/ded49c9ef06a946d9082680dbced795c07f8ab07))

## [1.5.2](https://github.com/deepmodeling/dflow/compare/v1.5.1...v1.5.2) (2022-09-18)


### Bug Fixes

* add parallelism for sliced step ([e620285](https://github.com/deepmodeling/dflow/commit/e620285ffaaaccf3250b53220fc33714681de69c))
* support dag of sliced tasks in debug mode ([bddfbac](https://github.com/deepmodeling/dflow/commit/bddfbaca77b4dd4cc664726d50feb314c3531e96))
* support debug mode for executor ([2cfc37b](https://github.com/deepmodeling/dflow/commit/2cfc37bd48d3649da8e7f336d4e3a53e6c7223b7))
* use process instead of thread ([2612f6e](https://github.com/deepmodeling/dflow/commit/2612f6eeadb73e3488d82342bd3311d46e6a8c3f))
* wait process by queue.get() ([ff27daf](https://github.com/deepmodeling/dflow/commit/ff27daff693d9643f4eb9402f660c0262a798d82))

## [1.5.1](https://github.com/deepmodeling/dflow/compare/v1.5.0...v1.5.1) (2022-09-16)


### Bug Fixes

* mixed mode for lebesgue executor ([9fe9490](https://github.com/deepmodeling/dflow/commit/9fe9490e7e52bddb361a78aea79c5875ecab8f75))

## [1.5.0](https://github.com/deepmodeling/dflow/compare/v1.4.0...v1.5.0) (2022-09-07)


### Features

* **syntax-sugar:** Offer `with` context syntax for workflow, dag. ([898d4e3](https://github.com/deepmodeling/dflow/commit/898d4e3e509acba3c1dfaa0ce376e54a7a3ab64f))


### Bug Fixes

* can set slices for PythonOPTemplate after initialization ([81a1f43](https://github.com/deepmodeling/dflow/commit/81a1f436c3ff0a394e0bcfdeadbe179185c8ec95))
* pass save_as_aritfact for value_from_expression ([26cb790](https://github.com/deepmodeling/dflow/commit/26cb790bdd0faabf9825427dfa89fa2bcc2023cc))
* remove duplicate export class: S3Artifact ([5f4198d](https://github.com/deepmodeling/dflow/commit/5f4198d18a720dd03f6f1b5685e3da170ee6facb))
* restore availability of download_sliced_output_artifact ([9441971](https://github.com/deepmodeling/dflow/commit/944197122b4aa2de1051442b5a018f3616fce0b5))

## [1.4.0](https://github.com/deepmodeling/dflow/compare/v1.3.1...v1.4.0) (2022-09-04)


### Features

* expose plugin dependencies class and utils for better extensibility ([5a0c349](https://github.com/deepmodeling/dflow/commit/5a0c3498d473a2ec734136bdcab1839e9845a5e1))


### Bug Fixes

* a bug of handling multiple big parameters ([e8c35c1](https://github.com/deepmodeling/dflow/commit/e8c35c134d6196f7db39ff7c45ab7835c2d13628))
* add parallelism to steps and dag ([dc3ec98](https://github.com/deepmodeling/dflow/commit/dc3ec98c1424f52515a41da58a586ffa0555c424))
* avoid double rendering of output parameter which refers to the input parameter of the same template ([96a4e2e](https://github.com/deepmodeling/dflow/commit/96a4e2ef24acfab8977b846c8543f245eb678d94))
* decrease time cost of query keys ([d546ad5](https://github.com/deepmodeling/dflow/commit/d546ad535d3eda29e36494885f49a8ddf1bb6491))

## [1.3.1](https://github.com/deepmodeling/dflow/compare/v1.3.0...v1.3.1) (2022-09-01)


### Bug Fixes

* __getitem__ of ArgoVar handle non-string items ([08f4336](https://github.com/deepmodeling/dflow/commit/08f4336b70fb0ccb514898ef2e4950b4e7cc22bb))
* add method render_script to PythonOPTemplate ([8b2cb4d](https://github.com/deepmodeling/dflow/commit/8b2cb4def78f6ca4d4872c733239a7e7675bb4b5))
* add pod GC strategy ([4b85f90](https://github.com/deepmodeling/dflow/commit/4b85f904fffaee1e226238945320ae84810fc516))
* skip uploading non-existing files in dispatcher executor ([65b7730](https://github.com/deepmodeling/dflow/commit/65b773005b2cc7c569f3cd9b62685fc5dca097ad))

## [1.3.0](https://github.com/deepmodeling/dflow/compare/v1.2.7...v1.3.0) (2022-08-10)


### Features

* **Init Container:** Support init_container in op_template. ([77264b2](https://github.com/deepmodeling/dflow/commit/77264b259c383bcb90ed16e6b425efb649a482dc))
* **Ray Executor:** Ray executor with dependencies settings. ([7695ad8](https://github.com/deepmodeling/dflow/commit/7695ad8e66048769071aab77791b78105d58674d))


### Bug Fixes

* mkdir return 0 and upload failed ([71bd31b](https://github.com/deepmodeling/dflow/commit/71bd31bc19d8aa7ff6e99d8100e96253d2c28676))
* update latest yaml ([be88142](https://github.com/deepmodeling/dflow/commit/be881424320f65c0a8b25ffeb458b5fc3d58ebf2))

## [1.2.7](https://github.com/deepmodeling/dflow/compare/v1.2.6...v1.2.7) (2022-08-07)


### Bug Fixes

* add image_pull_policy for util images ([d08c0ac](https://github.com/deepmodeling/dflow/commit/d08c0ac38ffef7216bbbc7de1c2e07f219fda5bf))

## [1.2.6](https://github.com/deepmodeling/dflow/compare/v1.2.5...v1.2.6) (2022-08-05)


### Bug Fixes

* add sub_path and slice to download_artifact ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))
* data persistent for minio ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))
* handle big parameters of failed step ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))

## [1.2.5](https://github.com/deepmodeling/dflow/compare/v1.2.4...v1.2.5) (2022-08-03)


### Bug Fixes

* add envs to script op template ([8ef468a](https://github.com/deepmodeling/dflow/commit/8ef468ad05fb52889f61e581b523f853191db411))
* add workflow actions: terminate, delete, resubmit, resume, retry, stop, suspend ([8ef468a](https://github.com/deepmodeling/dflow/commit/8ef468ad05fb52889f61e581b523f853191db411))

## [1.2.4](https://github.com/deepmodeling/dflow/compare/v1.2.3...v1.2.4) (2022-08-01)


### Bug Fixes

* add type to query conditions of workflow steps ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* avoid variable expansions in cat EOF ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* decoding error when read source file of OP ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* merge envs in dispatcher executor ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))

## [1.2.3](https://github.com/deepmodeling/dflow/compare/v1.2.2...v1.2.3) (2022-07-27)


### Bug Fixes

* bugs of reuse ([5290065](https://github.com/deepmodeling/dflow/commit/529006511913558b98fe717c2d5f3a13865a6d3a))
* github action of building docker image fails ([e40ac52](https://github.com/deepmodeling/dflow/commit/e40ac521f5a1c18f47f5370d4977d32d9459d20f))

## [1.2.2](https://github.com/deepmodeling/dflow/compare/v1.2.1...v1.2.2) (2022-07-27)


### Bug Fixes

* call ArgoWorkflow.get_step for multiple times ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* change query step by name from fuzzy query to exact query ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* **IfExpression:** transfer to right string. ([aee507f](https://github.com/deepmodeling/dflow/commit/aee507f4ea1e7a3c98074edf5d21e5a8db7a7820))
* RFC 1123 of step name ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* save each reused step into a single configmap to avoid 1MB limit ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* support for query step by a list of conditions ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))

## [1.2.1](https://github.com/deepmodeling/dflow/compare/v1.2.0...v1.2.1) (2022-07-24)


### Bug Fixes

* fix a bug of Path(None) ([0920062](https://github.com/deepmodeling/dflow/commit/09200622f236872f023667c899539cef594bd713))

## [1.2.0](https://github.com/deepmodeling/dflow/compare/v1.1.20...v1.2.0) (2022-07-24)


### Features

* add developer guide ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* add server-side installation script for Linux ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* make ArgoVar uniterable ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))


### Bug Fixes

* cannot pickle types in typing ([691dc57](https://github.com/deepmodeling/dflow/commit/691dc57163844627e89a7e75582141770c194bb4))
* fix a bug of path on Windows ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* handle big parameter whose name changes ([7513567](https://github.com/deepmodeling/dflow/commit/75135676452c4d1b028f4e4fd718e2e586f6a461))
* jsonize parameters before reusing ([7ff8272](https://github.com/deepmodeling/dflow/commit/7ff8272424c2a459bf6ba2165b6e49bde5d2bf22))
* line break in input parameter of Python OP ([ae8d11d](https://github.com/deepmodeling/dflow/commit/ae8d11d48d4dc9bbc9e9e815128e9f68eb752bdd))
* **readme:** fix import typo in README ([55f51a0](https://github.com/deepmodeling/dflow/commit/55f51a0c9f97cb7419ac01bd5e787df89aaa9f41))
* reused step does not have outputs ([8abb67d](https://github.com/deepmodeling/dflow/commit/8abb67dfd1df0de2427f1ed9390fde5529641fc5))
* step.outputs has no exitCode ([b98a635](https://github.com/deepmodeling/dflow/commit/b98a63597575f576af1cfd205019ced226c993b9))
* the conflict between input parameter of Python OP and dispatcher script ([c3981fb](https://github.com/deepmodeling/dflow/commit/c3981fbb191bbbedac20a3d8b7361e601c8307b3))

## 1.1.20

- add cloudpickle to dependencies
- add server-side installation script for Mac
- Lebesgue supports for mixed executor

## 1.1.19

- add container mode (docker, singularity or podman) to slurm remote executor, dispatcher executor and wlm executor.

## 1.1.18

- query archived workflows

## 1.1.17

- fit wlm for new features
- add __getitem__ to ArgoVar

## 1.1.16

- support for defining OP in the interactive mode (e.g. jupyter notebook)

## 1.1.15

- a catalog dir instead of catalog files for artifacts
- a better solution for subpath slices

## 1.1.14

- add save_path_as_artifact to handle large path lists

## 1.1.13

- improve uploading python packages
- compatible for python 3.5

## 1.1.12

- fix format problems
- add test examples
- copy_artifact supports for sort

## 1.1.11

### Added

- support for successive sub-path slices steps
- support for path of output artifact referring to path of input artifact in sub-path slices

## 1.1.10

### Added

- update path list of S3Artifact when used

## 1.1.9

### Added

- add type hints
- add interactive mode for Lebesgue's login
- add maximum parallelism for workflows
- add set_directory and run_command to utils

## 1.1.8

### Added

- modify the interface with Lebesgue

## 1.1.7

### Added

- add Parameter type sign for Python OP
- add username and password authentication for Lebesgue executor
- add global config archive_mode

### Fix

- fix: local config does not take effect for remote OP
- fix: dflow_dflow_private_key_path_list was not supplied

## 1.1.6

### Added

- support for DAG
- add catalog_file_name to configuration
- support for passing OP object instead of a derived class of OP to PythonOPTemplate
- add util_command argument to Step

## 1.1.5

### Added

- save path_list as parameter for each artifact
- add sub_path mode for slices

## 1.1.4

### Added

- add some verifications
- prevent mapping relative path repeatedly in dispatcher
- support for a sliced step outputing a list of paths with a single slice
- fix file processing on Windows
- modify interface of resource requests and limits

## 1.1.3

### Fix

- fix: the conflict between input parameter of Python OP and dispatcher script

## 1.1.2

### Fix

- fix: line break in input parameter of Python OP

## 1.1.1

### Added

- support for parallel step and sliced step

## 1.1.0

### Added

- support for using Python OP without dflow in the image

## 1.0.26

### Added

- fix a typo in io
- dicts override json_file in dispatcher executor
- allow for returning None for output artifact in OP
- work around Argo pod id != step id for slices and slurm job template

## 1.0.25

### Added

- support for id_rsa, id_dsa, id_ecdsa, id_ed25519

## 1.0.24

### Added

- fix \n in code for dispatcher executor
- handle io in slurm remote executor

## 1.0.23

### Added

- work around pod id != step id

### Fix

- fix: parameter has no attribute value

## 1.0.22

### Added

- map /tmp to pwd/tmp in slurm job

## 1.0.21

### Added

- add dispatcher executor
- with_param receives python list

## 1.0.20

### Fix

- fix: jsonize parameters before reusing

## 1.0.19

### Fix

- fix: handle big parameter whose name changes

## 1.0.18

### Added

- use str instead of jsonpickle to store parameter types

## 1.0.17

### Fix

- fix: cannot pickle types in typing

## 1.0.16

### Added

- fix a bug in 1.0.15
- record parameter type in parameter description

## 1.0.15

### Added

- handle big parameters in query step and reuse step

## 1.0.14

### Added

- name of OP template be optional
- add not allowed input artifact path
- not use pvc in slurm remote executor for default

## 1.0.13

### Added

- auto build docs to branch docs
- add retry for all actions in slurm remote executor
- support for use private key on node for SSH connection

## 1.0.12

### Added

- force lowercase for OP template to fix RFC 1123
- add private key option for SSH connection
- add container mode for remote Slurm executor
- add docs

## 1.0.11

### Added

- add global configurations
- modify lebesgue interfaces
- request kubernetes API server with token

## 1.0.10

### Added

- add Lebesgue plugins
- add resources requirements for templates
- support for slurm template for slices

## 1.0.9

### Fix

- fix a bug in sliced step

## 1.0.8

### Added

- handle empty sliced step

## 1.0.7

### Added

- add BigParameter
- retain empty dir
- optimize opening file descriptors

## 1.0.6

### Added

- add SlurmJobTemplate to submit slurm job by using wlm-operator

## 1.0.5

### Added

- download_artifact returns path list
- add convert_to_argo method to Workflow

## 1.0.4

### Added

- add resource template
- add some manifests

## 1.0.3

### Added

- add some arguments to PVC

## 1.0.2

### Fix

- restrict argo-workflows==5.0.0

## 1.0.1

### Added

- add comments doc

## 1.0.0

### Added

- initial release
