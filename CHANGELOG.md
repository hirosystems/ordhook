#### 1.4.0 (2023-01-23)

##### New Features

*  Polish LSP completion capability ([4cc24ed3](https://github.com/hirosystems/clarinet/commit/4cc24ed3c5edaf61d057c4c1e1ab3d32957e6a15), [16db8dd4](https://github.com/hirosystems/clarinet/commit/16db8dd454ddc5acaec1161ef4aba26cba4c37bf), [905e5433](https://github.com/hirosystems/clarinet/commit/905e5433cc7bf208ea480cc148865e8198bb0420), [9ffdad0f](https://github.com/hirosystems/clarinet/commit/9ffdad0f46294dd36c83ab92c3241b2b01499576), [d3a27933](https://github.com/hirosystems/clarinet/commit/d3a2793350e96ad224f038b11a6ada602fef46af), [cad54358](https://github.com/hirosystems/clarinet/commit/cad54358a1978ab4953aca9e0f3a6ff52ac3afc4), [439c4933](https://github.com/hirosystems/clarinet/commit/439c4933bcbeaaec9f3413892bbcc12fc8ec1b15))
*  Upgrade clarity vm ([fefdd1e0](https://github.com/hirosystems/clarinet/commit/fefdd1e092dad8e546e2db7683202d81dd91407a))
*  Upgrade stacks-node next image ([492804bb](https://github.com/hirosystems/clarinet/commit/492804bb472a950dded1b1d0c8a951b434a141ac))
*  Expose stacks-node settings wait_time_for_microblocks, first_attempt_time_ms, subsequent_attempt_time_ms in Devnet config file
*  Improve Epoch 2.1 deployments handling
*  Improve `stacks-devnet-js` stability

##### Documentation

*  Updated documentation to set clarity version of contract ([b124d96f](https://github.com/hirosystems/clarinet/commit/b124d96fbbef29befc26601cdbd8ed521d4a162a))


# [1.3.1](https://github.com/hirosystems/clarinet/compare/v1.3.0...v1.3.1) (2023-01-03)

### New Features

*  Introduce use_docker_gateway_routing setting for CI environments
*  Improve signature help in LSP ([eee03cff](https://github.com/hirosystems/clarinet/commit/eee03cff757d3e288abe7436eca06d4c440c71dc))
*  Add support for more keyword help in REPL ([f564d469](https://github.com/hirosystems/clarinet/commit/f564d469ccf5e79ab924643627fdda8715da6a1d, [0efcc75e](https://github.com/hirosystems/clarinet/commit/0efcc75e7da3b801e1a862094791f3747452f9e0))
*  Various Docker management optimizations / fixes ([b379d29f](https://github.com/hirosystems/clarinet/commit/b379d29f4ad4e85df42e804bc00cec2baff375c0), [4f4c8806](https://github.com/hirosystems/clarinet/commit/4f4c88064e2045de9e48d75b507dd321d4543046))

### Bug Fixes

*  Fix STX assets title ([fdc748e7](https://github.com/hirosystems/clarinet/commit/fdc748e7b7df6ef1a6b62ab5cb8c1b68bde9b1ad), [ce5d107c](https://github.com/hirosystems/clarinet/commit/ce5d107c76950d989eb0be8283adf35930283f18))
*  Fix define function grammar ([d02835ba](https://github.com/hirosystems/clarinet/commit/d02835bab06578eebb13a791f9faa1c2571d3fb9))
*  Fix get_costs panicking ([822d8e29](https://github.com/hirosystems/clarinet/commit/822d8e29965e11864f708a1efd7a8ad385bc1ba3), [e41ae715](https://github.com/hirosystems/clarinet/commit/e41ae71585a432d21cc16c109d2858f9e1d8e22b))
