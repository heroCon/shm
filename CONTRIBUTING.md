# Contributing

Build both Debug and Release and run `ctest --test-dir build --output-on-failure`. Every concurrency fix should include a bounded-time regression test using a unique shared-memory name and explicit cleanup. Document changes to delivery or recovery semantics and avoid claims stronger than the tested platform guarantees. Submit focused commits and include reproduction steps.
