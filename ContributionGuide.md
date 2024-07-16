# Contribution Guide

## Code Style

- We take advantage of autoformatting for all of our C++ code. Please be sure to use the `clang-format` file to properly format your code.

- Major user-facing functions/methods should be written in PascalCase. This signals to the end-user that this is an important action that we indeded them to use.

- Minor functions that are not intended to be user-facing should be written in snake_case. This will help further visually differentiate what functions the user were not intended to be interacting with.

- Variables should be written in camelCase. This will help maintainers quickly identify a variable and help make documentation cleaner.

- Internal constants should be written in SHOUTING_CASE. This is different from a variable given the `const` identifier which should conform to the above variable style. Internal constants should be static defined.

- External constants should be written as a kExternalConstant. Again, this provides a unique, easily identifiable, and clean looking way for the end-user to interact with the product.

- API documentation should be written for any functions or classes using the `/** */` Doxygen style and contain an `@brief`, a short explination of every `@param` and a `@return`. Including a longer description below the `@brief` and any `@tparam` is recommended but not required.

- Comments inside code are encouraged if something is not clear and should use the `//` style.

## Branching

Please branch off of the highest number version branch. This will reduce duplicate code and issues integrating changes back into the project. Versions should only get bumped on release, so adding new features within the project scope is completely acceptable.

### Branching considerations

1. Keep the scope as narrow as possible. Please don't implement multiple fixes or features in one branch.
2. Please follow the standard of relevant, concise branch names.
3. Please also indicate the parent branch in the name. For example `vX.Y.Z-feature_name`. This will help quickly identify whether a branch is stale or if it should be merged into the branch.

## Merges

When submitting a pull request, please ensure the following:

- All of the tests are passing. There is an extensive battery of tests that help make sure changes don't inadvertantly break support for existing features.
- The code style has been followed. You can run `clang-format` to ensure this. Not only does it provide a consistent look in the code but it also helps reduce merge conflicts.
- There is a detailed list of what changed. This can be done either with frequent commits with concise descriptions or a list at the end of a large commit. Please refrain from something like "bug fixes" as the only message.
- The feature branch should be set to delete on merge. This will help reduce clutter of excess branches.

## Tests

This project makes extensive use of the gtest utility for all of the code. This serves as both an insurance against breaking features as the software evolves and also a living document for examples on how to interact with the API.

Writing a test for every function is not required, but testing every function is required. What this means is that if there is a helper function that you have written that is only ever intended to be called by another function, a test is not required. You are strongly encouraged to extensively test the helper function however.

The acceptance test is the main test for MDIO. It is not intended to be exhaustive, but to demonstrate that all major features are working and to provide a single place to view all the pieces of the API working together.

### When a gtest is required

- A new file requires a new gtest suite.
- A new file should be added as a new test suite to the acceptance test if it is intended to be user-facing. If it is not user-facing it is acceptable to be added to the acceptance test but its own gtest suite will suffice.
- A user-facing function requires a gtest. Please be as through as possible.
- A user-facing function should also be added to the acceptance test.

Remember, your contributions are valuable and help improve the project. Thank you for your efforts!
