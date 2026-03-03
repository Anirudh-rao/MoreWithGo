# Runners

### Overview

Apache Beam provides a portable API layer for building sophisticated data-parallel processing `pipelines` that may be executed across a diversity of execution engines, or `runners`.

### Direct Runners:

The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model.Some of these checks include:

1. Enforcing immutablity of elements
2. Enforcing encodablity of elements
3. Elements are procesed in an arbitaray order at all points.
4. Serialization of user functions

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.
In Go SDK, the defauly is runner `DirectRunner`.


