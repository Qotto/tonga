Result
======

A *result* is a record containing results of the processing of a command.

Results should be sent to the **coffeemaker‑results** topic.

Results should use the *tonga.coffeemaker.results* namespace.

Commands have the same fields as records, and a few additional ones:

- *error*: an optional field describing the error that occurred during the command processing. If this field is empty, it means that the command was successfully completed. The *error* field has two specific sub-fields:
    - *label*: a short label describing the error (a string without any white space)
    - *message*: a human-readable description of the error

.. automodule:: tonga.models.events.result.result
    :members:
    :undoc-members:
    :show-inheritance:
