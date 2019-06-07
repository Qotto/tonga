Command
=======


A *command* is a record containing instructions to be processed by a service. The result of this processing is notified in a *result* record.

Commands, whenever possible, should be idempotent_. It means that a command applied twice should have the same effect as a command applied once.

Idempotence is not always possible. For instance, sending an e-mail cannot be idempotent: sending an e-mail once is
not the same as sending it twice. Even if we try to deduplicate commands, there is no way, with usual SMTP servers,
to send an e-mail and acknowledge the command in the same transaction.

Commands have the same fields as records, and a few additional ones:

processing_guarantee: one of
   - *at_least_once*: Should be processed at least once. It is tolerated, in case of a failure, for it to be processed multiple times.
   - *at_most_once*: Should be processed at most once. It is tolerated, in case of a failure, for it to not be processed at all.
   - *exactly_once*: Should be processed exactly once. It is not tolerated, in case of a failure, for it to be processed more or less than once.

.. _idempotent:
   https://en.wikipedia.org/wiki/Idempotence

Command class
-----------------------------------------

.. automodule:: tonga.models.events.command.command
    :members:
    :undoc-members:

Command errors
----------------------------------

.. automodule:: tonga.models.events.command.errors
    :members:
    :undoc-members:
    :show-inheritance:
