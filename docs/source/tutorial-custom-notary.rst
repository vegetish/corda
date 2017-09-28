.. highlight:: kotlin

Writing a custom notary service
===============================

.. warning:: Customising a notary service is an advanced feature and not recommended for most use-cases. Currently,
   customising Raft or BFT notaries is not yet fully supported. If you want to write your own Raft notary you will have to
   implement a custom database connector (or use a separate database for the notary), and use a custom configuration file.

Similarly to writing an oracle service, the first step is to create a service class in your CorDapp and annotate it
with ``@CordaService``. The Corda node scans for any class with this annotation and initialises them.
The requirements for a custom notary service are:

   - Provide a constructor with two parameters of types ``ServiceHub`` and ``PublicKey``.
   - Provide a static field ``type``, which denotes the notary service type.

.. literalinclude:: ../../samples/notary-demo/src/main/kotlin/net/corda/notarydemo/MyCustomNotaryService.kt
   :language: kotlin
   :start-after: START 1
   :end-before: END 1

The next step is to write a notary service flow. You are free to copy and modify the existing built-in flows such
as ``ValidatingNotaryFlow``, ``NonValidatingNotaryFlow``, or implement your own from scratch (following the
``NotaryFlow.Service`` template). Below is an example of a custom flow for a *validating* notary service:

.. literalinclude:: ../../samples/notary-demo/src/main/kotlin/net/corda/notarydemo/MyCustomNotaryService.kt
   :language: kotlin
   :start-after: START 2
   :end-before: END 2

To ensure the custom notary is installed and started by the node, specify the ``type`` value in the configuration file:

.. parsed-literal::

    extraAdvertisedServiceIds : ["corda.notary.validating.custom"]
