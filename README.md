Sincronia
=========

Sincronia is an algorithm for coflow scheduling. The paper is [here](https://github.com/sincronia-coflow/tech-report).

This is an implementation of sincronia using RPCs over TCP, with DiffServ for packet prioritization.

The `scheduler` directory contains the source for a binary implementing a central sincronia scehduler.
This communicates with applications via the shim library, in the `client` directory.

An example application, which generates traffic from a predefined network trace, is in the `app` directory.
