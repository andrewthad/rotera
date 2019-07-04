# rotera
A persistent rotating queue.

## About
Rotera is a high-performance, persisted rotating queue written in GHC Haskell. It is a research project, not (yet)
meant to be used in production.

There are a number of components to the project:

* `rotera` (library): The library defining the core types and functions around which all of the executables are built.
* `rotera` (executable): Used to create `rot` files, the file format rotera uses to back its data (using mmap).
                       It is strongly recommended to create the rot files using the `rotera` executable and not
                       by hand.
                       <i>Note</i>: Currently, this also has the limited capabilities of `rotera-client` as well, but this
                       functionality is deprecated and will be removed.
* `rotera-server` (executable): The server that manages rotera's queues and incoming requests.
* `rotera-client` (executable): A command line client for communicating with a rotera server.
                                This is mostly for debugging and shouldn't be used as a way to communicate
                                with rotera through code.
* `rotera-repl` (executable): A repl for communicating with a rotera-server. This is more convenient than
                              `rotera-client` if you need to issue multiple commands, though it doesn't support
                              streaming (which will be discussed below).

These will be discussed in further detail below.

## Motivation
Rotera was born out of curiousity, as well as frustration with Apache Kafka. Apache Kafka provides strong guarantees about
message passing, failover, persistence, consumer groups, etc. in a distributed system. These strong guarantees induce
a large API surface, which is difficult to understand and manage (so difficult to manage, in fact, that many companies
pay for individuals/entities to perform only the task of managing their Kafka setup). Rotera differs from Kafka in the
following ways:

| Attribute          | Rotera                | Kafka                                                                                                                                                     |
|--------------------|-----------------------|------------------------------|
| Delivery           | At least once         | Exactly once                 |
| Distributed?       | Single-Node only      | Distributed                  |
| Ordering           | In order              | In order                     |
| Multiple queues    | Yes                   | Yes                          |
| Failover           | N/A                   | Re-distributes load          |
| Consumer Groups?   | None                  | Exist                        |
| Persistence        | Keeps last n messages | Keeps messages based on time |
| finish this table  | finish this table     | finish this table            |

We believe that eventually rotera will be preferable to Apache Kafka if a single node is sufficient to handle your
throughput, i.e. your throughput does not warrant the need for a distributed system, because of Rotera's significantly
smaller API (functions, configuration).

## API

Rotera's API is made up of 5 functions, and a few simple configuration options.

### Functions

* `ping`: ping the server (simple health check).
* `commit`: ping the server, commit the most recent message id.
            note that the repl always commits after a push.
* `read`: read a batch of messages out of a queue.
* `push` : push a batch of messages to a queue (does not necessarily make a commit).
           note that the repl always commits after a push.
* `stream` : stream chunks of messages from a stream socket until a certain number of messages has been reached.
             note that this is not supported by the repl.
  
### Configuration
TODO

## Building

<i>Note</i>: Rotera should only work on Linux right now. Adding support for other operating systems is welcome, though we expect
Windows support to be non-trivial.

### Cabal

#### v2
```
cabal new-update
cabal new-build
```

#### v3
```
cabal update
cabal build
```

### Nix
```
#install cachix, if not already installed
nix-env -iA cachix -f https://cachix.org/api/v1/install

#use layer 3's cachix
cachix use layer-3-cachix

nix-build
```

## Installing/Running

### Cabal
First, build and install all components. Then, do the following:

```
# create a directory where rotera will look for rot files
mkdir rots

# create a queue, backed by a rot file, that fill be referred to by its identifier '55'
rotera --create -s rots/55.rot

# start the server
cd rots
rotera-server
```

This is enough to actually get up and running with rotera. To play around with it, we recommend to next run
the repl with `rotera-repl`, then type `:help` and read the options available to you.

### NixOS
On NixOS, copy the following service and import it into your nixos configuration: [service](https://github.com/chessai/nixos-configs/blob/master/services/rotera.nix)

It will run as a systemd service, and will also automatically set up your rot files.

You can use it like this:

```
config = {
  services.rotera = {
    enable = true;
    rotFiles = [ 13 17 ]; # have queues named '13' and '17'
    # other options here
    ...
  };
}
```

There are other options, documented within the service file. They can also be inspected via nix repl.

## Similar projects
<TODO: link to older java project here>
