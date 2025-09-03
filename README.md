# ph

Database internals experiments

## Current target architecture

Online part:

- In-memory bimap
- WAL

Checkpointed part:

- Data file: blocks, each have size and either allocated or free
- Data deduplication file: Self-balancing BST with allocated data blocks pointers sorted by data. BST nodes are of constant size, so free space tracking done using linked list (LLA)
- Data file free space file: Self-balancing BST with free data blocks pointers sorted by size. LLA
- Records file: key and value data file blocks pointers pairs. LLA
- Sorted-by-keys file: Self-balancing BST with records pointers sorted by key. LLA
- Sorted-by-values file: Self-balancing BST with records pointers sorted by value. LLA
- Undo file: file with parts of files mutated during checkpoint. Removed when checkpointing finished

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     ph:
       github: mentalblood0/ph
   ```

2. Run `shards install`

## Usage

See [spec/ph_spec.cr](./spec/ph_spec.cr)
