# Rust leveldb bindings

Almost-complete bindings for leveldb for Rust.

[Documentation](https://docs.rs/leveldb/0.8.4/leveldb/)

## Rust version policy

`leveldb` is built and tested on stable releases of Rust. Latest tested version: 1.80.1

## Dependencies

- LevelDB: master branch, commit id: 23e35d7
- Snappy: v1.2.1

## Usage

If your project is using [Cargo](http://crates.io), drop the following lines in your `Cargo.toml`:

```text
[dependencies]

leveldb-rs-binding = "0.8"
```

## Development

Make sure you have all prerequisites installed. Run

```sh
$ cargo build
```

for building and

```sh
$ cargo test
```

to run the test suite.

## Examples

```rust
extern crate tempdir;
extern crate leveldb;

use tempdir::TempDir;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::{Options,WriteOptions,ReadOptions};

fn main() {
  let tempdir = TempDir::new("demo").unwrap();
  let path = tempdir.path();

  let mut options = Options::new();
  options.create_if_missing = true;
  let mut database = match Database::open(path, options) {
      Ok(db) => { db },
      Err(e) => { panic!("failed to open database: {:?}", e) }
  };

  let write_opts = WriteOptions::new();
  match database.put(write_opts, 1, &[1]) {
      Ok(_) => { () },
      Err(e) => { panic!("failed to write to database: {:?}", e) }
  };

  let read_opts = ReadOptions::new();
  let res = database.get(read_opts, 1);

  match res {
    Ok(data) => {
      assert!(data.is_some());
      assert_eq!(data, Some(vec![1]));
    }
    Err(e) => { panic!("failed reading data: {:?}", e) }
  }

  let read_opts = ReadOptions::new();
  let mut iter = database.iter(read_opts);
  let entry = iter.next();
  assert_eq!(
    entry,
    Some((1, vec![1]))
  );
}
```

## Open issues

* Filter policies are missing
* Iterators with arbirary start and end points are unsupported

# License

MIT, see `LICENSE`
