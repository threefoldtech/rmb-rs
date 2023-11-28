# Coding guides

> This document will always be `work in progress` and will get updated every time something comes up

This documents will maintain the coding guide for this project. We will try to keep it up-to-date with what we find more useful and cleaner.

## module structure

in rust there are multiple ways to create a (sub)module in your crate

- `<module>.rs`  A single file module. can be imported in `main.rs` or `lib.rs` with the keyword `mod`
- `<module>/mod.rs` A directory module. Uses mod.rs as the module `entrypoint` always. Other sub-modules can be created next to mod.rs and can be made available by using the `mod` keyword again in the `mod.rs` file.

We will agree to use the 2nd way (directory) but with the following restrictions:

- `mod.rs` will have all `traits` and `concrete types` used by the traits.
- `<implementation>.rs` file next to `mod.rs` that can include implementation for the module trait.
z

### Example

Following is an example of `animal` module.

```
animal/
   mod.rs
   dog.rs
   cat.rs
```

> File names are always in `snake_case` but avoid the `_` as much as possible because they basically look ugly in file tree. For example we prefer the name `dog.rs` over `dog_animal.rs` because we already can tell from the module name that it's a `dog` __animal__. Hence in the identity module for example the name `ed25519.rs` is preferred over `ed25519_identity.rs` because that's already inferred from the module name.

The `mod.rs` file then can contain

```rust
pub mod dog;
pub mod cat;
pub use dog::Dog;

pub trait Food {
    fn calories(&self) -> u32;
}

pub trait Animal<F>
where
    F: Food,
{
    fn feed(&mut self, food: F);
}

```

The `dog.rs` file then can contain

```rust
use super::{Animal, Food};

pub struct DogFood {}
impl Food for DogFood {
    fn calories(&self) -> u32 {
        1000
    }
}
pub struct Dog {}

impl Animal<DogFood> for Dog {
    fn feed(&mut self, food: DogFood) {
        println!("yum yum yum {} calories", food.calories());
    }
}

```

A user of the module now can do

```
use animal::dog::{Dog, DogFood};
```

For common implementation that are usually used in your modules, a `pub use` can be added in `mod.rs` to make it easier to import your type. For example

```rust
// dog is brought directly from animal crate
use animal::Dog;
// cat i need to get from the sub-module
use animal::cat::Cat;
```

## naming conventions

following the rust guide lines for name

- `file names` are short snake case. avoid `_` if otherwise name will not be descriptive. Check note about file names above.
- `trait`, `struct`, `enum` names are all `CamelCase`
- `fn`, `variables` names are snake case

Note, names of functions and variables need to be `descriptive` but __short__ at the same time. Also avoid the `_` until absolutely necessary. A variable with a single `word` name is better if it doesn't cause confusion with other variables in the same context.

The name of the variable should never include the `type`.

## `error` Handling

We agreed to use `anyhow` crate in this project. Please read the docs for [`anyhow`](https://docs.rs/anyhow/1.0.57/anyhow/)

To unify the practice by default we import both `Result` and `Context` from `anyhow`
> Others can be imported as well if needed.

```rust
use anyhow::{Result, Context};

fn might_fail() -> Result<()> {
    // context adds a `context` to the error. so if another_call fails. I can tell exactly failed when i was doing what
    another_call().context("failed to do something")?; // <- we use ? to propagate the error unless you need to handle the error differently

    Ok(()) // we use Ok from std no need to import anyhow::Ok although it's probably the same.
}

fn might_fail2() -> Result<()> {
    if fail {
        // use the bail macro fom anyhow to exit with an error.
        bail!("failed because fail with set to true");
    }
}

> NOTE: all error messages starts with lowercase. for example it's `failed to ...` not `Failed to ...`
```

## `logs`

logging is important to trace the errors that cannot be propagated and also for debug messages that can help spotting a problem. We always gonna use `log` crate. as

```rust
log::debug!(); // for debug messages
log::info!(); // info messages
```

Note only `errors` that can __NOT__ be propagated are logged.

> NOTE: All log messages start with lowercase.
>
## function signatures

For function inputs (arguments) `generic` types are preferred if available over concrete types. This most obvious with `string` types. depending on the function behavior

### Examples

This is bad

```rust
fn call1(key: String);
fn call2(key: &str);
```

and preferred to use

```rust
// in case function will need to take ownership of the string.
fn call1<k: Into<String>>(k: K);
// inc ase function will just need to use a reference to the string.
fn call2<K: AsRef<str>>(k: K);

// this will allow both functions to be callable with `&str`, `String`.
```
