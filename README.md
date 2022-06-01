<div id="top"></div>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<!-- <br />
<div align="center">
  <a href="https://github.com/threefoldtech/rmb-rs">
    <img src="images/logo.jpeg" alt="Logo" width="80" height="80">
  </a> -->

<h3 align="center">RMB-RS</h3>

  <p align="center">
    RMB implementation in rust
    <br />
    <a href="https://github.com/threefoldtech/rmb-rs/tree/development/docs"><strong>Explore more docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/threefoldtech/rmb-rs/issues">Report Bug</a>
    ·
    <a href="https://github.com/threefoldtech/rmb-rs/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

RMB is (reliable message bus) is a set of tools (client and daemon) that aims to abstract inter-process communication between multiple processes running over multiple nodes. see the [design document](https://github.com/threefoldtech/rmb-rs/blob/development/docs/readme.md).

<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [Rust](https://www.rust-lang.org/)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

To get a local RMB up and running follow these simple steps.

### Prerequisites

* Rust
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
* Redis

  you can install Redis
  ```sh
  curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
  sudo apt-get update
  sudo apt-get install redis
  ```
  or you can start a redis instance in a container.
  ```sh
  docker run --name test-redis -d -p 6379:6379 redis
  ```
  see [here](https://hub.docker.com/_/redis) for more info
* yggdrasil

  see the [installatio instructions](https://yggdrasil-network.github.io/installation-linux-deb.html)
### Installation

1. Register a twin at [tfchain](https://polkadot.js.org/apps/?rpc=wss://tfchain.grid.tf/ws#/accounts).
2. make sure your yggdrasil service is up and that your ygg address was added to your twin either from [polkadot.js UI](https://polkadot.js.org/apps/?rpc=wss://tfchain.grid.tf/ws#/extrinsics) or from the [grid portal](https://portal.grid.tf/).
3. Clone the repo.
   ```sh
   git clone https://github.com/threefoldtech/rmb-rs.git
   ```
3. Build and install the Daemon binary.
   ```sh
   cargo install --path .
   ```

in case of any build issues, things to try:
- switch to the stable channel if you are on a nightly channel.

  ```sh
  rustup default stable
  ```
 
- rustup can be used to update the installed version to the latest release.

  ```sh
  rustup update
  ```

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

Example of using sr25519 key on dev network

```sh
rmb-rs --key-type sr25519 -s "wss://tfchain.dev.grid.tf" -m "<YOUR-MNEMONICS>"
```

**Debug logs** can be enabled by `-d` option.

use `-h` for **help**.

```sh
rmb-rs -h
```

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

- [ ] implement proxy feature according to specs
- [ ] Build integration tests

See the [open issues](https://github.com/github_username/repo_name/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the Apache License. See `LICENSE` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

THREEFOLD - [@threefold_io](https://twitter.com/threefold_io)

Project Link: [https://github.com/threefoldtech/rmb-rs](https://github.com/threefoldtech/rmb-rs)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [TO BE ADDED]()

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/threefoldtech/rmb-rs.svg?style=for-the-badge
[contributors-url]: https://github.com/threefoldtech/rmb-rs/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/threefoldtech/rmb-rs.svg?style=for-the-badge
[forks-url]: https://github.com/threefoldtech/rmb-rs/network/members
[stars-shield]: https://img.shields.io/github/stars/threefoldtech/rmb-rs.svg?style=for-the-badge
[stars-url]: https://github.com/threefoldtech/rmb-rs/stargazers
[issues-shield]: https://img.shields.io/github/issues/threefoldtech/rmb-rs.svg?style=for-the-badge
[issues-url]: https://github.com/threefoldtech/rmb-rs/issues
[license-shield]: https://img.shields.io/github/license/threefoldtech/rmb-rs.svg?style=for-the-badge
[license-url]: https://github.com/threefoldtech/rmb-rs/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/company/threefold-tech/
[product-screenshot]: images/screenshot.png
