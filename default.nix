# nixos-unstable. Find the current hash at https://status.nixos.org/.
# Check https://nodejs.org/en/about/releases/ for the current npm release.
{ nixpkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/d01cb18be494e3d860fcfe6be4ad63614360333c.tar.gz") {config.allowUnfree = true;}
}:

let
  pkgs = [
    nixpkgs.docker
    nixpkgs.docker-compose
    nixpkgs.git
    nixpkgs.nodejs-19_x
    nixpkgs.openssh
    nixpkgs.which
  ];

in
  nixpkgs.stdenv.mkDerivation {
    name = "crdt-experiments";
    buildInputs = pkgs;
  }
