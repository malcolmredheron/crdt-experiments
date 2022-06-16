# nixos-unstable. Find the current hash at https://status.nixos.org/.
{ nixpkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/29769d2a1390d294469bcc6518f17931953545e1.tar.gz") {config.allowUnfree = true;}
}:

let
  pkgs = [
    nixpkgs.docker
    nixpkgs.docker-compose
    nixpkgs.git
    nixpkgs.nodejs
    nixpkgs.openssh
    nixpkgs.which
  ];

in
  nixpkgs.stdenv.mkDerivation {
    name = "crdt-experiments";
    buildInputs = pkgs;
  }
