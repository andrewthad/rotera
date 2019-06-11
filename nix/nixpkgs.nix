let
  fetchNixpkgs = import ./fetchNixpkgs.nix;

  nixpkgs = fetchNixpkgs {
    owner = "NixOS";
    repo = "nixpkgs";
    rev = "f6fc49d0bfd177d84d64c15bada936a64e724de0";
    sha256 = "0kkyw7kszg0nvnfq50wgp81ix6vh562h2prfbv7xhhkazdz6zyhs";
  };

in
  nixpkgs
