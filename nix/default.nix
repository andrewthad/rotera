{ nixpkgs   ? import ./nixpkgs.nix # nix package set we're using
, profiling ? false # Whether or not to enable library profiling (applies to internal deps only)
, haddocks  ? false # Whether or not to enable haddock building (applies to internal deps only)
}:

with rec {
  compiler = "ghc865";

  overlay = import ./overlay.nix { inherit profiling haddocks; };

  pkgs = import nixpkgs {
    config = {
      allowUnfree = true;
      allowBroken = false;
    };
    overlays = [ overlay ];
  };

  make = name: pkgs.haskell.packages.${compiler}.${name};

  rotera = make "rotera";
};

rec {
  inherit pkgs;
  inherit nixpkgs;
  inherit rotera;
}
