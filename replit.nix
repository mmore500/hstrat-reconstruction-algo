{ pkgs }: {
    deps = [
      pkgs.htop
      pkgs.python311Packages.pytest
      pkgs.python310Packages.pytest
      pkgs.vim
      pkgs.pandoc
      pkgs.texlive.combined.scheme-full
      pkgs.python39Packages.pip
      pkgs.editorconfig-checker
      pkgs.python39Packages.editorconfig
      pkgs.cowsay
      pkgs.bibtex-tidy
    ];
}
