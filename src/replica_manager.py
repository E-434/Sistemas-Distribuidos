#!/usr/bin/env python3
"""Entrypoint legado; o modo unificado está em servidor.py."""

import sys


def main() -> None:
    print(
        "Este entrypoint foi incorporado a src/servidor.py. "
        "Inicie todos os nós com: python3 src/servidor.py <porta>",
        file=sys.stderr,
    )
    raise SystemExit(2)


if __name__ == "__main__":
    main()
