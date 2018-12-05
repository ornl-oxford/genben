from genomics_benchmarks import cli
import sys


def main():
    try:
        cli._main()
    except KeyboardInterrupt:
        print("Program interrupted. Exiting...")
        sys.exit(1)
