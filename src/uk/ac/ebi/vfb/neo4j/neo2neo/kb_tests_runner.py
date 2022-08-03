import argparse
from uk.ac.ebi.vfb.neo4j.neo2neo.kb_tests import test_kb


def main():
    parser = argparse.ArgumentParser(prog="kb_test", description='VFB KB consistency tester cli interface.')
    parser.add_argument('-k', '--kb', action='store', required=True, help='KB server URI')
    parser.add_argument('-u', '--user', action='store', required=True, help='KB server user')
    parser.add_argument('-p', '--password', action='store', required=True, help='KB server password')
    parser.add_argument('-s', '--silent', action='store_true', help='Activates silent mode that prevents abnormal exit.')
    parser.add_argument('-d', '--dataset', action='append', nargs='*', help='Short form of the dataset to test.')

    args = parser.parse_args()

    datasets = list()
    if 'dataset' in args and args.dataset:
        # handle both '-d x -d y' and '-d x y'
        datasets = [item for sublist in args.dataset for item in sublist]

    test_kb(args.kb, args.user, args.password, datasets, args.silent)


if __name__ == "__main__":
    main()