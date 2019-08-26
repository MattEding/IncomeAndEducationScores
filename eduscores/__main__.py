import argparse

from eduscores import webscrape


parser = argparse.ArgumentParser()
parser.add_argument('-l', '--loglevel', default='WARNING')

parser.add_argument('--sbac', action='store_true')
parser.add_argument('-y', '--years', nargs='*', type=int, default=[2018])
parser.add_argument('-d', '--download', action='store_true')
parser.add_argument('-p', '--parse', action='store_true')

parser.add_argument('--zipcode', action='store_true')
parser.add_argument('-s', '--seconds', type=int, default=3.0)

args = parser.parse_args()

if args.sbac:
    webscrape.sbac.main(
        years=args.years,
        download=args.download,
        parse=args.parse,
        loglevel=args.loglevel,
    )

if args.zipcode:
    webscrape.zipcode.main(
        seconds=args.seconds,
        loglevel=args.loglevel,
    )
