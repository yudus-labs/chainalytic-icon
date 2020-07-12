import argparse
import sys
import time

from chainalytic_icon.cli import Console

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-a', '--aggregator', help='Aggregator endpoint',
    )
    parser.add_argument(
        '-p', '--provider', help='Provider endpoint',
    )
    parser.add_argument('-i', '--init-config', action='store_true', help='Generate user config')
    parser.add_argument('--restart', action='store_true', help='Force restart all running services')
    parser.add_argument('--keep-running', action='store_true', help='Prevent console from exiting')

    subparsers = parser.add_subparsers(dest='command', help='Sub commands')
    stop_parser = subparsers.add_parser('stop', help='Kill running Chainalytic services')

    monitor_parser = subparsers.add_parser('m', help='Monitor all or some specific transform')
    monitor_parser.add_argument(
        'transform_id',
        nargs='?',
        default=None,
        help='Transform ID. Skip to monitor all transforms',
    )
    monitor_parser.add_argument(
        '-r', '--refresh-time', default='1', help='Refresh time of aggregation monitor'
    )

    args = parser.parse_args()
    console = Console(aggregator_endpoint=args.aggregator, provider_endpoint=args.provider)

    try:
        if args.command == 'stop':
            console.stop_services()
        elif args.command == 'm':
            console.monitor(
                args.transform_id, float(args.refresh_time),
            )
        elif args.init_config:
            console.init_config()
        else:
            console.init_services(force_restart=args.restart)
            if args.keep_running:
                while 1:
                    time.sleep(999)
    except KeyboardInterrupt:
        print('Exited Chainalytic Console')
        sys.exit()
