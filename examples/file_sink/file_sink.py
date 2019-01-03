import json
import logging
import pathlib
from typing import List, cast

from confluent_kafka.cimpl import Message

from pyconnect import PyConnectSink, SinkConfig
from pyconnect.core import Status, message_repr

logger = logging.getLogger(__name__)


class FileSinkConfig(SinkConfig):
    """
    In addition to the fields from :class:`pyconnect.config.SinkConfig` this class provides the following fields:

        **sink_directory**: :class:`pathlib.Path`
            The directory where this sink shall put the file it writes all messages to.

        **sink_filename**: str
            The name of the file that this sink writes all messages to.
    """
    __parsers = {'sink_directory': lambda p: pathlib.Path(p).absolute()}

    def __init__(self, conf_dict):
        conf_dict = conf_dict.copy()
        self['sink_directory'] = conf_dict.pop('sink_directory')
        self['sink_filename'] = conf_dict.pop('sink_filename')
        super().__init__(conf_dict)
        logger.debug(f'Configuration: {self!r}')


class FileSink(PyConnectSink):
    """
    A sink that writes all messages it receives to a single file.
    """

    def __init__(self, config: FileSinkConfig):
        super().__init__(config)
        self._buffer: List[Message] = []

    def on_message_received(self, msg: Message) -> None:
        logger.debug(f'Message Received: {message_repr(msg)}')
        self._buffer.append(msg)

    def on_startup(self):
        logger.debug(f'Creating parent directory: {self.config["sink_directory"]}')
        cast(pathlib.Path, self.config['sink_directory']).mkdir(parents=True, exist_ok=True)

    def on_flush(self) -> None:
        lines = [
            json.dumps({'key': msg.key(), 'value': msg.value()}) + '\n'
            for msg in self._buffer
        ]
        sinkfile = self.config['sink_directory'] / self.config['sink_filename']
        logger.info(f'Writing {len(lines)} line(s) to {sinkfile}')
        with open(sinkfile, 'a') as outfile:
            outfile.writelines(lines)

        logger.debug('The following lines were written:')
        for line in lines:
            logger.debug(f'> {line!r}')

        self._buffer.clear()

    def on_no_message_received(self):
        if self.has_partition_assignments and self.all_partitions_at_eof:
            logger.info('EOF reached, stopping.')
            return Status.STOPPED
        return None


def main():
    # TODO move to pyconnect.core.main(connector_cls, config_cls)
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', choices=['env', 'yaml', 'json'], default='env', help='Defines where the config '
                                                                                         'is loaded from')
    parser.add_argument('--conf_file', default=None, help='When `conf` is yaml or json, then config is loaded'
                                                          'from this file, default will be `./config.(yaml|json)` '
                                                          'depending on which kind of file you chose')
    parser.add_argument('--loglevel', choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set log level to given value, if "NOTSET" (default) no logging is active.',
                        default='NOTSET')

    args = parser.parse_args()
    config: FileSinkConfig = None

    if args.loglevel != 'NOTSET':
        base_logger = logging.getLogger()
        loglevel = getattr(logging, args.loglevel)

        formatter = logging.Formatter('%(levelname)-8s - %(name)-12s - %(message)s')

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(loglevel)
        stream_handler.setFormatter(formatter)

        base_logger.setLevel(loglevel)
        base_logger.addHandler(stream_handler)

    if args.config == 'env':
        config = FileSinkConfig.from_env_variables()
    elif args.config == 'yaml':
        config = FileSinkConfig.from_yaml_file(args.conf_file or ('./config.' + args.config))
    elif args.config == 'json':
        config = FileSinkConfig.from_json_file(args.conf_file or ('./config.' + args.config))
    else:
        print('Illegal Argument for --config!')
        parser.print_help()
        exit(1)

    sink = FileSink(config)
    sink.run()


if __name__ == '__main__':
    main()
