import json
import os
from typing import List

from confluent_kafka.cimpl import Message

from pyconnect import PyConnectSink, SinkConfig
from pyconnect.core import Status


class FileSinkConfig(SinkConfig):
    """
    In addition to the fields from :class:`pyconnect.config.SinkConfig` this class provides the following fields:

        **sink_directory**: str
            The directory where this sink shall put the file it writes all messages to.

        **sink_filename**: str
            The name of the file that this sink writes all messages to.
    """

    def __init__(self, conf_dict):
        conf_dict = conf_dict.copy()
        self['sink_directory'] = conf_dict.pop('sink_directory')
        self['sink_filename'] = conf_dict.pop('sink_filename')
        super().__init__(conf_dict)


class FileSink(PyConnectSink):
    """
    A sink that writes all messages it receives to a single file.
    """

    def __init__(self, config: FileSinkConfig):
        super().__init__(config)
        self._buffer: List[Message] = []

    def on_message_received(self, msg: Message) -> None:
        self._buffer.append(msg)

    def on_startup(self):
        sink_dir = os.path.abspath(self.config['sink_directory'])
        if not os.path.exists(sink_dir):
            os.makedirs(sink_dir)
        elif not os.path.isdir(sink_dir):
            raise RuntimeError(f'Sink directory {sink_dir!r} exists and is not a directory!')

    def on_flush(self) -> None:
        lines = [
            json.dumps({'key': msg.key(), 'value': msg.value()}) + '\n'
            for msg in self._buffer
        ]

        with open('{sink_directory}/{sink_filename}'.format(**self.config), 'a') as outfile:
            outfile.writelines(lines)

        self._buffer.clear()

    def on_no_message_received(self):
        if all(self.eof_reached.values()):
            return Status.STOPPED
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', choices=['env', 'yaml', 'json'], default='env', help='Defines where the config '
                                                                                         'is loaded from')
    parser.add_argument('--conf_file', default=None, help='When `conf` is yaml or json, then config is loaded'
                                                          'from this file, default will be `./config.(yaml|json)` '
                                                          'depending on which kind of file you chose')

    args = parser.parse_args()
    config: FileSinkConfig = None

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
