import json
import pathlib
from typing import Any, Optional, TextIO, Tuple

from pyconnect import PyConnectSource, SourceConfig
from pyconnect.core import Status


class FileSourceConfig(SourceConfig):
    """
    In addition to the fields from :class:`pyconnect.config.SourceConfig` this class provides the following fields:

        **source_directory**: :class:`pathlib.Path`
            The directory where this source looks for the file it reads all messages from.

        **source_filename**: str
            The name of the file that this source reads messages from.
            The file should contain lines of json objects like `{'key': Any, 'value': Any}`
    """

    __parsers = {'source_directory': lambda p: pathlib.Path(p).absolute()}

    def __init__(self, conf_dict):
        conf_dict = conf_dict.copy()
        self['source_directory'] = conf_dict.pop('source_directory')
        self['source_filename'] = conf_dict.pop('source_filename')
        super().__init__(conf_dict)


class FileSource(PyConnectSource):
    """
    A source that reads and publishes json objects from a file.
    """
    def __init__(self, config: FileSourceConfig):
        super().__init__(config)
        self._file: Optional[TextIO] = None

    def on_startup(self):
        source_path = self.config['source_directory'] / self.config['source_filename']
        self._file = open(source_path, 'r')

    def seek(self, index: int) -> None:
        self._file.seek(index)

    def read(self) -> Tuple[Any, Any]:
        line = next(self._file)
        record = json.loads(line)
        return record['key'], record['value']

    def on_eof(self) -> Status:
        return Status.STOPPED

    def get_index(self) -> int:
        return self._file.tell()

    def close(self):
        try:
            super().close()
        finally:
            self._file.close()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', choices=['env', 'yaml', 'json'], default='env', help='Defines where the config '
                                                                                         'is loaded from')
    parser.add_argument('--conf_file', default=None, help='When `conf` is yaml or json, then config is loaded'
                                                          'from this file, default will be `./config.(yaml|json)` '
                                                          'depending on which kind of file you chose')

    args = parser.parse_args()
    config: FileSourceConfig = None

    if args.config == 'env':
        config = FileSourceConfig.from_env_variables()
    elif args.config == 'yaml':
        config = FileSourceConfig.from_yaml_file(args.conf_file or ('./config.' + args.config))
    elif args.config == 'json':
        config = FileSourceConfig.from_json_file(args.conf_file or ('./config.' + args.config))
    else:
        print('Illegal Argument for --config!')
        parser.print_help()
        exit(1)

    source = FileSource(config)
    source.run()


if __name__ == '__main__':
    main()
