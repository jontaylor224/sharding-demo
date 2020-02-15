import json
import os
from shutil import copyfile
from typing import List, Dict

filename = "chapter2.txt"


def load_data_from_file(path=None) -> str:
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()
        self.last_char_position = 0

    mapfile = "mapping.json"

    def write_map(self) -> None:
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self) -> Dict:
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def _reset_char_position(self):
        self.last_char_position = 0

    def get_shard_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' not in key])

    def get_replication_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' in key])

    def build_shards(self, count: int, data: str = None) -> [str, None]:
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard_mapping(self, num: str, data: str, replication=False):
        """Write the requested data to the mapfile. The optional `replication`
        flag allows overriding the start and end information with the shard
        being replicated."""
        if replication:
            parent_shard = self.mapping.get(num[:num.index('-')])
            self.mapping.update(
                {
                    num: {
                        'start': parent_shard['start'],
                        'end': parent_shard['end']
                    }
                }
            )
        else:
            if int(num) == 0:
                # We reset it here in case we perform multiple write operations
                # within the same instantiation of the class. The char position
                # is used to power the index creation.
                self._reset_char_position()

            self.mapping.update(
                {
                    str(num): {
                        'start': (
                            self.last_char_position if
                            self.last_char_position == 0 else
                            self.last_char_position + 1
                        ),
                        'end': self.last_char_position + len(data)
                    }
                }
            )

            self.last_char_position += len(data)

    def _write_shard(self, num: int, data: str) -> None:
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)
        self._write_shard_mapping(str(num), data)

    def _generate_sharded_data(self, count: int, data: str) -> List[str]:
        """Split the data into as many pieces as needed."""
        splicenum, rem = divmod(len(data), count)

        result = [data[splicenum * z:splicenum *
                       (z + 1)] for z in range(count)]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self) -> str:
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()

        for db in self.get_shard_ids():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def add_shard(self) -> None:
        """Add a new shard to the existing pool and rebalance the data."""
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys) + 2

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def remove_shard(self) -> None:
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        if len(keys) == 1:
            raise Exception('Cannot remove last shard.')

        new_shard_num = max(keys)

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        files = os.listdir('./data')

        for filename in files:
            if '-' in filename:
                file_shard = filename.split('-')[0]
            else:
                file_shard = filename.split('.')[0]
            if str(new_shard_num) == file_shard:
                os.remove(f'./data/{filename}')
                map_shard = filename.split('.')[0]
                self.mapping.pop(map_shard)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def get_highest_replication_level(self) -> int:
        """Determine highest level of replication or return 0 if none
        """
        levels = set()
        files = os.listdir('./data')

        for filename in files:
            if '-' in filename:
                key = filename.split('.')[0]
                level = int(key.split('-')[1])
                levels.add(level)
        if levels:
            max_level = max(levels)
        else:
            max_level = 0
        return max_level

    def add_replication(self) -> None:
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        self.mapping = self.load_map()
        next_repl_lvl = self.get_highest_replication_level() + 1

        shards = self.get_shard_ids()

        for key in shards:
            primary_file = f'data/{key}.txt'
            replication_file = f'data/{key}-{next_repl_lvl}.txt'
            copyfile(primary_file, replication_file)

        for i, shard in enumerate(shards):
            self.mapping[f'{i}-{next_repl_lvl}'] = self.mapping[shard]

        self.write_map()

    def remove_replication(self) -> None:
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """
        self.mapping = self.load_map()

        max_level = self.get_highest_replication_level()

        if max_level == 0:
            raise Exception('No replications left to remove.')

        files = os.listdir('./data')

        for filename in files:
            if '-' in filename:
                key = filename.split('.')[0]
                repl = key.split('-')[1]
                if repl == str(max_level):
                    os.remove(f'./data/{filename}')
                    self.mapping.pop(key)

    def sync_replication(self) -> None:
        """Verify that all replications are equal to their primaries and that
        any missing primaries are appropriately recreated from their
        replications."""
        self.mapping = self.load_map()
        files = os.listdir('./data')
        primary_file_keys = []
        repl_file_keys = []
        primary_map_keys = []
        repl_map_keys = []
        for filename in files:
            if '-' in filename:
                repl_file_keys.append(filename.split('.')[0])
            else:
                primary_file_keys.append(filename.split('.')[0])

        for key in self.mapping.keys():
            if '-' in key:
                repl_map_keys.append(key)
            else:
                primary_map_keys.append(key)

        for key in primary_map_keys:
            if key not in primary_file_keys:
                print(f'Missing file {key}.txt.')
                for repl_key in repl_file_keys:
                    if repl_key.startswith(key):
                        with open(f'./data/{repl_key}.txt', 'r') as src:
                            data = src.read()
                            with open(f'./data/{key}.txt', 'w') as dest:
                                dest.write(data)
                        print(f'Restored {key}.txt from {repl_key}.txt.')
                        break

        highest_repl = self.get_highest_replication_level()
        if highest_repl:
            for i in range(1, highest_repl + 1):
                for key in primary_map_keys:
                    copyfile(f'./data/{key}.txt', f'./data/{key}-{i}.txt')
                    self.mapping[f'{key}-{i}'] = self.mapping[f'{key}']

        self.write_map()

    def get_shard_data(self, shardnum=None) -> [str, Dict]:
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.get_shard_ids()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self) -> Dict:
        """A helper function to view the mapping data."""
        return self.mapping

    def get_word_at_index(self, index):
        """Returns the word at a given index and the shard file containing the word"""
        filename = None
        shard_num = None
        keys = self.mapping.keys()
        for key in keys:
            first_index = self.mapping[key]['start']
            last_index = self.mapping[key]['end']
            if (index >= first_index and index <= last_index):
                filename = f'./data/{key}.txt'
                shard_num = key
        inner_index = index - self.mapping[shard_num]['start']
        with open(filename, 'r') as shard_file:
            data = shard_file.read()
            start_char = data[inner_index]

            while (not start_char.isalpha() and inner_index < len(data) - 1):
                inner_index += 1
                start_char = data[inner_index]

            data_fragment = data[inner_index:]
            data_fragment = data_fragment.replace('\n', ' ').replace('\r', ' ')
            words = data_fragment.split(' ')
            word = words[0]
            while ((data[inner_index - 1] != ' ' and data[inner_index -1] != '\n') and inner_index > 0):
                word = data[inner_index - 1] + word
                inner_index -= 1

                if (inner_index == 0 and int(shard_num) > 0):
                    with open(f'./data/{int(shard_num) - 1}.txt', 'r') as prev_shard:
                        prev_data = prev_shard.read()
                        last_fragment = prev_data.split(' ')[-1]
                        word = last_fragment + word
            max_shard = max([int(z) for z in self.get_shard_ids()])
            if len(words) == 1 and int(shard_num) < max_shard:
                while (data[inner_index].isalpha() and inner_index < len(data) - 1):
                    inner_index += 1
                    if inner_index == len(data) - 1:
                        with open(f'./data/{int(shard_num) + 1}.txt', 'r') as next_file:
                            next_data = next_file.read()
                            next_fragment = next_data.split(' ')[0]
                            word = word + next_fragment

        word = word.replace('\n', '').strip(' ,."?!;:')
        return (f'The word "{word}" was found at index {index} in {shard_num}.txt')


s = ShardHandler()

s.build_shards(5, load_data_from_file())

print(s.mapping.keys())

s.add_shard()

print(s.mapping.keys())
