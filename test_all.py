import os
import subprocess

import pytest

from controller import ShardHandler, load_data_from_file


def nuke_files():
    subprocess.call(
        "make clean",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
    )


@pytest.fixture(autouse=True)
def cleanup():
    nuke_files()


@pytest.fixture()
def sh():
    sh = ShardHandler()
    sh.build_shards(1, load_data_from_file())
    return sh

nuke_files()  # just in case their file runs things


def test_build_shards(sh):
    assert os.path.exists('mapping.json')
    assert os.path.exists('data/')
    assert os.path.exists('data/0.txt')
    assert not os.path.exists('data/1.txt')
    assert len(sh.mapping.keys()) == 1


def test_add_shard(sh):
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 3735

    sh.add_shard()
    assert len(sh.mapping.keys()) == 2

    assert os.path.exists('data/1.txt')
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 1867
    with open('data/1.txt', 'r') as f:
        assert len(f.read()) == 1868


def test_add_two_shards(sh):
    assert len(sh.mapping.keys()) == 1
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 3735

    sh.add_shard()
    sh.add_shard()

    assert len(sh.mapping.keys()) == 3
    assert os.path.exists('data/2.txt')
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 1245
    with open('data/1.txt', 'r') as f:
        assert len(f.read()) == 1245
    with open('data/2.txt', 'r') as f:
        assert len(f.read()) == 1245


def test_replication(sh):
    sh.add_replication()
    assert os.path.exists('data/0.txt')
    assert os.path.exists('data/0-1.txt')

    with open('data/0.txt') as f1:
        with open('data/0-1.txt') as f2:
            assert len(f1.read()) == len(f2.read())


def test_add_replication_2(sh):
    sh.add_shard()
    sh.add_shard()
    sh.add_replication()
    sh.add_replication()

    assert os.path.exists('data/0.txt')
    assert os.path.exists('data/0-1.txt')
    assert os.path.exists('data/0-2.txt')
    assert os.path.exists('data/1.txt')
    assert os.path.exists('data/1-1.txt')
    assert os.path.exists('data/1-2.txt')
    assert os.path.exists('data/2.txt')
    assert os.path.exists('data/2-1.txt')
    assert os.path.exists('data/2-2.txt')


def test_remove_shard(sh):
    sh.add_shard()

    assert len(sh.mapping.keys()) == 2

    assert os.path.exists('data/1.txt')
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 1867

    sh.remove_shard()

    assert not os.path.exists('data/1.txt')
    with open('data/0.txt', 'r') as f:
        assert len(f.read()) == 3735

    assert len(sh.mapping.keys()) == 1


def test_remove_shard_single_shard(sh):
    with pytest.raises(Exception):
        sh.remove_shard()


def test_remove_replication_no_reps(sh):
    with pytest.raises(Exception):
        sh.test_remove_replication()


def test_remove_replication(sh):
    sh.add_shard()
    sh.add_replication()

    assert len(sh.mapping.keys()) == 4

    assert os.path.exists('data/0-1.txt')
    sh.remove_replication()
    assert not os.path.exists('data/0-1.txt')

    assert len(sh.mapping.keys()) == 2


def test_multiple_remove_replications(sh):
    sh.add_shard()
    sh.add_replication()
    sh.add_replication()
    sh.add_replication()

    assert len(sh.mapping.keys()) == 8

    sh.remove_replication()
    sh.remove_replication()

    assert len(sh.mapping.keys()) == 4


def test_sync_replacing_replications(sh):
    sh.add_replication()
    assert os.path.exists('data/0-1.txt')
    os.remove('data/0-1.txt')
    assert not os.path.exists('data/0-1.txt')
    sh.sync_replication()
    assert os.path.exists('data/0-1.txt')

    with open('data/0.txt') as f1:
        with open('data/0-1.txt') as f2:
            assert len(f1.read()) == len(f2.read())


def test_sync_replacing_primary(sh):
    sh.add_replication()
    assert os.path.exists('data/0.txt')
    os.remove('data/0.txt')
    assert not os.path.exists('data/0.txt')
    sh.sync_replication()
    assert os.path.exists('data/0.txt')

    with open('data/0.txt') as f1:
        with open('data/0-1.txt') as f2:
            assert len(f1.read()) == len(f2.read())


def test_sync_replacing_primary_and_replication(sh):
    sh.add_replication()
    sh.add_replication()

    os.remove('data/0.txt')
    os.remove('data/0-1.txt')
    sh.sync_replication()
    assert os.path.exists('data/0.txt')
    assert os.path.exists('data/0-1.txt')


def test_add_shard_after_replications(sh):
    sh.add_replication()
    assert len(sh.mapping.keys()) == 2

    sh.add_shard()
    assert os.path.exists('data/1-1.txt')
    assert len(sh.mapping.keys()) == 4


def test_remove_shard_after_replications(sh):
    sh.add_shard()
    sh.add_replication()
    sh.add_shard()
    sh.add_replication()

    assert len(sh.mapping.keys()) == 9
    sh.remove_shard()
    assert len(sh.mapping.keys()) == 6


def test_go_crazy(sh):
    for _ in range(16):
        sh.add_shard()

    for _ in range(4):
        sh.add_replication()
        sh.remove_shard()

    for _ in range(3):
        sh.remove_replication()
        sh.add_replication()

    sh.remove_replication()

    assert len(sh.mapping.keys()) == 52
    assert len(sh.get_replication_ids()) == 39
    assert len(sh.get_shard_ids()) == 13
