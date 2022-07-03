import os
import shutil
from pathlib import Path
import pytest
from dflow.utils import set_directory, run_command

def test_set_directory():
    pwd = Path.cwd()
    with set_directory("test_dir", mkdir=True) as wdir:
        assert str(wdir) == str(pwd / "test_dir")
        assert os.getcwd() == str(wdir)
    shutil.rmtree(wdir)

def test_run_command():
    code, out, err = run_command("echo test")
    assert code == 0
    assert out == "test\n"

def test_run_command_err():
    with pytest.raises(AssertionError):
        run_command(["python", "-c", "raise ValueError('error')"])

def test_run_command_input():
    code, out, err = run_command(["sh"], input="echo test\nexit")
    assert code == 0
    assert out == "test\n"
    