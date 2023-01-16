import pytest

def pytest_addoption(parser: pytest.Parser):
    parser.addoption('--email', action='store', help="Email account to access the Clash of Clans API.", type=str, required=True)
    parser.addoption('--password', action='store', help="Password to access the Clash of Clans API.", type=str, required=True)
    parser.addoption('--name', action='store', help="Azure Table Storage account name.", type=str, required=False, default="")
    parser.addoption('--access_key', action='store', help="Azure Table Storage access key.", type=str, required=False, default="")
    parser.addoption('--connection_string', action='store', help="Azure Table Storage connection string.", type=str, required=False, default="")

def pytest_generate_tests(metafunc: pytest.Metafunc):
    if 'email' in metafunc.fixturenames:
        metafunc.parametrize('email', [metafunc.config.getoption('email')])
    if 'password' in metafunc.fixturenames:
        metafunc.parametrize('password', [metafunc.config.getoption('password')])
    if 'name' in metafunc.fixturenames:
        metafunc.parametrize('name', [metafunc.config.getoption('name')])
    if 'access_key' in metafunc.fixturenames:
        metafunc.parametrize('access_key', [metafunc.config.getoption('access_key')])
    if 'connection_string' in metafunc.fixturenames:
        metafunc.parametrize('connection_string', [metafunc.config.getoption('connection_string')])