from urllib.request import urlopen, Request
import json
import os

# Remove any normal user token from the environment so that we don't
# accidentally use it.
os.environ.pop("motherduck_token", None)

MD_TEST_USER_CREATOR_TOKEN = os.environ.get("md_test_user_creator_token")
CACHED_TEST_USER = None


def can_run_md_tests():
    return MD_TEST_USER_CREATOR_TOKEN is not None


def create_test_user():
    """
    Given a `motherduck_token` environment variable, this function creates a test user and returns its details.
    The user to which the `motherduck_token` belongs needs to have the rights to create test users.

    For now, this function only supports one user, that will be re-used for all tests.
    """

    global CACHED_TEST_USER
    if CACHED_TEST_USER is not None:
        return CACHED_TEST_USER

    token = MD_TEST_USER_CREATOR_TOKEN
    if not token:
        raise ValueError("Environment variable 'motherduck_token' not set.")

    data = json.dumps({"token": token}).encode("utf-8")

    host = os.environ.get("motherduck_host", "api.motherduck.com")
    req = Request(url=f"https://{host}/tuc/createTestUser", data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    with urlopen(req) as f:
        res = f.read().decode("utf-8")
        res_json = json.loads(res)
        print(
            f"Created user with email='{res_json['testEmail']}' and id='{res_json['id']}'"
        )
        CACHED_TEST_USER = res_json
        return res_json
