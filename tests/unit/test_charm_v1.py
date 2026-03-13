# Copyright 2022 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from pathlib import Path
from unittest.mock import patch

import pytest
from charms.rolling_ops.v1.rollingops import (
    SECRET_FIELD,
    CertificatesManager,
    EtcdCtl,
    RollingOpsEtcdNotConfiguredError,
    RollingOpsKeys,
)
from ops.testing import Context, PeerRelation, Secret, State

from tests.charms.v1.src.charm import CharmRollingOpsCharmV1


def test_rollingopskeys_paths() -> None:
    keys = RollingOpsKeys.for_owner("cluster-a", "unit-1")

    assert keys.cluster_prefix == "/rollingops/cluster-a"
    assert keys._owner_prefix == "/rollingops/cluster-a/unit-1"
    assert keys.lock_key == "/rollingops/cluster-a/granted-unit"
    assert keys.pending == "/rollingops/cluster-a/unit-1/pending/"
    assert keys.inprogress == "/rollingops/cluster-a/unit-1/inprogress/"
    assert keys.completed == "/rollingops/cluster-a/unit-1/completed/"


def test_rollingopskeys_lock_key_is_shared_within_cluster() -> None:
    k1 = RollingOpsKeys.for_owner("cluster-a", "unit-1")
    k2 = RollingOpsKeys.for_owner("cluster-a", "unit-2")

    assert k1.lock_key == k2.lock_key
    assert k1.pending != k2.pending
    assert k1.inprogress != k2.inprogress
    assert k1.completed != k2.completed


@pytest.fixture
def temp_cert_manager(tmp_path):
    class TestCertificatesManager(CertificatesManager):
        BASE_DIR = tmp_path / "tls"
        CA_KEY = BASE_DIR / "client-ca.key"
        CA_CERT = BASE_DIR / "client-ca.pem"
        CLIENT_KEY = BASE_DIR / "client.key"
        CLIENT_CERT = BASE_DIR / "client.pem"

    TestCertificatesManager.BASE_DIR.mkdir(parents=True, exist_ok=True)
    return TestCertificatesManager


def test_certificates_manager_exists_returns_false_when_no_files(temp_cert_manager) -> None:
    assert temp_cert_manager.exists() is False


def test_certificates_manager_exists_returns_true_when_all_files_exist(temp_cert_manager) -> None:
    temp_cert_manager.CA_KEY.write_text("ca-key")
    temp_cert_manager.CA_CERT.write_text("ca-cert")
    temp_cert_manager.CLIENT_KEY.write_text("client-key")
    temp_cert_manager.CLIENT_CERT.write_text("client-cert")

    assert temp_cert_manager.exists() is True


def test_certificates_manager_load_cert_and_key(temp_cert_manager) -> None:
    temp_cert_manager.CLIENT_CERT.write_text("client-cert-pem")
    temp_cert_manager.CLIENT_KEY.write_text("client-key-pem")

    cert_pem, key_pem = temp_cert_manager.load_client_cert_and_key()

    assert cert_pem == "client-cert-pem"
    assert key_pem == "client-key-pem"


def test_certificates_manager_client_paths(temp_cert_manager) -> None:
    cert_path, key_path = temp_cert_manager.client_paths()

    assert cert_path == temp_cert_manager.CLIENT_CERT
    assert key_path == temp_cert_manager.CLIENT_KEY


def test_certificates_manager_persist_client_cert_and_key_writes_files(
    temp_cert_manager,
) -> None:

    temp_cert_manager.persist_client_cert_and_key("cert-pem", "key-pem")

    assert temp_cert_manager.CLIENT_CERT.read_text() == "cert-pem"
    assert temp_cert_manager.CLIENT_KEY.read_text() == "key-pem"


def test_certificates_manager_has_client_cert_and_key_returns_false_when_files_missing(
    temp_cert_manager,
) -> None:
    assert temp_cert_manager.has_client_cert_and_key("cert", "key") is False


def test_certificates_manager_has_client_cert_and_key_returns_true_when_material_matches(
    temp_cert_manager,
) -> None:
    temp_cert_manager.CLIENT_CERT.write_text("cert-pem")
    temp_cert_manager.CLIENT_KEY.write_text("key-pem")

    assert temp_cert_manager.has_client_cert_and_key("cert-pem", "key-pem") is True


def test_certificates_manager_has_client_cert_and_key_returns_false_when_material_differs(
    temp_cert_manager,
) -> None:
    temp_cert_manager.CLIENT_CERT.write_text("cert-pem")
    temp_cert_manager.CLIENT_KEY.write_text("key-pem")

    assert temp_cert_manager.has_client_cert_and_key("other-cert", "key-pem") is False
    assert temp_cert_manager.has_client_cert_and_key("cert-pem", "other-key") is False


def test_certificates_manager_generate_does_nothing_when_files_already_exist(
    temp_cert_manager,
) -> None:
    temp_cert_manager.CA_KEY.write_text("existing-ca-key")
    temp_cert_manager.CA_CERT.write_text("existing-ca-cert")
    temp_cert_manager.CLIENT_KEY.write_text("existing-client-key")
    temp_cert_manager.CLIENT_CERT.write_text("existing-client-cert")

    temp_cert_manager.generate(common_name="unit-1")

    assert temp_cert_manager.CA_KEY.read_text() == "existing-ca-key"
    assert temp_cert_manager.CA_CERT.read_text() == "existing-ca-cert"
    assert temp_cert_manager.CLIENT_KEY.read_text() == "existing-client-key"
    assert temp_cert_manager.CLIENT_CERT.read_text() == "existing-client-cert"


def test_certificates_manager_generate_creates_all_files(
    temp_cert_manager,
) -> None:

    temp_cert_manager.generate(common_name="unit-1")
    assert temp_cert_manager.exists() is True

    assert temp_cert_manager.CA_KEY.read_text().startswith("-----BEGIN RSA PRIVATE KEY-----")
    assert temp_cert_manager.CA_CERT.read_text().startswith("-----BEGIN CERTIFICATE-----")
    assert temp_cert_manager.CLIENT_KEY.read_text().startswith("-----BEGIN RSA PRIVATE KEY-----")
    assert temp_cert_manager.CLIENT_CERT.read_text().startswith("-----BEGIN CERTIFICATE-----")


@pytest.fixture
def temp_etcdctl(tmp_path):
    class TestEtcdCtl(EtcdCtl):
        BASE_DIR = tmp_path / "etcd"
        SERVER_CA = BASE_DIR / "server-ca.pem"
        ENV_FILE = BASE_DIR / "etcdctl.env"

    return TestEtcdCtl


def test_etcdctl_write_env_file_creates_dir_files(temp_etcdctl) -> None:

    client_cert = Path("/tmp/client.pem")
    client_key = Path("/tmp/client.key")

    temp_etcdctl.write_env_file(
        endpoints="https://10.0.0.1:2379,https://10.0.0.2:2379",
        tls_ca_pem="CA-PEM",
        client_cert_path=client_cert,
        client_key_path=client_key,
    )

    assert temp_etcdctl.BASE_DIR.exists()
    assert temp_etcdctl.SERVER_CA.read_text() == "CA-PEM"

    env_text = temp_etcdctl.ENV_FILE.read_text()
    assert 'export ETCDCTL_API="3"' in env_text
    assert 'export ETCDCTL_ENDPOINTS="https://10.0.0.1:2379,https://10.0.0.2:2379"' in env_text
    assert f'export ETCDCTL_CACERT="{temp_etcdctl.SERVER_CA}"' in env_text
    assert 'export ETCDCTL_CERT="/tmp/client.pem"' in env_text
    assert 'export ETCDCTL_KEY="/tmp/client.key"' in env_text


def test_etcdctl_ensure_initialized_raises_when_env_missing(temp_etcdctl) -> None:
    with pytest.raises(RollingOpsEtcdNotConfiguredError):
        temp_etcdctl.ensure_initialized()


def test_etcdctl_load_env_parses_exported_vars(temp_etcdctl) -> None:
    temp_etcdctl.BASE_DIR.mkdir(parents=True, exist_ok=True)
    temp_etcdctl.ENV_FILE.write_text(
        "\n".join([
            "# comment",
            'export ETCDCTL_API="3"',
            'export ETCDCTL_ENDPOINTS="https://10.0.0.1:2379"',
            "export ETCDCTL_CERT='/tmp/client.pem'",
            'export ETCDCTL_KEY="/tmp/client.key"',
            'export ETCDCTL_CACERT="/tmp/server-ca.pem"',
            "",
        ])
    )

    with patch.dict("os.environ", {"EXISTING_VAR": "present"}, clear=True):
        env = temp_etcdctl.load_env()

    assert env["EXISTING_VAR"] == "present"
    assert env["ETCDCTL_API"] == "3"
    assert env["ETCDCTL_ENDPOINTS"] == "https://10.0.0.1:2379"
    assert env["ETCDCTL_CERT"] == "/tmp/client.pem"
    assert env["ETCDCTL_KEY"] == "/tmp/client.key"
    assert env["ETCDCTL_CACERT"] == "/tmp/server-ca.pem"


@pytest.fixture
def certificates_manager_patches():
    with (
        patch(
            "charms.rolling_ops.v1.rollingops.CertificatesManager.exists",
            return_value=False,
        ),
        patch(
            "charms.rolling_ops.v1.rollingops.CertificatesManager.generate",
            return_value=None,
        ) as mock_generate,
        patch(
            "charms.rolling_ops.v1.rollingops.CertificatesManager.load_client_cert_and_key",
            return_value=("CERT_PEM", "KEY_PEM"),
        ),
        patch(
            "charms.rolling_ops.v1.rollingops.CertificatesManager.client_paths",
            return_value=(Path("/tmp/client.pem"), Path("/tmp/client.key")),
        ),
        patch(
            "charms.rolling_ops.v1.rollingops.CertificatesManager.persist_client_cert_and_key",
            return_value=(Path("/tmp/client.pem"), Path("/tmp/client.key")),
        ) as mock_persit,
    ):
        yield {
            "generate": mock_generate,
            "persist": mock_persit,
        }


@pytest.fixture
def etcdctl_patch():
    with patch("charms.rolling_ops.v1.rollingops.EtcdCtl") as mock_etcdctl:
        yield mock_etcdctl


def test_leader_elected_creates_shared_secret_and_stores_id(
    certificates_manager_patches, etcdctl_patch
):
    ctx = Context(CharmRollingOpsCharmV1)
    peer_relation = PeerRelation(endpoint="restart")

    state_in = State(leader=True, relations={peer_relation})
    state_out = ctx.run(ctx.on.leader_elected(), state_in)

    peer_out = next(r for r in state_out.relations if r.endpoint == "restart")
    assert SECRET_FIELD in peer_out.local_app_data
    assert peer_out.local_app_data[SECRET_FIELD].startswith("secret:")

    certificates_manager_patches["generate"].assert_called_once()


def test_leader_elected_does_not_regenerate_when_secret_already_exists(
    certificates_manager_patches, etcdctl_patch
):
    ctx = Context(CharmRollingOpsCharmV1)
    peer_relation = PeerRelation(
        endpoint="restart", local_app_data={SECRET_FIELD: "secret:existing"}
    )
    secret = Secret(
        id="secret:existing",
        owner="application",
        tracked_content={
            "cert": "CERT_PEM",
            "key": "KEY_PEM",
        },
    )

    state_in = State(leader=True, relations={peer_relation}, secrets=[secret])

    state_out = ctx.run(ctx.on.leader_elected(), state_in)

    peer_out = next(r for r in state_out.relations if r.endpoint == "restart")
    assert peer_out.local_app_data[SECRET_FIELD] == "secret:existing"
    certificates_manager_patches["generate"].assert_not_called()


def test_non_leader_does_not_create_shared_secret(certificates_manager_patches, etcdctl_patch):
    ctx = Context(CharmRollingOpsCharmV1)
    peer_relation = PeerRelation(endpoint="restart")
    state_in = State(leader=False, relations={peer_relation})

    state_out = ctx.run(ctx.on.relation_changed(peer_relation, remote_unit=1), state_in)

    peer_out = next(r for r in state_out.relations if r.endpoint == "restart")
    assert SECRET_FIELD not in peer_out.local_app_data
    certificates_manager_patches["generate"].assert_not_called()


def test_relation_changed_syncs_local_certificate_from_secret(
    certificates_manager_patches, etcdctl_patch
):
    ctx = Context(CharmRollingOpsCharmV1)
    peer_relation = PeerRelation(
        endpoint="restart", local_app_data={SECRET_FIELD: "secret:rollingops-cert"}
    )

    secret = Secret(
        id="secret:rollingops-cert",
        tracked_content={"cert": "CERT_PEM", "key": "KEY_PEM"},
    )

    state_in = State(leader=False, relations={peer_relation}, secrets=[secret])

    ctx.run(ctx.on.relation_changed(peer_relation, remote_unit=1), state_in)
    certificates_manager_patches["persist"].assert_called_once_with("CERT_PEM", "KEY_PEM")
