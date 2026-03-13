import textwrap

from olah.configs import OlahConfig, OlahRule, OlahRuleList


def test_olah_rule_supports_fnmatch_and_regex():
    fn_rule = OlahRule(repo="org/*", allow=True, use_re=False)
    regex_rule = OlahRule(repo=r"^org/.+-model$", allow=True, use_re=True)

    assert fn_rule.match("org/demo")
    assert not fn_rule.match("other/demo")
    assert regex_rule.match("org/test-model")
    assert not regex_rule.match("org/test-dataset")


def test_olah_rule_list_last_match_wins():
    rules = OlahRuleList.from_list(
        [
            {"repo": "*", "allow": False, "use_re": False},
            {"repo": "org/*", "allow": True, "use_re": False},
            {"repo": "org/private-*", "allow": False, "use_re": False},
        ]
    )

    assert rules.allow("org/public-model") is True
    assert rules.allow("org/private-model") is False
    assert rules.allow("another/repo") is False


def test_olah_config_reads_toml_and_normalizes_empty_ssl_fields(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        textwrap.dedent(
            """
            [basic]
            host = "0.0.0.0"
            port = 9000
            ssl-key = ""
            ssl-cert = ""
            repos-path = "/srv/olah/repos"
            cache-size-limit = "2GB"
            cache-clean-strategy = "FIFO"
            hf-scheme = "http"
            hf-netloc = "hf.internal"
            hf-lfs-netloc = "lfs.internal"
            mirror-scheme = "https"
            mirror-netloc = "mirror.internal"
            mirror-lfs-netloc = "mirror-lfs.internal"
            mirrors-path = ["/mirror/a", "/mirror/b"]

            [accessibility]
            offline = true
            proxy = [
              { repo = "*", allow = false, use_re = false },
              { repo = "team/*", allow = true, use_re = false },
            ]
            cache = [
              { repo = "^team/.+$", allow = true, use_re = true },
            ]
            """
        ),
        encoding="utf-8",
    )

    config = OlahConfig(str(config_path))

    assert config.host == "0.0.0.0"
    assert config.port == 9000
    assert config.ssl_key is None
    assert config.ssl_cert is None
    assert config.repos_path == "/srv/olah/repos"
    assert config.cache_size_limit == 2 * 1024**3
    assert config.cache_clean_strategy == "FIFO"
    assert config.hf_url_base() == "http://hf.internal"
    assert config.hf_lfs_url_base() == "http://lfs.internal"
    assert config.mirror_url_base() == "https://mirror.internal"
    assert config.mirror_lfs_url_base() == "https://mirror-lfs.internal"
    assert config.mirrors_path == ["/mirror/a", "/mirror/b"]
    assert config.offline is True
    assert config.proxy.allow("team/project") is True
    assert config.proxy.allow("other/project") is False
    assert config.cache.allow("team/project") is True


def test_is_specific_addr_only_accepts_real_single_host():
    config = OlahConfig()

    assert config._is_specific_addr("localhost") is True
    assert config._is_specific_addr("0.0.0.0") is False
    assert config._is_specific_addr("::") is False
    assert config._is_specific_addr(["0.0.0.0", "::"]) is False
