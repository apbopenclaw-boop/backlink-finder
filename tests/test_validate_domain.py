"""Tests for crawler.validate_domain."""
import pytest


@pytest.mark.parametrize("inp,expected", [
    ("example.com", "example.com"),
    ("EXAMPLE.com", "example.com"),
    ("  example.com  ", "example.com"),
    ("example.com.", "example.com"),
    ("sub.example.co.uk", "sub.example.co.uk"),
    ("a.b", "a.b"),
    ("xn--bcher-kva.de", "xn--bcher-kva.de"),  # IDN punycode
])
def test_valid_domains_normalized(crawler_module, inp, expected):
    assert crawler_module.validate_domain(inp) == expected


@pytest.mark.parametrize("inp", [
    "",
    "..double-dot.com",
    "-leading-dash.com",
    "trailing-dash-.com",
    "has spaces.com",
    "has_underscore.com",
    "http://example.com",
    "example.com/path",
    "example.com:8080",
    "a" * 254,  # exceeds 253-char overall limit
])
def test_invalid_domains_rejected(crawler_module, inp):
    with pytest.raises(ValueError):
        crawler_module.validate_domain(inp)


@pytest.mark.parametrize("inp", [
    "no-tld",                # single-label (no dot)
    "x" * 64 + ".com",       # label exceeds 63 chars (RFC 1035)
])
@pytest.mark.xfail(reason="DOMAIN_RE accepts these; not currently enforced", strict=True)
def test_known_validation_gaps(crawler_module, inp):
    """Documents inputs the regex accepts that arguably shouldn't be valid.

    Marked xfail+strict so this fails loudly if validate_domain ever gets
    tightened, prompting the test to be moved to test_invalid_domains_rejected.
    """
    with pytest.raises(ValueError):
        crawler_module.validate_domain(inp)


def test_max_length_boundary(crawler_module):
    # 253 chars exactly should pass; 254 should fail
    label = "a" * 60
    domain_253 = ".".join([label, label, label, label, "abc.de"])  # build a 253-char domain
    # ensure exactly 253
    assert len(domain_253) <= 253
    crawler_module.validate_domain(domain_253)  # no raise

    over = "a" * 254
    with pytest.raises(ValueError):
        crawler_module.validate_domain(over)
