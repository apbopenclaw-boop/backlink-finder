"""Tests for the monthly-usage counter helpers in main.py."""


def test_monthly_count_starts_at_zero(main_module):
    # Fresh DB from the fixture — month bucket should not exist yet
    assert main_module._get_monthly_count() == 0


def test_increment_creates_and_increments(main_module):
    before = main_module._get_monthly_count()
    main_module._increment_monthly_count()
    main_module._increment_monthly_count()
    main_module._increment_monthly_count()
    assert main_module._get_monthly_count() == before + 3


def test_init_email_db_is_idempotent(main_module):
    # Running it twice should not raise or wipe data
    main_module._init_email_db()
    main_module._increment_monthly_count()
    n = main_module._get_monthly_count()
    main_module._init_email_db()
    assert main_module._get_monthly_count() == n
