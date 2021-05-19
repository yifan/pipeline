from pipeline import Settings


def test_settings(monkeypatch):
    class TestSettings(Settings):
        name: str = "value3"

    monkeypatch.setenv("NAME", "value2")
    settings = TestSettings()
    assert settings.name == "value2"

    settings.parse_args(args=["--name", "value4"])
    assert settings.name == "value4"
