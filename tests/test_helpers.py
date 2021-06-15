from pipeline import Settings


class ASettings(Settings):
    name: str = "value3"
    flag: bool = False


class TestSettings:
    def test_default(self):
        settings = ASettings()
        assert settings.name == "value3"
        assert settings.flag is False

    def test_env(self, monkeypatch):
        monkeypatch.setenv("NAME", "value2")
        monkeypatch.setenv("FLAG", "true")
        settings = ASettings()
        assert settings.name == "value2"
        assert settings.flag is True

    def test_args(self):
        settings = ASettings()
        settings.parse_args(args=["--name", "value4", "--flag"])
        assert settings.name == "value4"
        assert settings.flag is True
