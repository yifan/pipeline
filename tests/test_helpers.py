from pipeline import Settings


class ASettings(Settings):
    somename: str = "value3"
    someflag: bool = False


class TestSettings:
    def test_default(self):
        settings = ASettings()
        assert settings.somename == "value3"
        assert settings.someflag is False

    def test_env(self, monkeypatch):
        monkeypatch.setenv("SOMENAME", "value2")
        monkeypatch.setenv("SOMEFLAG", "true")
        settings = ASettings()
        assert settings.somename == "value2"
        assert settings.someflag is True

    def test_args(self):
        settings = ASettings()
        settings.parse_args(args=["--somename", "value4", "--someflag"])
        assert settings.somename == "value4"
        assert settings.someflag is True
