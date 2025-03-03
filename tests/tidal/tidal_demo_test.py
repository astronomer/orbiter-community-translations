from orbiter_translations.tidal.xml_demo import translation_ruleset


def test_tidal_demo(project_root):
    actual = translation_ruleset.translate_fn(translation_ruleset, (project_root / "tests/tidal/demo/workflow"))
    assert list(actual.dags.keys()) == ["demo"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "n010_get_data",
            "n080_run_script",
        ]
    )
    assert sorted(list(list(actual.dags.values())[0].tasks["n010_get_data"].downstream)) == sorted(["n080_run_script"])
